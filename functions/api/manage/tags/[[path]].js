/**
 * Tag Management API for Single Files
 *
 * GET /api/manage/tags/{fileId} - Get tags for a file
 * POST /api/manage/tags/{fileId} - Update tags for a file
 * DELETE /api/manage/tags/{fileId} - Clear all tags for a file
 * PATCH /api/manage/tags/{fileId} - Partial tag update
 *
 * POST body format:
 * {
 *   action: "set" | "add" | "remove" | "replace" | "toggle",
 *   tags: ["tag1", "tag2", ...],
 *   options: {
 *     caseSensitive: false,
 *     unique: true,
 *     normalize: true
 *   }
 * }
 */

import { purgeCFCache } from "../../../utils/purgeCache.js";
import { addFileToIndex } from "../../../utils/indexManager.js";
import { getDatabase } from "../../../utils/databaseAdapter.js";
import { mergeTags, normalizeTags, validateTag } from "../../../utils/tagHelpers.js";

/**
 * @typedef {Object} TagRequest
 * @property {string} action - Tag operation action
 * @property {Array<string>} tags - Array of tags to apply
 * @property {TagOptions} [options] - Operation options
 */

/**
 * @typedef {Object} TagOptions
 * @property {boolean} [caseSensitive=false] - Case sensitive tag matching
 * @property {boolean} [unique=true] - Ensure tags are unique
 * @property {boolean} [normalize=true] - Normalize tag names
 * @property {boolean} [validate=true] - Validate tag format
 */

/**
 * @typedef {Object} TagResponse
 * @property {boolean} success - Operation success status
 * @property {string} fileId - File identifier
 * @property {Array<string>} tags - Current tags
 * @property {string} [action] - Operation action
 * @property {Object} [metadata] - File metadata
 * @property {string} [processingTime] - Processing time
 */

/**
 * 配置常量
 */
const CONFIG = {
    CACHE_TTL: 300, // 5分钟缓存
    MAX_TAG_LENGTH: 100,
    MIN_TAG_LENGTH: 1,
    SUPPORTED_ACTIONS: ['set', 'add', 'remove', 'replace', 'toggle', 'clear'],
    ALLOWED_METHODS: ['GET', 'POST', 'DELETE', 'PATCH', 'OPTIONS']
};

/**
 * 缓存管理器
 */
const CacheManager = {
    /** @type {Map<string, { data: any, expires: number }>} */
    cache: new Map(),
    
    /**
     * 获取缓存数据
     * @param {string} key - 缓存键
     * @returns {any|null} 缓存数据或null
     */
    get(key) {
        const entry = this.cache.get(key);
        if (!entry) return null;
        
        if (Date.now() > entry.expires) {
            this.cache.delete(key);
            return null;
        }
        
        return entry.data;
    },
    
    /**
     * 设置缓存数据
     * @param {string} key - 缓存键
     * @param {any} data - 缓存数据
     * @param {number} ttl - 缓存时间（秒）
     */
    set(key, data, ttl = CONFIG.CACHE_TTL) {
        this.cache.set(key, {
            data,
            expires: Date.now() + (ttl * 1000)
        });
        
        // 限制缓存大小
        if (this.cache.size > 1000) {
            const oldestKey = this.cache.keys().next().value;
            this.cache.delete(oldestKey);
        }
    },
    
    /**
     * 清除缓存
     * @param {string} [key] - 特定键，不提供则清除所有
     */
    clear(key) {
        if (key) {
            this.cache.delete(key);
        } else {
            this.cache.clear();
        }
    }
};

/**
 * 处理预检请求
 * @param {Request} request - 请求对象
 * @returns {Response|null} 预检响应或null
 */
function handlePreflightRequest(request) {
    if (request.method === 'OPTIONS') {
        return new Response(null, {
            status: 204,
            headers: {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': CONFIG.ALLOWED_METHODS.join(', '),
                'Access-Control-Allow-Headers': 'Content-Type, Authorization',
                'Access-Control-Max-Age': '86400'
            }
        });
    }
    return null;
}

/**
 * 验证文件ID
 * @param {string} fileId - 文件ID
 * @returns {boolean} 是否有效
 */
function validateFileId(fileId) {
    if (!fileId || typeof fileId !== 'string') {
        return false;
    }
    
    // 简单的文件ID验证（根据实际需求调整）
    const fileIdRegex = /^[a-zA-Z0-9_\-]{8,64}$/;
    return fileIdRegex.test(fileId);
}

/**
 * 验证标签请求
 * @param {TagRequest} requestBody - 请求体
 * @returns {Object} 验证结果
 */
function validateTagRequest(requestBody) {
    const { action = 'set', tags = [], options = {} } = requestBody;
    
    // Validate action
    if (!CONFIG.SUPPORTED_ACTIONS.includes(action)) {
        return {
            valid: false,
            error: 'Invalid action',
            message: `Action must be one of: ${CONFIG.SUPPORTED_ACTIONS.join(', ')}`
        };
    }
    
    // Validate tags array for non-clear actions
    if (action !== 'clear' && (!Array.isArray(tags) || tags.length === 0)) {
        return {
            valid: false,
            error: 'Invalid tags',
            message: 'Tags must be a non-empty array for this action'
        };
    }
    
    // Validate options
    const validOptions = {
        caseSensitive: typeof options.caseSensitive === 'boolean' ? options.caseSensitive : false,
        unique: typeof options.unique === 'boolean' ? options.unique : true,
        normalize: typeof options.normalize === 'boolean' ? options.normalize : true,
        validate: typeof options.validate === 'boolean' ? options.validate : true
    };
    
    // Validate tags if needed
    if (validOptions.validate && tags.length > 0) {
        const validation = validateTags(tags, validOptions);
        if (!validation.valid) {
            return validation;
        }
    }
    
    return {
        valid: true,
        params: {
            action,
            tags: processTags(tags, validOptions),
            options: validOptions
        }
    };
}

/**
 * 验证标签数组
 * @param {Array<string>} tags - 标签数组
 * @param {TagOptions} options - 验证选项
 * @returns {Object} 验证结果
 */
function validateTags(tags, options) {
    const invalidTags = [];
    const duplicateTags = [];
    const seenTags = new Set();
    
    for (const tag of tags) {
        // Check tag format
        if (!validateTag(tag)) {
            invalidTags.push(tag);
            continue;
        }
        
        // Check tag length
        if (tag.length < CONFIG.MIN_TAG_LENGTH || tag.length > CONFIG.MAX_TAG_LENGTH) {
            invalidTags.push(tag);
            continue;
        }
        
        // Check for duplicates
        const tagKey = options.caseSensitive ? tag : tag.toLowerCase();
        if (seenTags.has(tagKey)) {
            duplicateTags.push(tag);
        }
        seenTags.add(tagKey);
    }
    
    if (invalidTags.length > 0) {
        return {
            valid: false,
            error: 'Invalid tag format',
            message: `Tags must be ${CONFIG.MIN_TAG_LENGTH}-${CONFIG.MAX_TAG_LENGTH} characters and contain only valid characters`,
            invalidTags
        };
    }
    
    if (duplicateTags.length > 0 && options.unique) {
        return {
            valid: false,
            error: 'Duplicate tags',
            message: 'Tags array contains duplicate entries',
            duplicateTags
        };
    }
    
    return { valid: true };
}

/**
 * 处理标签（规范化、去重等）
 * @param {Array<string>} tags - 标签数组
 * @param {TagOptions} options - 处理选项
 * @returns {Array<string>} 处理后的标签
 */
function processTags(tags, options) {
    if (!tags || tags.length === 0) return [];
    
    let processed = [...tags];
    
    // Normalize tags
    if (options.normalize) {
        processed = normalizeTags(processed);
    }
    
    // Remove duplicates
    if (options.unique) {
        const seen = new Set();
        processed = processed.filter(tag => {
            const tagKey = options.caseSensitive ? tag : tag.toLowerCase();
            if (seen.has(tagKey)) return false;
            seen.add(tagKey);
            return true;
        });
    }
    
    return processed;
}

/**
 * 获取文件标签
 * @param {Object} db - 数据库实例
 * @param {string} fileId - 文件ID
 * @param {boolean} [bypassCache=false] - 是否绕过缓存
 * @returns {Promise<Object>} 文件标签信息
 */
async function getFileTags(db, fileId, bypassCache = false) {
    const cacheKey = `tags:${fileId}`;
    
    // Check cache first
    if (!bypassCache) {
        const cached = CacheManager.get(cacheKey);
        if (cached) {
            return {
                ...cached,
                fromCache: true
            };
        }
    }
    
    // Get from database
    const fileData = await db.getWithMetadata(fileId);
    
    if (!fileData || !fileData.metadata) {
        throw new Error('File not found');
    }
    
    const tags = fileData.metadata.Tags || [];
    const result = {
        fileId,
        tags,
        metadata: fileData.metadata,
        fromCache: false
    };
    
    // Cache the result
    CacheManager.set(cacheKey, result);
    
    return result;
}

/**
 * 更新文件标签
 * @param {Object} params - 更新参数
 * @returns {Promise<Object>} 更新结果
 */
async function updateFileTags({
    db,
    fileId,
    action,
    tags,
    options,
    context
}) {
    const { waitUntil, env } = context;
    
    // Get current file data
    const fileData = await db.getWithMetadata(fileId);
    
    if (!fileData || !fileData.metadata) {
        throw new Error('File not found');
    }
    
    // Get existing tags
    const existingTags = fileData.metadata.Tags || [];
    let updatedTags = [...existingTags];
    
    // Process tags based on action
    switch (action) {
        case 'set':
            updatedTags = [...tags];
            break;
            
        case 'add':
            updatedTags = mergeTags(existingTags, tags, 'add');
            break;
            
        case 'remove':
            updatedTags = mergeTags(existingTags, tags, 'remove');
            break;
            
        case 'replace':
            // Replace existing tags that match any in the tags array
            updatedTags = existingTags.filter(tag => 
                !tags.some(t => 
                    options.caseSensitive ? t === tag : t.toLowerCase() === tag.toLowerCase()
                )
            ).concat(tags);
            break;
            
        case 'toggle':
            // Toggle each tag in the tags array
            updatedTags = existingTags.filter(tag => 
                !tags.some(t => 
                    options.caseSensitive ? t === tag : t.toLowerCase() === tag.toLowerCase()
                )
            );
            
            // Add tags that were not present
            const tagsToAdd = tags.filter(tag => 
                !existingTags.some(t => 
                    options.caseSensitive ? t === tag : t.toLowerCase() === tag.toLowerCase()
                )
            );
            
            updatedTags = [...updatedTags, ...tagsToAdd];
            break;
            
        case 'clear':
            updatedTags = [];
            break;
    }
    
    // Remove duplicates and empty tags
    updatedTags = [...new Set(updatedTags.filter(tag => tag && tag.trim() !== ''))];
    
    // Check if tags actually changed
    const tagsChanged = JSON.stringify(existingTags.sort()) !== JSON.stringify(updatedTags.sort());
    
    if (!tagsChanged) {
        return {
            fileId,
            action,
            tags: existingTags,
            existingTags,
            updatedTags,
            changed: false,
            metadata: fileData.metadata
        };
    }
    
    // Update metadata
    fileData.metadata.Tags = updatedTags;
    fileData.metadata.updatedAt = new Date().toISOString();
    
    // Save to database
    await db.put(fileId, fileData.value, {
        metadata: fileData.metadata
    });
    
    // Clear cache
    CacheManager.clear(`tags:${fileId}`);
    
    return {
        fileId,
        action,
        tags: updatedTags,
        existingTags,
        updatedTags,
        changed: true,
        metadata: fileData.metadata
    };
}

/**
 * 处理GET请求 - 获取文件标签
 */
async function handleGetTags(db, fileId) {
    try {
        const result = await getFileTags(db, fileId);
        
        return new Response(JSON.stringify({
            success: true,
            fileId: result.fileId,
            tags: result.tags,
            fromCache: result.fromCache
        }), {
            status: 200,
            headers: {
                'Content-Type': 'application/json',
                'Cache-Control': 'public, max-age=60',
                'X-Cache': result.fromCache ? 'HIT' : 'MISS'
            }
        });
    } catch (error) {
        if (error.message === 'File not found') {
            return new Response(JSON.stringify({
                error: 'File not found',
                fileId: fileId
            }), {
                status: 404,
                headers: { 
                    'Content-Type': 'application/json',
                    'Cache-Control': 'no-store'
                }
            });
        }
        
        throw error;
    }
}

/**
 * 处理POST请求 - 更新文件标签
 */
async function handlePostTags(context, db, fileId, hostname) {
    const { request, waitUntil } = context;
    
    try {
        // Check content type
        const contentType = request.headers.get('Content-Type');
        if (!contentType || !contentType.includes('application/json')) {
            return new Response(JSON.stringify({
                error: 'Unsupported media type',
                message: 'Request must be JSON'
            }), {
                status: 415,
                headers: { 'Content-Type': 'application/json' }
            });
        }
        
        // Parse request body with error handling
        let body;
        try {
            body = await request.json();
        } catch (error) {
            return new Response(JSON.stringify({
                error: 'Invalid JSON',
                message: 'Request body contains invalid JSON',
                details: error.message
            }), {
                status: 400,
                headers: { 'Content-Type': 'application/json' }
            });
        }
        
        // Validate request
        const validation = validateTagRequest(body);
        if (!validation.valid) {
            return new Response(JSON.stringify({
                error: validation.error,
                message: validation.message,
                ...(validation.invalidTags && { invalidTags: validation.invalidTags }),
                ...(validation.duplicateTags && { duplicateTags: validation.duplicateTags })
            }), {
                status: 400,
                headers: { 'Content-Type': 'application/json' }
            });
        }
        
        const { action, tags, options } = validation.params;
        
        // Update tags
        const result = await updateFileTags({
            db,
            fileId,
            action,
            tags,
            options,
            context
        });
        
        // If tags changed, update index and purge cache
        if (result.changed) {
            // Clear CDN cache
            const cdnUrl = `https://${hostname}/file/${fileId}`;
            waitUntil(purgeCFCache(context.env, cdnUrl));
            
            // Update file index
            waitUntil(addFileToIndex(context, fileId, result.metadata));
        }
        
        return new Response(JSON.stringify({
            success: true,
            fileId: result.fileId,
            action: result.action,
            tags: result.tags,
            existingTags: result.existingTags,
            updatedTags: result.updatedTags,
            changed: result.changed,
            metadata: {
                ...result.metadata,
                value: undefined // Don't include file content in response
            }
        }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' }
        });
        
    } catch (error) {
        if (error.message === 'File not found') {
            return new Response(JSON.stringify({
                error: 'File not found',
                fileId: fileId
            }), {
                status: 404,
                headers: { 'Content-Type': 'application/json' }
            });
        }
        
        throw error;
    }
}

/**
 * 处理DELETE请求 - 清除文件标签
 */
async function handleDeleteTags(context, db, fileId, hostname) {
    try {
        // Clear tags (equivalent to action: 'clear')
        const result = await updateFileTags({
            db,
            fileId,
            action: 'clear',
            tags: [],
            options: {},
            context
        });
        
        // If tags changed, update index and purge cache
        if (result.changed) {
            // Clear CDN cache
            const cdnUrl = `https://${hostname}/file/${fileId}`;
            context.waitUntil(purgeCFCache(context.env, cdnUrl));
            
            // Update file index
            context.waitUntil(addFileToIndex(context, fileId, result.metadata));
        }
        
        return new Response(JSON.stringify({
            success: true,
            fileId: result.fileId,
            action: 'clear',
            tags: result.tags,
            existingTags: result.existingTags,
            changed: result.changed
        }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' }
        });
        
    } catch (error) {
        if (error.message === 'File not found') {
            return new Response(JSON.stringify({
                error: 'File not found',
                fileId: fileId
            }), {
                status: 404,
                headers: { 'Content-Type': 'application/json' }
            });
        }
        
        throw error;
    }
}

/**
 * 主API处理函数
 */
export async function onRequest(context) {
    const startTime = performance.now();
    const { request, env, params, waitUntil } = context;
    const url = new URL(request.url);
    
    try {
        // Handle preflight requests
        const preflightResponse = handlePreflightRequest(request);
        if (preflightResponse) {
            return preflightResponse;
        }
        
        // Parse file path
        if (params.path) {
            params.path = String(params.path).split(',').join('/');
        }
        
        // Decode file path
        const fileId = decodeURIComponent(params.path || '');
        
        // Validate file ID
        if (!validateFileId(fileId)) {
            return new Response(JSON.stringify({
                error: 'Invalid file ID',
                message: 'File ID format is invalid'
            }), {
                status: 400,
                headers: { 'Content-Type': 'application/json' }
            });
        }
        
        const db = getDatabase(env);
        
        switch (request.method) {
            case 'GET':
                return await handleGetTags(db, fileId);
                
            case 'POST':
            case 'PATCH':
                return await handlePostTags(context, db, fileId, url.hostname);
                
            case 'DELETE':
                return await handleDeleteTags(context, db, fileId, url.hostname);
                
            default:
                return new Response(JSON.stringify({
                    error: 'Method not allowed',
                    allowedMethods: CONFIG.ALLOWED_METHODS
                }), {
                    status: 405,
                    headers: { 
                        'Content-Type': 'application/json',
                        'Allow': CONFIG.ALLOWED_METHODS.join(', ')
                    }
                });
        }
        
    } catch (error) {
        const duration = performance.now() - startTime;
        console.error(`[Tag API] Error processing ${request.method} ${request.url} (${duration.toFixed(2)}ms):`, error);
        
        return new Response(JSON.stringify({
            error: 'Internal server error',
            message: env.DEBUG_MODE === 'true' ? error.message : 'An unexpected error occurred',
            processingTime: `${duration.toFixed(2)}ms`
        }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
        });
    }
}

/**
 * 清除标签缓存（用于管理API）
 * @param {string} [fileId] - 特定文件ID，不提供则清除所有
 * @returns {Object} 清除结果
 */
export function clearTagCache(fileId) {
    if (fileId) {
        CacheManager.clear(`tags:${fileId}`);
        return { success: true, cleared: 1 };
    }
    
    const cacheSize = CacheManager.cache.size;
    CacheManager.clear();
    return { success: true, cleared: cacheSize };
}
