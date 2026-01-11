/**
 * Debug version of Tag Management API with enhanced error handling
 * This version adds more detailed error logging and validation
 */

import { purgeCFCache } from "../../../utils/purgeCache.js";
import { addFileToIndex } from "../../../utils/indexManager.js";
import { getDatabase } from "../../../utils/databaseAdapter.js";
import { mergeTags, normalizeTags, validateTag } from "../../../utils/tagHelpers.js";

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
    cache: new Map(),
    
    get(key) {
        const entry = this.cache.get(key);
        if (!entry) return null;
        
        if (Date.now() > entry.expires) {
            this.cache.delete(key);
            return null;
        }
        
        return entry.data;
    },
    
    set(key, data, ttl = CONFIG.CACHE_TTL) {
        this.cache.set(key, {
            data,
            expires: Date.now() + (ttl * 1000)
        });
        
        if (this.cache.size > 1000) {
            const oldestKey = this.cache.keys().next().value;
            this.cache.delete(oldestKey);
        }
    },
    
    clear(key) {
        if (key) {
            this.cache.delete(key);
        } else {
            this.cache.clear();
        }
    }
};

/**
 * 安全的JSON字符串化函数
 * 处理循环引用和特殊值
 */
function safeStringify(obj) {
    try {
        return JSON.stringify(obj, (key, value) => {
            if (typeof value === 'bigint') {
                return value.toString();
            }
            if (value === undefined) {
                return null;
            }
            return value;
        });
    } catch (error) {
        console.error('JSON stringify error:', error);
        console.error('Object causing error:', obj);
        return JSON.stringify({
            error: 'Serialization error',
            message: 'Failed to serialize response',
            details: error.message
        });
    }
}

/**
 * 处理预检请求
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
 */
function validateFileId(fileId) {
    if (!fileId || typeof fileId !== 'string') {
        return false;
    }
    
    const fileIdRegex = /^[a-zA-Z0-9_\-]{8,64}$/;
    return fileIdRegex.test(fileId);
}

/**
 * 验证标签请求
 */
function validateTagRequest(requestBody) {
    console.log('Validating tag request:', requestBody);
    
    if (!requestBody || typeof requestBody !== 'object') {
        return {
            valid: false,
            error: 'Invalid request body',
            message: 'Request body must be a JSON object'
        };
    }
    
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
    if (action !== 'clear') {
        if (!Array.isArray(tags)) {
            return {
                valid: false,
                error: 'Invalid tags format',
                message: 'Tags must be an array for this action'
            };
        }
        
        if (tags.length === 0) {
            return {
                valid: false,
                error: 'Empty tags array',
                message: 'Tags array must not be empty for this action'
            };
        }
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
 */
async function getFileTags(db, fileId, bypassCache = false) {
    console.log(`Getting tags for file: ${fileId}`);
    
    const cacheKey = `tags:${fileId}`;
    
    // Check cache first
    if (!bypassCache) {
        const cached = CacheManager.get(cacheKey);
        if (cached) {
            console.log(`Cache hit for file ${fileId}`);
            return {
                ...cached,
                fromCache: true
            };
        }
    }
    
    try {
        // Get from database
        console.log(`Querying database for file ${fileId}`);
        const fileData = await db.getWithMetadata(fileId);
        
        console.log(`Database response for ${fileId}:`, fileData);
        
        if (!fileData) {
            console.error(`File ${fileId} not found in database`);
            throw new Error('File not found');
        }
        
        if (!fileData.metadata) {
            console.warn(`File ${fileId} has no metadata, initializing empty metadata`);
            fileData.metadata = {};
        }
        
        const tags = fileData.metadata.Tags || [];
        console.log(`Found tags for ${fileId}:`, tags);
        
        const result = {
            fileId,
            tags,
            metadata: fileData.metadata,
            fromCache: false
        };
        
        // Cache the result
        CacheManager.set(cacheKey, result);
        
        return result;
        
    } catch (error) {
        console.error(`Error getting tags for ${fileId}:`, error);
        throw error;
    }
}

/**
 * 更新文件标签
 */
async function updateFileTags({
    db,
    fileId,
    action,
    tags,
    options,
    context
}) {
    console.log(`Updating tags for file ${fileId}:`, { action, tags, options });
    
    const { waitUntil, env } = context;
    
    try {
        // Get current file data
        console.log(`Getting current file data for ${fileId}`);
        const fileData = await db.getWithMetadata(fileId);
        
        console.log(`Current file data for ${fileId}:`, fileData);
        
        if (!fileData) {
            console.error(`File ${fileId} not found in database`);
            throw new Error('File not found');
        }
        
        // Initialize metadata if it doesn't exist
        if (!fileData.metadata) {
            console.warn(`File ${fileId} has no metadata, creating new metadata object`);
            fileData.metadata = {};
        }
        
        // Get existing tags
        const existingTags = fileData.metadata.Tags || [];
        console.log(`Existing tags for ${fileId}:`, existingTags);
        
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
        console.log(`Updated tags for ${fileId}:`, updatedTags);
        
        // Check if tags actually changed
        const tagsChanged = JSON.stringify(existingTags.sort()) !== JSON.stringify(updatedTags.sort());
        console.log(`Tags changed for ${fileId}:`, tagsChanged);
        
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
        console.log(`Updating metadata for ${fileId}:`, fileData.metadata);
        
        // Save to database
        console.log(`Saving updated tags to database for ${fileId}`);
        await db.put(fileId, fileData.value, {
            metadata: fileData.metadata
        });
        
        // Clear cache
        CacheManager.clear(`tags:${fileId}`);
        console.log(`Cache cleared for ${fileId}`);
        
        return {
            fileId,
            action,
            tags: updatedTags,
            existingTags,
            updatedTags,
            changed: true,
            metadata: fileData.metadata
        };
        
    } catch (error) {
        console.error(`Error updating tags for ${fileId}:`, error);
        throw error;
    }
}

/**
 * 处理GET请求 - 获取文件标签
 */
async function handleGetTags(db, fileId) {
    try {
        console.log(`Handling GET request for file ${fileId}`);
        const result = await getFileTags(db, fileId);
        
        const responseBody = {
            success: true,
            fileId: result.fileId,
            tags: result.tags,
            fromCache: result.fromCache
        };
        
        console.log(`GET response for ${fileId}:`, responseBody);
        
        return new Response(safeStringify(responseBody), {
            status: 200,
            headers: {
                'Content-Type': 'application/json',
                'Cache-Control': 'public, max-age=60',
                'X-Cache': result.fromCache ? 'HIT' : 'MISS'
            }
        });
    } catch (error) {
        console.error(`Error handling GET for ${fileId}:`, error);
        
        if (error.message === 'File not found') {
            return new Response(safeStringify({
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
        
        return new Response(safeStringify({
            error: 'Internal server error',
            message: 'Failed to get tags',
            details: error.message
        }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
        });
    }
}

/**
 * 处理POST请求 - 更新文件标签
 */
async function handlePostTags(context, db, fileId, hostname) {
    const { request, waitUntil } = context;
    
    try {
        console.log(`Handling POST request for file ${fileId}`);
        
        // Check content type
        const contentType = request.headers.get('Content-Type');
        if (!contentType || !contentType.includes('application/json')) {
            console.error(`Unsupported content type: ${contentType}`);
            return new Response(safeStringify({
                error: 'Unsupported media type',
                message: 'Request must be JSON',
                receivedType: contentType
            }), {
                status: 415,
                headers: { 'Content-Type': 'application/json' }
            });
        }
        
        // Parse request body with error handling
        let body;
        try {
            console.log('Parsing request body');
            const bodyText = await request.text();
            console.log('Request body text:', bodyText);
            
            if (!bodyText.trim()) {
                throw new Error('Empty request body');
            }
            
            body = JSON.parse(bodyText);
            console.log('Parsed request body:', body);
            
        } catch (error) {
            console.error('JSON parse error:', error);
            return new Response(safeStringify({
                error: 'Invalid JSON',
                message: 'Request body contains invalid JSON',
                details: error.message,
                receivedBody: await request.text().catch(() => 'Unable to read body')
            }), {
                status: 400,
                headers: { 'Content-Type': 'application/json' }
            });
        }
        
        // Validate request
        const validation = validateTagRequest(body);
        if (!validation.valid) {
            console.error('Request validation failed:', validation);
            return new Response(safeStringify({
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
        console.log(`Validated parameters:`, { action, tags, options });
        
        // Update tags
        const result = await updateFileTags({
            db,
            fileId,
            action,
            tags,
            options,
            context
        });
        
        console.log(`Update result for ${fileId}:`, result);
        
        // If tags changed, update index and purge cache
        if (result.changed) {
            console.log(`Tags changed, updating index and cache for ${fileId}`);
            // Clear CDN cache
            const cdnUrl = `https://${hostname}/file/${fileId}`;
            waitUntil(purgeCFCache(context.env, cdnUrl).catch(err => 
                console.error(`Cache purge error:`, err)
            ));
            
            // Update file index
            waitUntil(addFileToIndex(context, fileId, result.metadata).catch(err => 
                console.error(`Index update error:`, err)
            ));
        }
        
        const responseBody = {
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
        };
        
        console.log(`POST response for ${fileId}:`, responseBody);
        
        return new Response(safeStringify(responseBody), {
            status: 200,
            headers: { 'Content-Type': 'application/json' }
        });
        
    } catch (error) {
        console.error(`Error handling POST for ${fileId}:`, error);
        
        if (error.message === 'File not found') {
            return new Response(safeStringify({
                error: 'File not found',
                fileId: fileId
            }), {
                status: 404,
                headers: { 'Content-Type': 'application/json' }
            });
        }
        
        return new Response(safeStringify({
            error: 'Internal server error',
            message: 'Failed to update tags',
            details: error.message,
            stack: error.stack
        }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
        });
    }
}

/**
 * 处理DELETE请求 - 清除文件标签
 */
async function handleDeleteTags(context, db, fileId, hostname) {
    try {
        console.log(`Handling DELETE request for file ${fileId}`);
        
        // Clear tags (equivalent to action: 'clear')
        const result = await updateFileTags({
            db,
            fileId,
            action: 'clear',
            tags: [],
            options: {},
            context
        });
        
        console.log(`Delete result for ${fileId}:`, result);
        
        // If tags changed, update index and purge cache
        if (result.changed) {
            console.log(`Tags cleared, updating index and cache for ${fileId}`);
            // Clear CDN cache
            const cdnUrl = `https://${hostname}/file/${fileId}`;
            context.waitUntil(purgeCFCache(context.env, cdnUrl).catch(err => 
                console.error(`Cache purge error:`, err)
            ));
            
            // Update file index
            context.waitUntil(addFileToIndex(context, fileId, result.metadata).catch(err => 
                console.error(`Index update error:`, err)
            ));
        }
        
        return new Response(safeStringify({
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
        console.error(`Error handling DELETE for ${fileId}:`, error);
        
        if (error.message === 'File not found') {
            return new Response(safeStringify({
                error: 'File not found',
                fileId: fileId
            }), {
                status: 404,
                headers: { 'Content-Type': 'application/json' }
            });
        }
        
        return new Response(safeStringify({
            error: 'Internal server error',
            message: 'Failed to clear tags',
            details: error.message
        }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
        });
    }
}

/**
 * 主API处理函数
 */
export async function onRequest(context) {
    const startTime = performance.now();
    const { request, env, params, waitUntil } = context;
    const url = new URL(request.url);
    
    console.log(`=== Tag API Request ===`);
    console.log(`Method: ${request.method}`);
    console.log(`URL: ${request.url}`);
    console.log(`Params:`, params);
    
    try {
        // Handle preflight requests
        const preflightResponse = handlePreflightRequest(request);
        if (preflightResponse) {
            console.log(`Handling preflight request`);
            return preflightResponse;
        }
        
        // Parse file path
        if (params.path) {
            params.path = String(params.path).split(',').join('/');
        }
        
        // Decode file path
        const fileId = decodeURIComponent(params.path || '');
        console.log(`File ID: ${fileId}`);
        
        // Validate file ID
        if (!validateFileId(fileId)) {
            console.error(`Invalid file ID: ${fileId}`);
            return new Response(safeStringify({
                error: 'Invalid file ID',
                message: 'File ID format is invalid',
                fileId: fileId
            }), {
                status: 400,
                headers: { 'Content-Type': 'application/json' }
            });
        }
        
        const db = getDatabase(env);
        console.log(`Database instance created`);
        
        switch (request.method) {
            case 'GET':
                return await handleGetTags(db, fileId);
                
            case 'POST':
            case 'PATCH':
                return await handlePostTags(context, db, fileId, url.hostname);
                
            case 'DELETE':
                return await handleDeleteTags(context, db, fileId, url.hostname);
                
            default:
                console.error(`Method not allowed: ${request.method}`);
                return new Response(safeStringify({
                    error: 'Method not allowed',
                    allowedMethods: CONFIG.ALLOWED_METHODS,
                    receivedMethod: request.method
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
        console.error(`=== Tag API Error (${duration.toFixed(2)}ms) ===`);
        console.error(`Error:`, error);
        console.error(`Stack:`, error.stack);
        
        return new Response(safeStringify({
            error: 'Internal server error',
            message: 'An unexpected error occurred',
            details: env.DEBUG_MODE === 'true' ? error.message : undefined,
            stack: env.DEBUG_MODE === 'true' ? error.stack : undefined,
            processingTime: `${duration.toFixed(2)}ms`
        }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
        });
    } finally {
        const duration = performance.now() - startTime;
        console.log(`=== Request completed in ${duration.toFixed(2)}ms ===`);
    }
}

/**
 * 清除标签缓存（用于管理API）
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
