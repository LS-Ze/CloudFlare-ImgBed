/**
 * Tag Management API with Path Handling Fixes
 * 
 * This version fixes:
 * 1. File ID path handling (supports IDs with /img/ prefix)
 * 2. Authentication issues
 * 3. JSON response formatting
 * 4. CORS and error handling
 */

import { purgeCFCache } from "../../../utils/purgeCache.js";
import { addFileToIndex } from "../../../utils/indexManager.js";
import { getDatabase } from "../../../utils/databaseAdapter.js";
import { mergeTags, normalizeTags, validateTag } from "../../../utils/tagHelpers.js";

/**
 * 安全的JSON字符串化函数
 * 确保始终返回有效的JSON
 */
function safeStringify(obj) {
    try {
        return JSON.stringify(obj, (key, value) => {
            if (typeof value === 'bigint') return value.toString();
            if (value === undefined) return null;
            return value;
        });
    } catch (error) {
        console.error('JSON serialization error:', error);
        return JSON.stringify({
            error: 'Serialization error',
            message: 'Failed to serialize response',
            details: error.message
        });
    }
}

/**
 * 配置常量
 */
const CONFIG = {
    CACHE_TTL: 300,
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
            this.cache.delete(this.cache.keys().next().value);
        }
    },
    
    clear(key) {
        key ? this.cache.delete(key) : this.cache.clear();
    }
};

/**
 * 处理CORS和预检请求
 */
function handleCORS(request) {
    const headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': CONFIG.ALLOWED_METHODS.join(', '),
        'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
        'Access-Control-Max-Age': '86400'
    };

    if (request.method === 'OPTIONS') {
        return new Response(null, { 
            status: 204, 
            headers 
        });
    }

    return headers;
}

/**
 * 清理文件ID（移除路径前缀）
 * @param {string} fileId - 原始文件ID
 * @returns {string} 清理后的文件ID
 */
function cleanFileId(fileId) {
    if (!fileId) return '';
    
    // 移除常见的路径前缀
    const cleaned = fileId
        .replace(/^img\//i, '')       // 移除 img/ 前缀
        .replace(/^files\//i, '')     // 移除 files/ 前缀
        .replace(/^uploads\//i, '')   // 移除 uploads/ 前缀
        .replace(/^images\//i, '')    // 移除 images/ 前缀
        .replace(/^\//, '');          // 移除开头的斜杠
    
    console.log(`Cleaned file ID: "${fileId}" -> "${cleaned}"`);
    return cleaned;
}

/**
 * 验证文件ID格式
 */
function validateFileId(fileId) {
    if (!fileId || typeof fileId !== 'string') {
        return false;
    }
    
    // 支持包含路径的文件ID格式
    const fileIdRegex = /^[a-zA-Z0-9_\-\/\.]{8,128}$/;
    return fileIdRegex.test(fileId);
}

/**
 * 验证标签请求
 */
function validateTagRequest(requestBody) {
    if (!requestBody || typeof requestBody !== 'object') {
        return {
            valid: false,
            error: 'Invalid request',
            message: 'Request body must be a JSON object'
        };
    }

    const { action = 'set', tags = [], options = {} } = requestBody;

    if (!CONFIG.SUPPORTED_ACTIONS.includes(action)) {
        return {
            valid: false,
            error: 'Invalid action',
            message: `Action must be one of: ${CONFIG.SUPPORTED_ACTIONS.join(', ')}`
        };
    }

    if (action !== 'clear' && (!Array.isArray(tags) || tags.length === 0)) {
        return {
            valid: false,
            error: 'Invalid tags',
            message: 'Tags must be a non-empty array'
        };
    }

    const validOptions = {
        caseSensitive: typeof options.caseSensitive === 'boolean' ? options.caseSensitive : false,
        unique: typeof options.unique === 'boolean' ? options.unique : true,
        normalize: typeof options.normalize === 'boolean' ? options.normalize : true,
        validate: typeof options.validate === 'boolean' ? options.validate : true
    };

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
        if (!validateTag(tag)) {
            invalidTags.push(tag);
            continue;
        }

        if (tag.length < CONFIG.MIN_TAG_LENGTH || tag.length > CONFIG.MAX_TAG_LENGTH) {
            invalidTags.push(tag);
            continue;
        }

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
            message: `Tags must be ${CONFIG.MIN_TAG_LENGTH}-${CONFIG.MAX_TAG_LENGTH} characters`,
            invalidTags
        };
    }

    if (duplicateTags.length > 0 && options.unique) {
        return {
            valid: false,
            error: 'Duplicate tags',
            message: 'Duplicate tags found',
            duplicateTags
        };
    }

    return { valid: true };
}

/**
 * 处理标签
 */
function processTags(tags, options) {
    if (!tags || tags.length === 0) return [];

    let processed = [...tags];

    if (options.normalize) {
        processed = normalizeTags(processed);
    }

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
    const cleanedId = cleanFileId(fileId);
    const cacheKey = `tags:${cleanedId}`;

    if (!bypassCache) {
        const cached = CacheManager.get(cacheKey);
        if (cached) {
            return { ...cached, fromCache: true };
        }
    }

    try {
        console.log(`Getting tags for file: ${cleanedId}`);
        const fileData = await db.getWithMetadata(cleanedId);

        if (!fileData) {
            console.error(`File ${cleanedId} not found`);
            throw new Error('File not found');
        }

        if (!fileData.metadata) {
            fileData.metadata = {};
        }

        const tags = fileData.metadata.Tags || [];
        const result = {
            fileId: cleanedId,
            originalFileId: fileId,
            tags,
            metadata: fileData.metadata,
            fromCache: false
        };

        CacheManager.set(cacheKey, result);
        return result;

    } catch (error) {
        console.error(`Error getting tags:`, error);
        throw error;
    }
}

/**
 * 更新文件标签
 */
async function updateFileTags({ db, fileId, action, tags, options, context }) {
    const cleanedId = cleanFileId(fileId);
    const { waitUntil, env } = context;

    try {
        console.log(`Updating tags for ${cleanedId}:`, { action, tags, options });
        const fileData = await db.getWithMetadata(cleanedId);

        if (!fileData) {
            console.error(`File ${cleanedId} not found`);
            throw new Error('File not found');
        }

        if (!fileData.metadata) {
            fileData.metadata = {};
        }

        const existingTags = fileData.metadata.Tags || [];
        let updatedTags = [...existingTags];

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
                updatedTags = existingTags.filter(tag => 
                    !tags.some(t => 
                        options.caseSensitive ? t === tag : t.toLowerCase() === tag.toLowerCase()
                    )
                ).concat(tags);
                break;
            case 'toggle':
                updatedTags = existingTags.filter(tag => 
                    !tags.some(t => 
                        options.caseSensitive ? t === tag : t.toLowerCase() === tag.toLowerCase()
                    )
                );
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

        updatedTags = [...new Set(updatedTags.filter(tag => tag && tag.trim() !== ''))];
        const tagsChanged = JSON.stringify(existingTags.sort()) !== JSON.stringify(updatedTags.sort());

        if (!tagsChanged) {
            return {
                fileId: cleanedId,
                originalFileId: fileId,
                action,
                tags: existingTags,
                existingTags,
                updatedTags,
                changed: false,
                metadata: fileData.metadata
            };
        }

        fileData.metadata.Tags = updatedTags;
        fileData.metadata.updatedAt = new Date().toISOString();

        await db.put(cleanedId, fileData.value, {
            metadata: fileData.metadata
        });

        CacheManager.clear(`tags:${cleanedId}`);

        return {
            fileId: cleanedId,
            originalFileId: fileId,
            action,
            tags: updatedTags,
            existingTags,
            updatedTags,
            changed: true,
            metadata: fileData.metadata
        };

    } catch (error) {
        console.error(`Error updating tags:`, error);
        throw error;
    }
}

/**
 * 处理GET请求
 */
async function handleGetTags(db, fileId) {
    try {
        const result = await getFileTags(db, fileId);

        return {
            status: 200,
            body: {
                success: true,
                fileId: result.fileId,
                originalFileId: result.originalFileId,
                tags: result.tags,
                fromCache: result.fromCache
            }
        };

    } catch (error) {
        console.error(`GET error:`, error);
        
        if (error.message === 'File not found') {
            return {
                status: 404,
                body: {
                    error: 'File not found',
                    fileId: cleanFileId(fileId),
                    originalFileId: fileId
                }
            };
        }

        return {
            status: 500,
            body: {
                error: 'Internal server error',
                message: 'Failed to get tags',
                details: error.message
            }
        };
    }
}

/**
 * 处理POST请求
 */
async function handlePostTags(context, db, fileId, hostname) {
    const { request, waitUntil } = context;

    try {
        const contentType = request.headers.get('Content-Type');
        if (!contentType || !contentType.includes('application/json')) {
            return {
                status: 415,
                body: {
                    error: 'Unsupported media type',
                    message: 'Request must be JSON',
                    receivedType: contentType
                }
            };
        }

        let body;
        try {
            const bodyText = await request.text();
            console.log('Request body:', bodyText);
            
            if (!bodyText.trim()) {
                throw new Error('Empty request body');
            }
            
            body = JSON.parse(bodyText);
            console.log('Parsed body:', body);

        } catch (error) {
            console.error('JSON parse error:', error);
            return {
                status: 400,
                body: {
                    error: 'Invalid JSON',
                    message: 'Request body contains invalid JSON',
                    details: error.message
                }
            };
        }

        const validation = validateTagRequest(body);
        if (!validation.valid) {
            console.error('Validation failed:', validation);
            return {
                status: 400,
                body: {
                    error: validation.error,
                    message: validation.message,
                    ...(validation.invalidTags && { invalidTags: validation.invalidTags }),
                    ...(validation.duplicateTags && { duplicateTags: validation.duplicateTags })
                }
            };
        }

        const { action, tags, options } = validation.params;
        const result = await updateFileTags({
            db,
            fileId,
            action,
            tags,
            options,
            context
        });

        if (result.changed) {
            const cdnUrl = `https://${hostname}/file/${result.fileId}`;
            waitUntil(purgeCFCache(context.env, cdnUrl).catch(err => 
                console.error(`Cache purge error:`, err)
            ));

            waitUntil(addFileToIndex(context, result.fileId, result.metadata).catch(err => 
                console.error(`Index update error:`, err)
            ));
        }

        return {
            status: 200,
            body: {
                success: true,
                fileId: result.fileId,
                originalFileId: result.originalFileId,
                action: result.action,
                tags: result.tags,
                existingTags: result.existingTags,
                updatedTags: result.updatedTags,
                changed: result.changed
            }
        };

    } catch (error) {
        console.error(`POST error:`, error);
        
        if (error.message === 'File not found') {
            return {
                status: 404,
                body: {
                    error: 'File not found',
                    fileId: cleanFileId(fileId),
                    originalFileId: fileId
                }
            };
        }

        return {
            status: 500,
            body: {
                error: 'Internal server error',
                message: 'Failed to update tags',
                details: error.message
            }
        };
    }
}

/**
 * 处理DELETE请求
 */
async function handleDeleteTags(context, db, fileId, hostname) {
    try {
        const result = await updateFileTags({
            db,
            fileId,
            action: 'clear',
            tags: [],
            options: {},
            context
        });

        if (result.changed) {
            const cdnUrl = `https://${hostname}/file/${result.fileId}`;
            context.waitUntil(purgeCFCache(context.env, cdnUrl).catch(err => 
                console.error(`Cache purge error:`, err)
            ));

            context.waitUntil(addFileToIndex(context, result.fileId, result.metadata).catch(err => 
                console.error(`Index update error:`, err)
            ));
        }

        return {
            status: 200,
            body: {
                success: true,
                fileId: result.fileId,
                originalFileId: result.originalFileId,
                action: 'clear',
                tags: result.tags,
                existingTags: result.existingTags,
                changed: result.changed
            }
        };

    } catch (error) {
        console.error(`DELETE error:`, error);
        
        if (error.message === 'File not found') {
            return {
                status: 404,
                body: {
                    error: 'File not found',
                    fileId: cleanFileId(fileId),
                    originalFileId: fileId
                }
            };
        }

        return {
            status: 500,
            body: {
                error: 'Internal server error',
                message: 'Failed to clear tags',
                details: error.message
            }
        };
    }
}

/**
 * 主API处理函数
 */
export async function onRequest(context) {
    const startTime = performance.now();
    const { request, env, params } = context;
    const url = new URL(request.url);

    console.log(`=== Tag API Request ===`);
    console.log(`Method: ${request.method}`);
    console.log(`URL: ${request.url}`);
    console.log(`Params:`, params);

    try {
        // 处理CORS
        const corsHeaders = handleCORS(request);
        if (corsHeaders instanceof Response) {
            return corsHeaders;
        }

        // 解析文件路径
        let fileId = '';
        if (params.path) {
            fileId = String(params.path).split(',').join('/');
        }
        fileId = decodeURIComponent(fileId || '');

        console.log(`Original file ID: "${fileId}"`);

        // 验证文件ID
        if (!validateFileId(fileId)) {
            console.error(`Invalid file ID: "${fileId}"`);
            return new Response(safeStringify({
                error: 'Invalid file ID',
                message: 'File ID format is invalid',
                fileId: fileId
            }), {
                status: 400,
                headers: { 
                    ...corsHeaders,
                    'Content-Type': 'application/json'
                }
            });
        }

        const db = getDatabase(env);
        let response;

        switch (request.method) {
            case 'GET':
                response = await handleGetTags(db, fileId);
                break;
                
            case 'POST':
            case 'PATCH':
                response = await handlePostTags(context, db, fileId, url.hostname);
                break;
                
            case 'DELETE':
                response = await handleDeleteTags(context, db, fileId, url.hostname);
                break;
                
            default:
                response = {
                    status: 405,
                    body: {
                        error: 'Method not allowed',
                        allowedMethods: CONFIG.ALLOWED_METHODS,
                        receivedMethod: request.method
                    }
                };
        }

        const duration = performance.now() - startTime;
        console.log(`=== Response (${duration.toFixed(2)}ms) ===`);
        console.log(`Status: ${response.status}`);
        console.log(`Body:`, response.body);

        return new Response(safeStringify(response.body), {
            status: response.status,
            headers: {
                ...corsHeaders,
                'Content-Type': 'application/json',
                'X-Processing-Time': `${duration.toFixed(2)}ms`,
                'Cache-Control': response.status === 200 ? 'public, max-age=60' : 'no-store'
            }
        });

    } catch (error) {
        const duration = performance.now() - startTime;
        console.error(`=== Fatal Error (${duration.toFixed(2)}ms) ===`);
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
            headers: {
                ...handleCORS(request),
                'Content-Type': 'application/json',
                'Cache-Control': 'no-store'
            }
        });
    }
}

/**
 * 清除标签缓存
 */
export function clearTagCache(fileId) {
    if (fileId) {
        CacheManager.clear(`tags:${cleanFileId(fileId)}`);
        return { success: true, cleared: 1 };
    }
    
    const cacheSize = CacheManager.cache.size;
    CacheManager.clear();
    return { success: true, cleared: cacheSize };
}
