/**
 * Enhanced Tag Management API with Debug Features
 * 
 * This version includes:
 * 1. Comprehensive error handling
 * 2. Detailed debug logging
 * 3. Robust JSON response handling
 * 4. Input validation improvements
 * 5. Better error messages
 */

import { purgeCFCache } from "../../../utils/purgeCache.js";
import { addFileToIndex } from "../../../utils/indexManager.js";
import { getDatabase } from "../../../utils/databaseAdapter.js";
import { mergeTags, normalizeTags, validateTag } from "../../../utils/tagHelpers.js";

/**
 * 全局配置
 */
const CONFIG = {
    CACHE_TTL: 300,
    MAX_TAG_LENGTH: 100,
    MIN_TAG_LENGTH: 1,
    SUPPORTED_ACTIONS: ['set', 'add', 'remove', 'replace', 'toggle', 'clear'],
    ALLOWED_METHODS: ['GET', 'POST', 'DELETE', 'PATCH', 'OPTIONS'],
    DEBUG_MODE: true, // Force debug mode for troubleshooting
    ALLOW_ANONYMOUS: false,
    ALLOWED_ORIGINS: ['https://hub.lsdns.top', 'https://curl.img.lsdns.top'],
    MAX_REQUEST_SIZE: 1024 * 1024, // 1MB
    REQUEST_TIMEOUT: 30000 // 30 seconds
};

/**
 * 安全的JSON字符串化函数
 */
function safeStringify(obj) {
    try {
        return JSON.stringify(obj, (key, value) => {
            if (typeof value === 'bigint') return value.toString();
            if (value === undefined) return null;
            if (value instanceof Error) {
                return {
                    message: value.message,
                    stack: CONFIG.DEBUG_MODE ? value.stack : undefined,
                    name: value.name
                };
            }
            return value;
        }, CONFIG.DEBUG_MODE ? 2 : 0);
    } catch (error) {
        console.error('JSON serialization error:', error);
        return JSON.stringify({
            error: 'Serialization error',
            message: 'Failed to serialize response',
            details: CONFIG.DEBUG_MODE ? error.message : undefined,
            timestamp: Date.now()
        }, null, 2);
    }
}

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
            expires: Date.now() + (ttl * 1000),
            timestamp: Date.now()
        });
        if (this.cache.size > 1000) {
            const oldestKey = Array.from(this.cache.entries())
                .sort((a, b) => a[1].timestamp - b[1].timestamp)[0][0];
            this.cache.delete(oldestKey);
        }
    },
    
    clear(key) {
        key ? this.cache.delete(key) : this.cache.clear();
    }
};

/**
 * 日志工具
 */
const Logger = {
    log(...args) {
        console.log('[Tag API]', new Date().toISOString(), ...args);
    },
    
    error(...args) {
        console.error('[Tag API ERROR]', new Date().toISOString(), ...args);
    },
    
    warn(...args) {
        console.warn('[Tag API WARN]', new Date().toISOString(), ...args);
    },
    
    debug(...args) {
        if (CONFIG.DEBUG_MODE) {
            console.debug('[Tag API DEBUG]', new Date().toISOString(), ...args);
        }
    }
};

/**
 * 清理文件ID
 */
function cleanFileId(fileId) {
    if (!fileId) return '';
    
    const cleaned = fileId
        .replace(/^img\//i, '')
        .replace(/^files\//i, '')
        .replace(/^uploads\//i, '')
        .replace(/^images\//i, '')
        .replace(/^\//, '')
        .replace(/\?.*/, '')
        .replace(/\/+/g, '/')
        .trim();
    
    Logger.debug(`File ID cleaned: "${fileId}" -> "${cleaned}"`);
    return cleaned;
}

/**
 * 验证文件ID
 */
function validateFileId(fileId) {
    if (!fileId || typeof fileId !== 'string') {
        return false;
    }
    
    const fileIdRegex = /^[a-zA-Z0-9_\-\/\.]{8,128}$/;
    return fileIdRegex.test(fileId);
}

/**
 * 验证认证
 */
function validateAuth(request, env) {
    if (CONFIG.ALLOW_ANONYMOUS) {
        Logger.debug('Anonymous access allowed');
        return { valid: true };
    }
    
    // Check API token
    const authHeader = request.headers.get('Authorization');
    if (authHeader) {
        const token = authHeader.replace(/^Bearer\s+/i, '');
        if (token && token === env.API_TOKEN) {
            Logger.debug('API token authentication successful');
            return { valid: true };
        }
        Logger.debug('Invalid API token');
    }
    
    // Check cookie authentication
    const cookieHeader = request.headers.get('Cookie');
    if (cookieHeader && cookieHeader.includes('auth_token=')) {
        const tokenMatch = cookieHeader.match(/auth_token=([^;]+)/);
        if (tokenMatch && tokenMatch[1] === env.AUTH_TOKEN) {
            Logger.debug('Cookie authentication successful');
            return { valid: true };
        }
        Logger.debug('Invalid cookie token');
    }
    
    Logger.error('Authentication failed - no valid credentials provided');
    return {
        valid: false,
        error: 'Unauthorized',
        message: 'Authentication required'
    };
}

/**
 * 处理CORS
 */
function handleCORS(request) {
    const origin = request.headers.get('Origin') || '';
    const allowedOrigin = CONFIG.ALLOWED_ORIGINS.includes(origin) ? origin : '*';
    
    const headers = {
        'Access-Control-Allow-Origin': allowedOrigin,
        'Access-Control-Allow-Methods': CONFIG.ALLOWED_METHODS.join(', '),
        'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With, X-Debug',
        'Access-Control-Allow-Credentials': 'true',
        'Access-Control-Max-Age': '86400'
    };

    if (request.method === 'OPTIONS') {
        Logger.debug('Handling OPTIONS preflight request');
        return new Response(null, { 
            status: 204, 
            headers 
        });
    }

    return headers;
}

/**
 * 验证标签请求
 */
function validateTagRequest(requestBody) {
    Logger.debug('Validating tag request:', requestBody);
    
    if (!requestBody || typeof requestBody !== 'object') {
        return {
            valid: false,
            error: 'Invalid request',
            message: 'Request body must be a JSON object',
            details: 'Request body is not a valid JSON object'
        };
    }

    const { action = 'set', tags = [], options = {} } = requestBody;

    if (!CONFIG.SUPPORTED_ACTIONS.includes(action)) {
        return {
            valid: false,
            error: 'Invalid action',
            message: `Action must be one of: ${CONFIG.SUPPORTED_ACTIONS.join(', ')}`,
            details: `Received action: ${action}`
        };
    }

    if (action !== 'clear' && (!Array.isArray(tags) || tags.length === 0)) {
        return {
            valid: false,
            error: 'Invalid tags',
            message: 'Tags must be a non-empty array',
            details: `Received tags: ${JSON.stringify(tags)}`
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
            message: `Tags must be ${CONFIG.MIN_TAG_LENGTH}-${CONFIG.MAX_TAG_LENGTH} characters and contain only valid characters`,
            invalidTags,
            details: `Invalid tags: ${invalidTags.join(', ')}`
        };
    }

    if (duplicateTags.length > 0 && options.unique) {
        return {
            valid: false,
            error: 'Duplicate tags',
            message: 'Duplicate tags found in request',
            duplicateTags,
            details: `Duplicate tags: ${duplicateTags.join(', ')}`
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
            Logger.debug(`Cache hit for ${cleanedId}`);
            return { ...cached, fromCache: true };
        }
    }

    try {
        Logger.debug(`Getting tags from database for ${cleanedId}`);
        const fileData = await db.getWithMetadata(cleanedId);

        if (!fileData) {
            Logger.error(`File ${cleanedId} not found in database`);
            throw new Error('File not found');
        }

        if (!fileData.metadata) {
            Logger.warn(`File ${cleanedId} has no metadata, initializing empty`);
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
        Logger.error(`Error getting tags:`, error);
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
        Logger.debug(`Updating tags for ${cleanedId}:`, { action, tags, options });
        const fileData = await db.getWithMetadata(cleanedId);

        if (!fileData) {
            Logger.error(`File ${cleanedId} not found in database`);
            throw new Error('File not found');
        }

        if (!fileData.metadata) {
            Logger.warn(`File ${cleanedId} has no metadata, creating new`);
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
            Logger.debug(`No tag changes for ${cleanedId}`);
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
        Logger.debug(`Tags updated for ${cleanedId}:`, updatedTags);

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
        Logger.error(`Error updating tags:`, error);
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
                fromCache: result.fromCache,
                timestamp: Date.now()
            }
        };

    } catch (error) {
        Logger.error(`GET error:`, error);
        
        if (error.message === 'File not found') {
            return {
                status: 404,
                body: {
                    error: 'File not found',
                    fileId: cleanFileId(fileId),
                    originalFileId: fileId,
                    message: 'The requested file was not found in the database',
                    timestamp: Date.now()
                }
            };
        }

        return {
            status: 500,
            body: {
                error: 'Internal server error',
                message: 'Failed to get tags',
                details: CONFIG.DEBUG_MODE ? error.message : undefined,
                timestamp: Date.now()
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
            Logger.error(`Unsupported content type: ${contentType}`);
            return {
                status: 415,
                body: {
                    error: 'Unsupported media type',
                    message: 'Request must be JSON',
                    receivedType: contentType,
                    expectedType: 'application/json',
                    timestamp: Date.now()
                }
            };
        }

        let body;
        try {
            const bodyText = await request.text();
            Logger.debug('Request body text:', bodyText);
            
            if (!bodyText.trim()) {
                throw new Error('Empty request body');
            }
            
            body = JSON.parse(bodyText);
            Logger.debug('Parsed request body:', body);

        } catch (error) {
            Logger.error('JSON parse error:', error);
            return {
                status: 400,
                body: {
                    error: 'Invalid JSON',
                    message: 'Request body contains invalid JSON',
                    details: CONFIG.DEBUG_MODE ? error.message : undefined,
                    receivedBody: CONFIG.DEBUG_MODE ? await request.text().catch(() => 'Unable to read') : undefined,
                    timestamp: Date.now()
                }
            };
        }

        const validation = validateTagRequest(body);
        if (!validation.valid) {
            Logger.error('Request validation failed:', validation);
            return {
                status: validation.status || 400,
                body: {
                    error: validation.error,
                    message: validation.message,
                    ...(validation.invalidTags && { invalidTags: validation.invalidTags }),
                    ...(validation.duplicateTags && { duplicateTags: validation.duplicateTags }),
                    ...(validation.details && { details: validation.details }),
                    timestamp: Date.now()
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
                Logger.error(`Cache purge error:`, err)
            ));

            waitUntil(addFileToIndex(context, result.fileId, result.metadata).catch(err => 
                Logger.error(`Index update error:`, err)
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
                changed: result.changed,
                timestamp: Date.now()
            }
        };

    } catch (error) {
        Logger.error(`POST error:`, error);
        
        if (error.message === 'File not found') {
            return {
                status: 404,
                body: {
                    error: 'File not found',
                    fileId: cleanFileId(fileId),
                    originalFileId: fileId,
                    message: 'The requested file was not found in the database',
                    timestamp: Date.now()
                }
            };
        }

        return {
            status: 500,
            body: {
                error: 'Internal server error',
                message: 'Failed to update tags',
                details: CONFIG.DEBUG_MODE ? error.message : undefined,
                timestamp: Date.now()
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
                Logger.error(`Cache purge error:`, err)
            ));

            context.waitUntil(addFileToIndex(context, result.fileId, result.metadata).catch(err => 
                Logger.error(`Index update error:`, err)
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
                changed: result.changed,
                timestamp: Date.now()
            }
        };

    } catch (error) {
        Logger.error(`DELETE error:`, error);
        
        if (error.message === 'File not found') {
            return {
                status: 404,
                body: {
                    error: 'File not found',
                    fileId: cleanFileId(fileId),
                    originalFileId: fileId,
                    message: 'The requested file was not found in the database',
                    timestamp: Date.now()
                }
            };
        }

        return {
            status: 500,
            body: {
                error: 'Internal server error',
                message: 'Failed to clear tags',
                details: CONFIG.DEBUG_MODE ? error.message : undefined,
                timestamp: Date.now()
            }
        };
    }
}

/**
 * 创建响应
 */
function createResponse(body, status = 200, headers = {}) {
    return new Response(safeStringify(body), {
        status,
        headers: {
            'Content-Type': 'application/json',
            'X-Content-Type-Options': 'nosniff',
            'X-XSS-Protection': '1; mode=block',
            ...headers
        }
    });
}

/**
 * 主API处理函数
 */
export async function onRequest(context) {
    const startTime = performance.now();
    const { request, env, params } = context;
    let url;
    
    try {
        url = new URL(request.url);
    } catch (error) {
        url = new URL('https://example.com');
        Logger.error('Failed to parse URL:', error);
    }

    // Force debug mode for troubleshooting
    CONFIG.DEBUG_MODE = true;
    
    Logger.log(`=== New Request ===`);
    Logger.log(`Method: ${request.method}`);
    Logger.log(`URL: ${request.url}`);
    Logger.log(`Hostname: ${url.hostname}`);
    Logger.log(`Path: ${url.pathname}`);
    Logger.log(`Params:`, params);
    Logger.log(`Headers:`, Object.fromEntries(request.headers));

    try {
        // Handle CORS first
        const corsHeaders = handleCORS(request);
        if (corsHeaders instanceof Response) {
            return corsHeaders;
        }

        // Validate authentication
        const authResult = validateAuth(request, env);
        if (!authResult.valid) {
            return createResponse({
                error: authResult.error,
                message: authResult.message,
                timestamp: Date.now()
            }, 401, {
                ...corsHeaders,
                'WWW-Authenticate': 'Bearer'
            });
        }

        // Parse file ID from params
        let fileId = '';
        if (params.path) {
            fileId = String(params.path).split(',').join('/');
        }
        fileId = decodeURIComponent(fileId || '');

        Logger.debug(`Original file ID from request: "${fileId}"`);

        // Validate file ID format
        if (!validateFileId(fileId)) {
            Logger.error(`Invalid file ID format: "${fileId}"`);
            return createResponse({
                error: 'Invalid file ID',
                message: 'File ID format is invalid',
                fileId: fileId,
                validFormat: 'Should contain only letters, numbers, slashes, dots, underscores, and hyphens',
                validExample: 'img/abc123def.jpg or abc123def.jpg',
                timestamp: Date.now()
            }, 400, corsHeaders);
        }

        // Check if database is available
        let db;
        try {
            db = getDatabase(env);
            Logger.debug('Database connection established');
        } catch (dbError) {
            Logger.error('Database connection error:', dbError);
            return createResponse({
                error: 'Database error',
                message: 'Failed to connect to database',
                details: CONFIG.DEBUG_MODE ? dbError.message : undefined,
                timestamp: Date.now()
            }, 503, corsHeaders);
        }

        // Route request
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
                        receivedMethod: request.method,
                        timestamp: Date.now()
                    }
                };
        }

        // Log response details
        const duration = performance.now() - startTime;
        Logger.log(`=== Response (${duration.toFixed(2)}ms) ===`);
        Logger.log(`Status: ${response.status}`);
        Logger.log(`Body:`, response.body);

        // Send response
        return createResponse(response.body, response.status, {
            ...corsHeaders,
            'X-Processing-Time': `${duration.toFixed(2)}ms`,
            'X-API-Version': '1.3.0',
            'Cache-Control': response.status === 200 ? 'public, max-age=60' : 'no-store',
            'X-Debug-Mode': CONFIG.DEBUG_MODE ? 'true' : 'false'
        });

    } catch (error) {
        const duration = performance.now() - startTime;
        Logger.error(`=== Fatal Error (${duration.toFixed(2)}ms) ===`);
        Logger.error(`Error:`, error);
        Logger.error(`Stack:`, error.stack);

        // Send error response
        return createResponse({
            error: 'Internal server error',
            message: 'An unexpected error occurred',
            details: CONFIG.DEBUG_MODE ? error.message : undefined,
            stack: CONFIG.DEBUG_MODE ? error.stack : undefined,
            processingTime: `${duration.toFixed(2)}ms`,
            timestamp: Date.now()
        }, 500, {
            ...handleCORS(request),
            'Cache-Control': 'no-store'
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
