/**
 * Universal Path Fix for Tag Management API
 * 
 * This version focuses on path format: /file/img/KfONFGrt.webp
 * Both domains are supported: hub.lsdns.top and curl.img.lsdns.top
 */

import { purgeCFCache } from "../../../utils/purgeCache.js";
import { addFileToIndex } from "../../../utils/indexManager.js";
import { getDatabase } from "../../../utils/databaseAdapter.js";
import { mergeTags, normalizeTags, validateTag } from "../../../utils/tagHelpers.js";

/**
 * 全局配置 - 支持双域名
 */
const CONFIG = {
    CACHE_TTL: 300,
    MAX_TAG_LENGTH: 100,
    MIN_TAG_LENGTH: 1,
    SUPPORTED_ACTIONS: ['set', 'add', 'remove', 'replace', 'toggle', 'clear'],
    ALLOWED_METHODS: ['GET', 'POST', 'DELETE', 'PATCH', 'OPTIONS'],
    DEBUG_MODE: true, // 强制调试模式
    ALLOW_ANONYMOUS: false,
    
    // 支持双域名
    SUPPORTED_DOMAINS: ['hub.lsdns.top', 'curl.img.lsdns.top'],
    ALLOWED_ORIGINS: ['https://hub.lsdns.top', 'https://curl.img.lsdns.top'],
    
    // 关键路径配置
    FILE_PATH_PREFIX: 'file/',
    FILE_PATH_REGEX: /^\/?file\/?/i,
    
    MAX_REQUEST_SIZE: 1024 * 1024,
    REQUEST_TIMEOUT: 30000
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
 * 通用路径处理 - 确保格式为 /file/img/KfONFGrt.webp
 */
function processFilePath(fileId) {
    if (!fileId) return { 
        cleanedPath: '', 
        originalPath: '', 
        fullPath: '',
        fileId: '',
        pathParts: []
    };
    
    // 解码URL编码的字符
    const decodedId = decodeURIComponent(fileId);
    Logger.debug(`Original input: "${fileId}" (decoded: "${decodedId}")`);
    
    // 标准化路径格式
    let normalizedPath = decodedId
        .replace(/\/+/g, '/')            // 合并多个斜杠
        .replace(/^\//, '')              // 移除开头的斜杠
        .replace(/\?.*/, '')             // 移除查询参数
        .trim();                         // 去除首尾空格
    
    // 检查是否已经包含 file/ 前缀
    const hasFilePrefix = CONFIG.FILE_PATH_REGEX.test(normalizedPath);
    
    // 确保路径以 file/ 开头
    let fullPath = hasFilePrefix 
        ? normalizedPath 
        : `${CONFIG.FILE_PATH_PREFIX}${normalizedPath}`;
    
    // 分割路径部分
    const pathParts = fullPath.split('/').filter(part => part.trim() !== '');
    
    // 提取文件ID（最后一部分）
    const fileIdPart = pathParts.length > 0 ? pathParts[pathParts.length - 1] : '';
    
    // 清理后的路径（不包含 file/ 前缀）
    const cleanedPath = hasFilePrefix 
        ? normalizedPath.replace(CONFIG.FILE_PATH_REGEX, '') 
        : normalizedPath;
    
    Logger.debug(`Path processing result:`);
    Logger.debug(`  Original:     "${fileId}"`);
    Logger.debug(`  Decoded:      "${decodedId}"`);
    Logger.debug(`  Normalized:   "${normalizedPath}"`);
    Logger.debug(`  Has file/:    ${hasFilePrefix}`);
    Logger.debug(`  Full path:    "${fullPath}"`);
    Logger.debug(`  Cleaned path: "${cleanedPath}"`);
    Logger.debug(`  File ID:      "${fileIdPart}"`);
    Logger.debug(`  Path parts:   ${JSON.stringify(pathParts)}`);
    
    return {
        cleanedPath,
        originalPath: fileId,
        fullPath,
        fileId: fileIdPart,
        pathParts
    };
}

/**
 * 验证文件路径格式
 */
function validateFilePath(filePath) {
    if (!filePath || typeof filePath !== 'string') {
        return false;
    }
    
    // 允许包含 file/ 前缀的路径
    const pathRegex = /^[a-zA-Z0-9_\-\/\.%]{8,128}$/;
    return pathRegex.test(filePath);
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
async function getFileTags(db, pathInfo, hostname) {
    const { fullPath, cleanedPath, fileId } = pathInfo;
    const cacheKey = `tags:${fullPath}`;

    // 检查缓存
    const cached = CacheManager.get(cacheKey);
    if (cached) {
        Logger.debug(`Cache hit for ${fullPath}`);
        return { ...cached, fromCache: true };
    }

    try {
        Logger.debug(`Getting tags from database for:`, {
            fullPath,
            cleanedPath,
            fileId,
            originalPath: pathInfo.originalPath
        });
        
        // 尝试用不同的路径格式查询
        const pathsToTry = [
            fullPath,          // file/img/KfONFGrt.webp
            cleanedPath,       // img/KfONFGrt.webp
            fileId,            // KfONFGrt.webp
            pathInfo.originalPath // 原始输入
        ];
        
        let fileData = null;
        let foundPath = null;
        for (const path of pathsToTry) {
            if (!path) continue;
            
            try {
                fileData = await db.getWithMetadata(path);
                if (fileData) {
                    foundPath = path;
                    Logger.debug(`Found file with path: ${path}`);
                    break;
                }
            } catch (error) {
                Logger.debug(`Error trying path ${path}:`, error.message);
            }
        }

        if (!fileData) {
            Logger.error(`File not found in database. Tried paths: ${pathsToTry.filter(p => p).join(', ')}`);
            throw new Error('File not found');
        }

        if (!fileData.metadata) {
            Logger.warn(`File ${fullPath} has no metadata, initializing empty`);
            fileData.metadata = {};
        }

        const tags = fileData.metadata.Tags || [];
        const result = {
            fullPath,
            cleanedPath,
            fileId,
            originalPath: pathInfo.originalPath,
            tags,
            metadata: fileData.metadata,
            fromCache: false,
            fileUrl: `https://${hostname}/${fullPath}`
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
async function updateFileTags({ db, pathInfo, action, tags, options, context, hostname }) {
    const { fullPath, cleanedPath, fileId } = pathInfo;
    const { waitUntil, env } = context;

    try {
        Logger.debug(`Updating tags for ${fullPath}:`, { action, tags, options });
        
        // 尝试用不同的路径格式查询
        const pathsToTry = [
            fullPath,          // file/img/KfONFGrt.webp
            cleanedPath,       // img/KfONFGrt.webp
            fileId,            // KfONFGrt.webp
            pathInfo.originalPath // 原始输入
        ];
        
        let fileData = null;
        let foundPath = null;
        for (const path of pathsToTry) {
            if (!path) continue;
            
            try {
                fileData = await db.getWithMetadata(path);
                if (fileData) {
                    foundPath = path;
                    Logger.debug(`Found file with path: ${path}`);
                    break;
                }
            } catch (error) {
                Logger.debug(`Error trying path ${path}:`, error.message);
            }
        }

        if (!fileData) {
            Logger.error(`File not found in database. Tried paths: ${pathsToTry.filter(p => p).join(', ')}`);
            throw new Error('File not found');
        }

        if (!fileData.metadata) {
            Logger.warn(`File ${fullPath} has no metadata, creating new`);
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
            Logger.debug(`No tag changes for ${fullPath}`);
            return {
                fullPath,
                cleanedPath,
                fileId,
                originalPath: pathInfo.originalPath,
                action,
                tags: existingTags,
                existingTags,
                updatedTags,
                changed: false,
                metadata: fileData.metadata,
                fileUrl: `https://${hostname}/${fullPath}`
            };
        }

        fileData.metadata.Tags = updatedTags;
        fileData.metadata.updatedAt = new Date().toISOString();

        // 使用找到的路径保存
        await db.put(foundPath, fileData.value, {
            metadata: fileData.metadata
        });

        CacheManager.clear(`tags:${fullPath}`);
        Logger.debug(`Tags updated for ${fullPath}:`, updatedTags);

        return {
            fullPath,
            cleanedPath,
            fileId,
            originalPath: pathInfo.originalPath,
            action,
            tags: updatedTags,
            existingTags,
            updatedTags,
            changed: true,
            metadata: fileData.metadata,
            fileUrl: `https://${hostname}/${fullPath}`
        };

    } catch (error) {
        Logger.error(`Error updating tags:`, error);
        throw error;
    }
}

/**
 * 处理GET请求
 */
async function handleGetTags(db, pathInfo, hostname) {
    try {
        const result = await getFileTags(db, pathInfo, hostname);

        return {
            status: 200,
            body: {
                success: true,
                fullPath: result.fullPath,
                cleanedPath: result.cleanedPath,
                fileId: result.fileId,
                originalPath: result.originalPath,
                tags: result.tags,
                fromCache: result.fromCache,
                fileUrl: result.fileUrl,
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
                    fullPath: pathInfo.fullPath,
                    cleanedPath: pathInfo.cleanedPath,
                    fileId: pathInfo.fileId,
                    originalPath: pathInfo.originalPath,
                    triedPaths: [
                        pathInfo.fullPath,
                        pathInfo.cleanedPath,
                        pathInfo.fileId,
                        pathInfo.originalPath
                    ],
                    message: 'The requested file was not found in the database',
                    fileUrl: `https://${hostname}/${pathInfo.fullPath}`,
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
async function handlePostTags(context, db, pathInfo, hostname) {
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
            pathInfo,
            action,
            tags,
            options,
            context,
            hostname
        });

        if (result.changed) {
            // 使用当前请求的域名
            const fileUrl = `https://${hostname}/${result.fullPath}`;
            Logger.debug(`Purging CDN cache for: ${fileUrl}`);
            
            waitUntil(purgeCFCache(context.env, fileUrl).catch(err => 
                Logger.error(`Cache purge error:`, err)
            ));

            waitUntil(addFileToIndex(context, result.fullPath, result.metadata).catch(err => 
                Logger.error(`Index update error:`, err)
            ));
        }

        return {
            status: 200,
            body: {
                success: true,
                fullPath: result.fullPath,
                cleanedPath: result.cleanedPath,
                fileId: result.fileId,
                originalPath: result.originalPath,
                action: result.action,
                tags: result.tags,
                existingTags: result.existingTags,
                updatedTags: result.updatedTags,
                changed: result.changed,
                fileUrl: result.fileUrl,
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
                    fullPath: pathInfo.fullPath,
                    cleanedPath: pathInfo.cleanedPath,
                    fileId: pathInfo.fileId,
                    originalPath: pathInfo.originalPath,
                    triedPaths: [
                        pathInfo.fullPath,
                        pathInfo.cleanedPath,
                        pathInfo.fileId,
                        pathInfo.originalPath
                    ],
                    message: 'The requested file was not found in the database',
                    fileUrl: `https://${hostname}/${pathInfo.fullPath}`,
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
async function handleDeleteTags(context, db, pathInfo, hostname) {
    try {
        const result = await updateFileTags({
            db,
            pathInfo,
            action: 'clear',
            tags: [],
            options: {},
            context,
            hostname
        });

        if (result.changed) {
            const fileUrl = `https://${hostname}/${result.fullPath}`;
            Logger.debug(`Purging CDN cache for: ${fileUrl}`);
            
            context.waitUntil(purgeCFCache(context.env, fileUrl).catch(err => 
                Logger.error(`Cache purge error:`, err)
            ));

            context.waitUntil(addFileToIndex(context, result.fullPath, result.metadata).catch(err => 
                Logger.error(`Index update error:`, err)
            ));
        }

        return {
            status: 200,
            body: {
                success: true,
                fullPath: result.fullPath,
                cleanedPath: result.cleanedPath,
                fileId: result.fileId,
                originalPath: result.originalPath,
                action: 'clear',
                tags: result.tags,
                existingTags: result.existingTags,
                changed: result.changed,
                fileUrl: result.fileUrl,
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
                    fullPath: pathInfo.fullPath,
                    cleanedPath: pathInfo.cleanedPath,
                    fileId: pathInfo.fileId,
                    originalPath: pathInfo.originalPath,
                    triedPaths: [
                        pathInfo.fullPath,
                        pathInfo.cleanedPath,
                        pathInfo.fileId,
                        pathInfo.originalPath
                    ],
                    message: 'The requested file was not found in the database',
                    fileUrl: `https://${hostname}/${pathInfo.fullPath}`,
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

    // 强制调试模式
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

        // 验证域名
        if (!CONFIG.SUPPORTED_DOMAINS.includes(url.hostname)) {
            Logger.error(`Unsupported domain: ${url.hostname}`);
            return createResponse({
                error: 'Unsupported domain',
                message: `This API only supports domains: ${CONFIG.SUPPORTED_DOMAINS.join(', ')}`,
                receivedDomain: url.hostname,
                supportedDomains: CONFIG.SUPPORTED_DOMAINS,
                timestamp: Date.now()
            }, 403, corsHeaders);
        }

        // 解析和处理文件路径 - 关键修复
        let fileId = '';
        if (params.path) {
            fileId = String(params.path).split(',').join('/');
        }
        
        // 处理文件路径
        const pathInfo = processFilePath(fileId);
        
        // 验证文件路径格式
        if (!validateFilePath(pathInfo.originalPath)) {
            Logger.error(`Invalid file path format: "${pathInfo.originalPath}"`);
            return createResponse({
                error: 'Invalid file path',
                message: 'File path format is invalid',
                originalPath: pathInfo.originalPath,
                fullPath: pathInfo.fullPath,
                validFormat: 'Should contain only letters, numbers, slashes, dots, underscores, and hyphens',
                validExample: 'img/KfONFGrt.webp or file/img/KfONFGrt.webp',
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
                response = await handleGetTags(db, pathInfo, url.hostname);
                break;
                
            case 'POST':
            case 'PATCH':
                response = await handlePostTags(context, db, pathInfo, url.hostname);
                break;
                
            case 'DELETE':
                response = await handleDeleteTags(context, db, pathInfo, url.hostname);
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
            'X-API-Version': '1.5.0',
            'Cache-Control': response.status === 200 ? 'public, max-age=60' : 'no-store',
            'X-Debug-Mode': CONFIG.DEBUG_MODE ? 'true' : 'false',
            'X-Request-Host': url.hostname
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
export function clearTagCache(filePath) {
    if (filePath) {
        const { fullPath } = processFilePath(filePath);
        CacheManager.clear(`tags:${fullPath}`);
        return { success: true, cleared: 1 };
    }
    
    const cacheSize = CacheManager.cache.size;
    CacheManager.clear();
    return { success: true, cleared: cacheSize };
}
