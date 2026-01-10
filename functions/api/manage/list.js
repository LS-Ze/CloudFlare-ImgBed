import {
    readIndex, mergeOperationsToIndex, deleteAllOperations, rebuildIndex,
    getIndexInfo, getIndexStorageStats
} from '../../utils/indexManager.js';
import { getDatabase } from '../../utils/databaseAdapter.js';

// CORS 跨域响应头
const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Max-Age': '86400',
};

// 缓存配置
const CACHE_TTL = 5 * 60; // 5分钟缓存
const MAX_MEMORY_CACHE_SIZE = 100; // 最多缓存100个不同的查询
const RATE_LIMIT = 60; // 每分钟请求限制
const RATE_LIMIT_WINDOW = 60; // 限制窗口（秒）

// 内存缓存
const memoryCache = new Map();

export async function onRequest(context) {
    const { request, env, waitUntil } = context;
    const url = new URL(request.url);

    try {
        // 1. 安全检查
        const securityCheck = await performSecurityChecks(context, url);
        if (securityCheck) return securityCheck;

        // 2. 解析查询参数
        const params = parseRequestParams(url);
        
        // 3. 处理特殊操作
        const actionResponse = await handleSpecialActions(context, params);
        if (actionResponse) return actionResponse;

        // 4. 尝试从缓存获取数据
        const cachedData = getCachedData(params);
        if (cachedData) {
            return createResponse(cachedData, true);
        }

        // 5. 处理普通查询
        const result = await processIndexQuery(context, params);

        // 6. 缓存结果
        cacheResult(params, result);

        // 7. 返回结果
        return createResponse(result, false);

    } catch (error) {
        return handleError(error, context);
    }
}

/**
 * 执行安全检查
 */
async function performSecurityChecks(context, url) {
    const { request, env } = context;

    // API密钥认证
    const apiKey = url.searchParams.get('apiKey');
    if (env.API_KEY && (!apiKey || apiKey !== env.API_KEY)) {
        return createErrorResponse('Unauthorized', 'Invalid API key', 401);
    }

    // 请求频率限制
    if (env.ENABLE_RATE_LIMIT && env.KV) {
        const clientIp = request.headers.get('CF-Connecting-IP');
        const rateLimitKey = `rate_limit_${clientIp}`;
        
        const rateLimitCount = await getRateLimitCount(env, rateLimitKey);
        if (rateLimitCount >= RATE_LIMIT) {
            return createErrorResponse(
                'Too Many Requests', 
                'Rate limit exceeded',
                429,
                { "Retry-After": RATE_LIMIT_WINDOW.toString() }
            );
        }

        // 更新请求计数
        await updateRateLimitCount(env, rateLimitKey, rateLimitCount);
    }

    return null;
}

/**
 * 解析请求参数
 */
function parseRequestParams(url) {
    // 解析基本参数
    const params = {
        start: Math.max(0, parseInt(url.searchParams.get('start'), 10) || 0),
        count: Math.max(1, Math.min(1000, parseInt(url.searchParams.get('count'), 10) || 50)),
        sum: url.searchParams.get('sum') === 'true',
        recursive: url.searchParams.get('recursive') === 'true',
        dir: processDirectory(url.searchParams.get('dir') || ''),
        search: url.searchParams.get('search') ? decodeURIComponent(url.searchParams.get('search')).trim() : '',
        channel: url.searchParams.get('channel') || '',
        listType: url.searchParams.get('listType') || '',
        action: url.searchParams.get('action') || '',
        includeTags: url.searchParams.get('includeTags') || '',
        excludeTags: url.searchParams.get('excludeTags') || ''
    };

    // 处理标签参数
    params.includeTagsArray = params.includeTags 
        ? params.includeTags.split(',').map(t => t.trim()).filter(t => t) 
        : [];
    params.excludeTagsArray = params.excludeTags 
        ? params.excludeTags.split(',').map(t => t.trim()).filter(t => t) 
        : [];

    return params;
}

/**
 * 处理特殊操作
 */
async function handleSpecialActions(context, params) {
    const { action, env, waitUntil } = context;

    if (action === 'rebuild') {
        waitUntil(rebuildIndex(context, (processed) => {
            console.log(`Rebuilt ${processed} files...`);
        }));
        return createPlainResponse('Index rebuilt asynchronously');
    }

    if (action === 'merge-operations') {
        waitUntil(mergeOperationsToIndex(context));
        return createPlainResponse('Operations merged into index asynchronously');
    }

    if (action === 'delete-operations') {
        waitUntil(deleteAllOperations(context));
        return createPlainResponse('All operations deleted asynchronously');
    }

    if (action === 'index-storage-stats') {
        const stats = await getIndexStorageStats(context);
        return createResponse(stats);
    }

    if (action === 'info') {
        const info = await getIndexInfo(context);
        return createResponse(info);
    }

    return null;
}

/**
 * 处理普通查询
 */
async function processIndexQuery(context, params) {
    const { sum, count, dir, search, channel, listType, 
            includeTagsArray, excludeTagsArray, recursive, start } = params;

    // 只返回总数的查询
    if (count === -1 && sum) {
        const result = await readIndex(context, {
            search,
            directory: dir,
            channel,
            listType,
            includeTags: includeTagsArray,
            excludeTags: excludeTagsArray,
            countOnly: true
        });

        return {
            sum: result.totalCount,
            indexLastUpdated: result.indexLastUpdated
        };
    }

    // 普通查询
    const result = await readIndex(context, {
        search,
        directory: dir,
        start,
        count,
        channel,
        listType,
        includeTags: includeTagsArray,
        excludeTags: excludeTagsArray,
        includeSubdirFiles: recursive,
    });

    // 索引读取失败，直接从 KV 中获取所有文件记录
    if (!result.success) {
        const dbRecords = await getAllFileRecords(context.env, dir);
        return {
            files: dbRecords.files,
            directories: dbRecords.directories,
            totalCount: dbRecords.totalCount,
            directFileCount: dbRecords.directFileCount,
            directFolderCount: dbRecords.directFolderCount,
            returnedCount: dbRecords.returnedCount,
            indexLastUpdated: Date.now(),
            isIndexedResponse: false // 标记这是来自 KV 的响应
        };
    }

    // 转换文件格式
    const compatibleFiles = result.files.map(file => ({
        name: file.id,
        metadata: file.metadata
    }));

    return {
        files: compatibleFiles,
        directories: result.directories,
        totalCount: result.totalCount,
        directFileCount: result.directFileCount,
        directFolderCount: result.directFolderCount,
        returnedCount: result.returnedCount,
        indexLastUpdated: result.indexLastUpdated,
        isIndexedResponse: true // 标记这是来自索引的响应
    };
}

/**
 * 从KV获取所有文件记录
 */
async function getAllFileRecords(env, dir) {
    const allRecords = [];
    let cursor = null;

    try {
        const db = getDatabase(env);

        while (true) {
            const response = await db.list({
                prefix: dir,
                limit: 1000,
                cursor: cursor
            });

            // 检查响应格式
            if (!response || !response.keys || !Array.isArray(response.keys)) {
                console.error('Invalid response from database list:', response);
                break;
            }

            cursor = response.cursor;

            for (const item of response.keys) {
                // 跳过管理相关的键和没有元数据的文件
                if (isValidFileRecord(item)) {
                    allRecords.push(item);
                }
            }

            if (!cursor) break;

            // 添加协作点
            await new Promise(resolve => setTimeout(resolve, 10));
        }

        // 提取目录信息
        const { directories, filteredRecords } = extractDirectories(allRecords, dir);

        return {
            files: filteredRecords,
            directories: Array.from(directories),
            totalCount: allRecords.length,
            directFileCount: filteredRecords.length,
            directFolderCount: directories.size,
            returnedCount: filteredRecords.length
        };

    } catch (error) {
        console.error('Error in getAllFileRecords:', error);
        return {
            files: [],
            directories: [],
            totalCount: 0,
            directFileCount: 0,
            directFolderCount: 0,
            returnedCount: 0,
            error: error.message
        };
    }
}

/**
 * 检查文件记录是否有效
 */
function isValidFileRecord(item) {
    // 跳过管理相关的键
    if (item.name.startsWith('manage@') || item.name.startsWith('chunk_')) {
        return false;
    }

    // 跳过没有元数据的文件
    if (!item.metadata || !item.metadata.TimeStamp) {
        return false;
    }

    return true;
}

/**
 * 提取目录信息
 */
function extractDirectories(records, dir) {
    const directories = new Set();
    const filteredRecords = [];

    records.forEach(item => {
        const subDir = item.name.substring(dir.length);
        const firstSlashIndex = subDir.indexOf('/');
        
        if (firstSlashIndex !== -1) {
            // 这是一个目录
            directories.add(dir + subDir.substring(0, firstSlashIndex));
        } else {
            // 这是一个文件
            filteredRecords.push(item);
        }
    });

    return { directories, filteredRecords };
}

/**
 * 处理目录参数
 */
function processDirectory(dir) {
    if (!dir) return '';
    
    // 标准化目录格式
    let processedDir = dir.trim();
    
    // 移除开头的斜杠
    if (processedDir.startsWith('/')) {
        processedDir = processedDir.substring(1);
    }
    
    // 确保以斜杠结尾
    if (processedDir && !processedDir.endsWith('/')) {
        processedDir += '/';
    }
    
    return processedDir;
}

/**
 * 生成缓存键
 */
function generateCacheKey(params) {
    const { dir, search, channel, listType, includeTags, excludeTags, start, count, recursive } = params;
    return `index_${dir}_${search}_${channel}_${listType}_${includeTags}_${excludeTags}_${start}_${count}_${recursive}`;
}

/**
 * 获取缓存数据
 */
function getCachedData(params) {
    const cacheKey = generateCacheKey(params);
    
    if (memoryCache.has(cacheKey)) {
        const cached = memoryCache.get(cacheKey);
        if (Date.now() - cached.timestamp < CACHE_TTL * 1000) {
            return cached.data;
        }
        // 缓存过期，移除
        memoryCache.delete(cacheKey);
    }
    
    return null;
}

/**
 * 缓存结果
 */
function cacheResult(params, data) {
    const cacheKey = generateCacheKey(params);
    
    memoryCache.set(cacheKey, {
        data,
        timestamp: Date.now()
    });
    
    // 限制缓存大小
    limitMemoryCacheSize();
}

/**
 * 限制内存缓存大小
 */
function limitMemoryCacheSize() {
    if (memoryCache.size > MAX_MEMORY_CACHE_SIZE) {
        const oldestKeys = Array.from(memoryCache.keys()).sort((a, b) => 
            memoryCache.get(a).timestamp - memoryCache.get(b).timestamp
        );
        
        // 删除最旧的缓存项
        for (let i = 0; i < memoryCache.size - MAX_MEMORY_CACHE_SIZE; i++) {
            memoryCache.delete(oldestKeys[i]);
        }
    }
}

/**
 * 获取速率限制计数
 */
async function getRateLimitCount(env, key) {
    try {
        const count = await env.KV.get(key);
        return count ? parseInt(count, 10) : 0;
    } catch (error) {
        console.warn('Rate limit read error:', error);
        return 0;
    }
}

/**
 * 更新速率限制计数
 */
async function updateRateLimitCount(env, key, currentCount) {
    try {
        await env.KV.put(key, (currentCount + 1).toString(), {
            expirationTtl: RATE_LIMIT_WINDOW
        });
    } catch (error) {
        console.warn('Rate limit update error:', error);
    }
}

/**
 * 创建JSON响应
 */
function createResponse(data, fromCache = false) {
    return new Response(JSON.stringify(data), {
        headers: { 
            "Content-Type": "application/json",
            "X-Cache": fromCache ? "HIT" : "MISS",
            "X-Timestamp": Date.now().toString(),
            ...corsHeaders
        }
    });
}

/**
 * 创建纯文本响应
 */
function createPlainResponse(message) {
    return new Response(message, {
        headers: { "Content-Type": "text/plain", ...corsHeaders }
    });
}

/**
 * 创建错误响应
 */
function createErrorResponse(error, message, status = 500, extraHeaders = {}) {
    return new Response(JSON.stringify({
        error,
        message,
        timestamp: Date.now()
    }), {
        status,
        headers: { 
            "Content-Type": "application/json",
            ...extraHeaders,
            ...corsHeaders
        }
    });
}

/**
 * 统一错误处理
 */
function handleError(error, context) {
    console.error('Error in list-indexed API:', error);
    
    // 记录详细错误信息
    const errorDetails = {
        message: error.message,
        stack: error.stack,
        timestamp: Date.now(),
        requestId: context.request.headers.get('CF-Request-ID')
    };
    
    // 可以考虑将错误信息记录到KV
    if (context.env.KV) {
        context.waitUntil(
            context.env.KV.put(`error_${Date.now()}`, JSON.stringify(errorDetails), {
                expirationTtl: 86400 // 保存24小时
            })
        );
    }
    
    return createErrorResponse('Internal server error', error.message, 500);
}
