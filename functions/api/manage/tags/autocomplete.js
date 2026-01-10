/**
 * Tag Autocomplete API
 *
 * GET /api/manage/tags/autocomplete?prefix=ph - Get tag suggestions
 * GET /api/manage/tags/autocomplete?prefix=ph&match=contains - Get tags containing prefix
 * GET /api/manage/tags/autocomplete?prefix=ph&sort=popularity - Sort by popularity
 *
 * Returns tags matching the given criteria, useful for autocomplete functionality
 */

import { readIndex } from "../../../utils/indexManager.js";
import { filterTagsByPrefix } from "../../../utils/tagHelpers.js";

/**
 * @typedef {Object} TagSuggestion
 * @property {string} tag - Tag name
 * @property {number} count - Usage count
 * @property {string} normalized - Normalized tag name
 */

/**
 * @typedef {Object} AutocompleteOptions
 * @property {string} prefix - Tag prefix to match
 * @property {number} limit - Maximum number of results
 * @property {string} match - Match type: 'prefix' (default) or 'contains'
 * @property {string} sort - Sort order: 'alphabetical' (default) or 'popularity'
 * @property {boolean} caseSensitive - Case sensitive matching
 * @property {boolean} includeCounts - Include usage counts
 */

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
    set(key, data, ttl = 300) {
        this.cache.set(key, {
            data,
            expires: Date.now() + (ttl * 1000)
        });
        
        // 限制缓存大小
        if (this.cache.size > 100) {
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
 * 验证请求参数
 * @param {URL} url - 请求URL
 * @returns {Object} 验证结果
 */
function validateParameters(url) {
    // 获取参数
    const prefix = (url.searchParams.get('prefix') || '').trim();
    const limit = Math.min(Math.max(parseInt(url.searchParams.get('limit') || '20', 10), 1), 100);
    const match = (url.searchParams.get('match') || 'prefix').toLowerCase();
    const sort = (url.searchParams.get('sort') || 'alphabetical').toLowerCase();
    const caseSensitive = url.searchParams.get('caseSensitive') === 'true';
    const includeCounts = url.searchParams.get('includeCounts') !== 'false';
    
    // 验证匹配类型
    const validMatchTypes = ['prefix', 'contains', 'exact'];
    if (!validMatchTypes.includes(match)) {
        return {
            valid: false,
            error: 'Invalid match type',
            message: `Match type must be one of: ${validMatchTypes.join(', ')}`
        };
    }
    
    // 验证排序方式
    const validSortTypes = ['alphabetical', 'popularity', 'length'];
    if (!validSortTypes.includes(sort)) {
        return {
            valid: false,
            error: 'Invalid sort type',
            message: `Sort type must be one of: ${validSortTypes.join(', ')}`
        };
    }
    
    return {
        valid: true,
        params: {
            prefix,
            limit,
            match,
            sort,
            caseSensitive,
            includeCounts
        }
    };
}

/**
 * 从文件中提取标签
 * @param {Array<Object>} files - 文件数组
 * @returns {Array<TagSuggestion>} 标签建议数组
 */
function extractTagsFromFiles(files) {
    const tagMap = new Map();
    
    for (const file of files) {
        if (file.metadata && Array.isArray(file.metadata.Tags)) {
            file.metadata.Tags.forEach(tag => {
                if (!tag || typeof tag !== 'string') return;
                
                const normalizedTag = tag.toLowerCase().trim();
                const existing = tagMap.get(normalizedTag) || {
                    tag: normalizedTag,
                    count: 0,
                    normalized: normalizedTag
                };
                
                tagMap.set(normalizedTag, {
                    ...existing,
                    count: existing.count + 1
                });
            });
        }
    }
    
    return Array.from(tagMap.values());
}

/**
 * 过滤标签
 * @param {Array<TagSuggestion>} tags - 标签数组
 * @param {AutocompleteOptions} options - 过滤选项
 * @returns {Array<TagSuggestion>} 过滤后的标签
 */
function filterTags(tags, options) {
    const { prefix, match, caseSensitive } = options;
    
    if (!prefix) {
        return [...tags];
    }
    
    const searchPrefix = caseSensitive ? prefix : prefix.toLowerCase();
    
    return tags.filter(tag => {
        const tagToCheck = caseSensitive ? tag.tag : tag.normalized;
        
        switch (match) {
            case 'prefix':
                return tagToCheck.startsWith(searchPrefix);
            case 'contains':
                return tagToCheck.includes(searchPrefix);
            case 'exact':
                return tagToCheck === searchPrefix;
            default:
                return tagToCheck.startsWith(searchPrefix);
        }
    });
}

/**
 * 排序标签
 * @param {Array<TagSuggestion>} tags - 标签数组
 * @param {string} sort - 排序方式
 * @returns {Array<TagSuggestion>} 排序后的标签
 */
function sortTags(tags, sort) {
    const sorted = [...tags];
    
    switch (sort) {
        case 'popularity':
            return sorted.sort((a, b) => b.count - a.count || a.tag.localeCompare(b.tag));
        case 'length':
            return sorted.sort((a, b) => a.tag.length - b.tag.length || a.tag.localeCompare(b.tag));
        case 'alphabetical':
        default:
            return sorted.sort((a, b) => a.tag.localeCompare(b.tag));
    }
}

/**
 * 格式化响应
 * @param {Array<TagSuggestion>} tags - 标签数组
 * @param {AutocompleteOptions} options - 选项
 * @param {number} totalAvailable - 可用标签总数
 * @returns {Object} 格式化的响应
 */
function formatResponse(tags, options, totalAvailable) {
    const { limit, includeCounts } = options;
    
    // 限制结果数量
    const limitedTags = tags.slice(0, limit);
    
    // 格式化标签
    const formattedTags = includeCounts 
        ? limitedTags.map(tag => ({ tag: tag.tag, count: tag.count }))
        : limitedTags.map(tag => tag.tag);
    
    return {
        success: true,
        prefix: options.prefix,
        tags: formattedTags,
        total: formattedTags.length,
        totalAvailable: totalAvailable,
        hasMore: totalAvailable > limitedTags.length,
        limit: limit,
        match: options.match,
        sort: options.sort,
        timestamp: Date.now()
    };
}

/**
 * 获取标签缓存键
 * @param {AutocompleteOptions} options - 选项
 * @returns {string} 缓存键
 */
function getCacheKey(options) {
    return `tags:autocomplete:${JSON.stringify(options)}`;
}

/**
 * 主API处理函数
 */
export async function onRequest(context) {
    const startTime = performance.now();
    const { request, env } = context;
    const url = new URL(request.url);
    
    try {
        // 只允许GET请求
        if (request.method !== 'GET') {
            return new Response(JSON.stringify({
                error: 'Method not allowed',
                allowedMethods: ['GET'],
                status: 405
            }), {
                status: 405,
                headers: { 
                    'Content-Type': 'application/json',
                    'Allow': 'GET'
                }
            });
        }
        
        // 验证请求参数
        const validation = validateParameters(url);
        if (!validation.valid) {
            return new Response(JSON.stringify({
                error: validation.error,
                message: validation.message,
                status: 400
            }), {
                status: 400,
                headers: { 'Content-Type': 'application/json' }
            });
        }
        
        const options = validation.params;
        const cacheKey = getCacheKey(options);
        
        // 检查缓存
        const cachedResponse = CacheManager.get(cacheKey);
        if (cachedResponse) {
            console.log(`[Tag Autocomplete] Cache hit for prefix: "${options.prefix}"`);
            return new Response(JSON.stringify(cachedResponse), {
                status: 200,
                headers: {
                    'Content-Type': 'application/json',
                    'Cache-Control': 'public, max-age=60',
                    'X-Cache': 'HIT'
                }
            });
        }
        
        console.log(`[Tag Autocomplete] Processing request for prefix: "${options.prefix}" (limit: ${options.limit}, match: ${options.match}, sort: ${options.sort})`);
        
        // 从索引读取文件
        const indexResult = await readIndex(context, {
            start: 0,
            count: 2000, // 增加文件读取数量以获取更全面的标签
            includeSubdirFiles: true
        });
        
        if (!indexResult.success) {
            console.error('[Tag Autocomplete] Failed to read index:', indexResult.error);
            return new Response(JSON.stringify({
                error: 'Failed to read index',
                message: indexResult.error || 'Index not available',
                status: 500
            }), {
                status: 500,
                headers: { 'Content-Type': 'application/json' }
            });
        }
        
        // 提取标签
        const allTags = extractTagsFromFiles(indexResult.files);
        const totalAvailable = allTags.length;
        
        // 过滤和排序标签
        const filteredTags = filterTags(allTags, options);
        const sortedTags = sortTags(filteredTags, options.sort);
        
        // 格式化响应
        const responseData = formatResponse(sortedTags, options, totalAvailable);
        
        // 缓存响应
        CacheManager.set(cacheKey, responseData, 300); // 缓存5分钟
        
        const duration = performance.now() - startTime;
        console.log(`[Tag Autocomplete] Completed in ${duration.toFixed(2)}ms - Found ${responseData.total} tags (${totalAvailable} available)`);
        
        return new Response(JSON.stringify(responseData), {
            status: 200,
            headers: {
                'Content-Type': 'application/json',
                'Cache-Control': 'public, max-age=60',
                'X-Cache': 'MISS',
                'X-Processing-Time': `${duration.toFixed(2)}ms`
            }
        });
        
    } catch (error) {
        const duration = performance.now() - startTime;
        console.error(`[Tag Autocomplete] Error (${duration.toFixed(2)}ms):`, error);
        
        return new Response(JSON.stringify({
            error: 'Internal server error',
            message: env.DEBUG_MODE === 'true' ? error.message : 'An unexpected error occurred',
            status: 500
        }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
        });
    }
}

/**
 * 清除标签缓存（用于管理API）
 * @returns {Object} 清除结果
 */
export function clearTagCache() {
    const cacheSize = CacheManager.cache.size;
    CacheManager.clear();
    console.log(`[Tag Autocomplete] Cleared ${cacheSize} cache entries`);
    return {
        success: true,
        cleared: cacheSize
    };
}
