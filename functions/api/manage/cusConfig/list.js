import { readIndex } from "../../../utils/indexManager";

// 缓存配置
const CACHE_TTL = 5 * 60; // 5分钟缓存
const MAX_CACHE_SIZE = 100; // 最多缓存100个不同的查询

// 内存缓存
const memoryCache = new Map();

export async function onRequest(context) {
    const { request, env, waitUntil } = context;
    const url = new URL(request.url);

    try {
        // 解析URL参数
        const start = Math.max(0, parseInt(url.searchParams.get('start'), 10) || 0);
        const count = Math.max(1, Math.min(100, parseInt(url.searchParams.get('count'), 10) || 10));
        
        // 创建缓存键
        const cacheKey = `ip_stats_${start}_${count}`;
        
        // 检查内存缓存
        if (memoryCache.has(cacheKey)) {
            const cached = memoryCache.get(cacheKey);
            if (Date.now() - cached.timestamp < CACHE_TTL * 1000) {
                return new Response(JSON.stringify(cached.data), {
                    headers: { 
                        "Content-Type": "application/json",
                        "X-Cache": "HIT"
                    }
                });
            }
        }

        // 获取所有文件记录（使用优化的readIndex）
        const indexResult = await readIndex(context, { 
            count: -1, 
            includeSubdirFiles: true,
            countOnly: false
        });

        // 检查是否获取成功
        if (!indexResult.success) {
            throw new Error(`Failed to read index: ${indexResult.error || 'Unknown error'}`);
        }

        // 按IP分组统计（优化版本）
        const ipStats = groupByIP(indexResult.files);
        
        // 按文件数量倒序排序
        const sortedStats = [...ipStats.values()].sort((a, b) => b.count - a.count);
        
        // 分页处理
        const paginatedResult = sortedStats.slice(start, start + count);
        
        // 只保留必要的信息，减少内存占用
        const result = paginatedResult.map(item => ({
            ip: item.ip,
            address: item.address,
            count: item.count,
            // 只返回文件ID列表而不是完整的文件记录
            fileIds: item.fileIds
        }));

        // 缓存结果
        memoryCache.set(cacheKey, {
            data: result,
            timestamp: Date.now()
        });
        
        // 限制缓存大小
        if (memoryCache.size > MAX_CACHE_SIZE) {
            const oldestKey = Array.from(memoryCache.keys()).sort((a, b) => 
                memoryCache.get(a).timestamp - memoryCache.get(b).timestamp
            )[0];
            memoryCache.delete(oldestKey);
        }

        // 返回结果
        return new Response(JSON.stringify(result), {
            headers: { 
                "Content-Type": "application/json",
                "X-Cache": "MISS",
                "X-Total-Count": sortedStats.length,
                "X-Returned-Count": result.length
            }
        });

    } catch (error) {
        console.error('Error processing request:', error);
        return new Response(JSON.stringify({
            error: error.message,
            stack: error.stack,
            timestamp: Date.now()
        }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
        });
    }
}

/**
 * 优化的按IP分组函数
 * 使用Map而不是Set和filter，提高性能
 * @param {Array} files - 文件数组
 * @returns {Map} - 按IP分组的统计结果
 */
function groupByIP(files) {
    const ipMap = new Map();

    for (const file of files) {
        const ip = file.metadata?.UploadIP;
        if (!ip) continue;

        if (!ipMap.has(ip)) {
            ipMap.set(ip, {
                ip,
                address: file.metadata?.UploadAddress || '未知',
                count: 0,
                fileIds: []
            });
        }

        const ipEntry = ipMap.get(ip);
        ipEntry.count++;
        ipEntry.fileIds.push(file.id);
    }

    return ipMap;
}
