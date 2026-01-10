/* 优化版索引管理器 */

/**
 * 文件索引结构（分块存储）：
 * 
 * 索引元数据：
 * - key: manage@index@meta
 * - value: JSON.stringify(metadata)
 * - metadata: {
 *     lastUpdated: 1640995200000,
 *     totalCount: 1000,
 *     lastOperationId: "operation_timestamp_uuid",
 *     chunkCount: 3,
 *     chunkSize: 10000
 *   }
 * 
 * 索引分块：
 * - key: manage@index_${chunkId} (例如: manage@index_0, manage@index_1, ...)
 * - value: JSON.stringify(filesChunk)
 * - filesChunk: [
 *     {
 *       id: "file_unique_id",
 *       metadata: {}
 *     },
 *     ...
 *   ]
 * 
 * 原子操作结构（保持不变）：
 * - key: manage@index@operation_${timestamp}_${uuid}
 * - value: JSON.stringify(operation)
 * - operation: {
 *     type: "add" | "remove" | "move" | "batch_add" | "batch_remove" | "batch_move",
 *     timestamp: 1640995200000,
 *     data: {
 *       // 根据操作类型包含不同的数据
 *     }
 *   }
 */

import { getDatabase } from './databaseAdapter.js';
import { matchesTags } from './tagHelpers.js';

// 常量定义
const INDEX_KEY = 'manage@index';
const INDEX_META_KEY = 'manage@index@meta'; // 索引元数据键
const OPERATION_KEY_PREFIX = 'manage@index@operation_';
const INDEX_CHUNK_SIZE = 10000; // 索引分块大小
const KV_LIST_LIMIT = 1000; // 数据库列出批量大小
const BATCH_SIZE = 10; // 批量处理大小

// 缓存配置
const CACHE_TTL = 5 * 60 * 1000; // 5分钟缓存
const QUERY_CACHE_TTL = 10 * 60 * 1000; // 10分钟查询缓存
const MAX_QUERY_CACHE_SIZE = 50; // 最多缓存50个查询

// 索引缓存
const indexCache = {
    data: null,
    timestamp: 0
};

// 查询缓存
const queryCache = {};

// 统计信息
const indexStats = {
    operations: {
        add: 0,
        remove: 0,
        move: 0,
        batchAdd: 0,
        batchRemove: 0,
        batchMove: 0,
        merge: 0,
        rebuild: 0
    },
    errors: 0,
    totalOperations: 0,
    totalDuration: 0
};

// TypeScript类型定义（JSDoc形式）
/**
 * @typedef {Object} FileItem
 * @property {string} id - 文件ID
 * @property {Object} metadata - 文件元数据
 */

/**
 * @typedef {Object} Index
 * @property {FileItem[]} files - 文件数组
 * @property {number} lastUpdated - 最后更新时间戳
 * @property {number} totalCount - 总文件数
 * @property {string|null} lastOperationId - 最后处理的操作ID
 * @property {boolean} success - 是否成功
 */

/**
 * @typedef {Object} Operation
 * @property {string} id - 操作ID
 * @property {string} type - 操作类型
 * @property {number} timestamp - 时间戳
 * @property {Object} data - 操作数据
 */

/**
 * @typedef {Object} IndexMetadata
 * @property {number} lastUpdated - 最后更新时间戳
 * @property {number} totalCount - 总文件数
 * @property {number} totalSizeMB - 总大小（MB）
 * @property {Object} channelStats - 渠道统计
 * @property {string|null} lastOperationId - 最后处理的操作ID
 * @property {number} chunkCount - 分块数量
 * @property {number} chunkSize - 分块大小
 */

/**
 * 中间件：统一处理索引操作
 * @param {Function} operation - 具体操作函数
 * @param {Object} context - 上下文对象
 * @param {Array} args - 操作参数
 * @returns {Object} 操作结果
 */
async function indexOperationMiddleware(operation, context, ...args) {
    const startTime = Date.now();
    try {
        // 执行实际操作
        const result = await operation(context, ...args);
        
        // 更新统计
        updateIndexStats(operation.name, Date.now() - startTime, true);
        
        // 清除相关缓存
        clearIndexCache();
        
        return {
            ...result,
            duration: Date.now() - startTime,
            timestamp: Date.now()
        };
    } catch (error) {
        console.error(`Index operation ${operation.name} failed:`, error);
        
        // 更新统计
        updateIndexStats(operation.name, Date.now() - startTime, false);
        
        return {
            success: false,
            error: error.message,
            stack: error.stack,
            timestamp: Date.now(),
            duration: Date.now() - startTime
        };
    }
}

/**
 * 更新操作统计
 * @param {string} operation - 操作名称
 * @param {number} duration - 操作持续时间
 * @param {boolean} success - 是否成功
 */
function updateIndexStats(operation, duration, success = true) {
    indexStats.totalOperations++;
    indexStats.totalDuration += duration;
    
    const operationMap = {
        'addFileToIndex': 'add',
        'removeFileFromIndex': 'remove',
        'moveFileInIndex': 'move',
        'batchAddFilesToIndex': 'batchAdd',
        'batchRemoveFilesFromIndex': 'batchRemove',
        'batchMoveFilesInIndex': 'batchMove',
        'mergeOperationsToIndex': 'merge',
        'rebuildIndex': 'rebuild'
    };
    
    const statName = operationMap[operation] || operation;
    if (indexStats.operations[statName] !== undefined) {
        indexStats.operations[statName]++;
    }
    
    if (!success) {
        indexStats.errors++;
    }
}

/**
 * 添加文件到索引
 * @param {Object} context - 上下文对象，包含 env 和其他信息
 * @param {string} fileId - 文件 ID
 * @param {Object} metadata - 文件元数据
 */
export async function addFileToIndex(context, fileId, metadata = null) {
    return indexOperationMiddleware(async (ctx, id, meta) => {
        const { env } = ctx;
        const db = getDatabase(env);

        if (meta === null) {
            // 如果未传入metadata，尝试从数据库中获取
            try {
                const fileData = await db.getWithMetadata(id);
                meta = fileData.metadata || {};
            } catch (error) {
                console.warn(`Failed to get metadata for file ${id}, using empty metadata`);
                meta = {};
            }
        }

        // 记录原子操作
        const operationId = await recordOperation(ctx, 'add', {
            fileId: id,
            metadata: meta
        });

        console.log(`File ${id} add operation recorded with ID: ${operationId}`);
        return { success: true, operationId };
    }, context, fileId, metadata);
}

/**
 * 批量添加文件到索引
 * @param {Object} context - 上下文对象，包含 env 和其他信息
 * @param {Array} files - 文件数组，每个元素包含 { fileId, metadata }
 * @param {Object} options - 选项
 * @param {boolean} options.skipExisting - 是否跳过已存在的文件，默认为 false（更新已存在的文件）
 * @returns {Object} 返回操作结果 { operationId, totalProcessed }
 */
export async function batchAddFilesToIndex(context, files, options = {}) {
    return indexOperationMiddleware(async (ctx, fileList, opts) => {
        const { env } = ctx;
        const { skipExisting = false } = opts;
        const db = getDatabase(env);

        // 验证输入
        if (!Array.isArray(fileList) || fileList.length === 0) {
            return {
                success: true,
                operationId: null,
                totalProcessed: 0,
                message: 'No files to process'
            };
        }

        // 处理每个文件的metadata
        const processedFiles = [];
        for (const fileItem of fileList) {
            const { fileId, metadata } = fileItem;
            let finalMetadata = metadata;

            // 验证fileId
            if (!fileId || typeof fileId !== 'string') {
                console.warn('Skipping invalid fileId:', fileItem);
                continue;
            }

            // 如果没有提供metadata，尝试从数据库中获取
            if (!finalMetadata) {
                try {
                    const fileData = await db.getWithMetadata(fileId);
                    finalMetadata = fileData.metadata || {};
                } catch (error) {
                    console.warn(`Failed to get metadata for file ${fileId}:`, error);
                    finalMetadata = {};
                }
            }

            processedFiles.push({
                fileId,
                metadata: finalMetadata
            });
        }

        if (processedFiles.length === 0) {
            return {
                success: true,
                operationId: null,
                totalProcessed: 0,
                message: 'No valid files to process'
            };
        }

        // 记录批量添加操作
        const operationId = await recordOperation(ctx, 'batch_add', {
            files: processedFiles,
            options: { skipExisting }
        });

        console.log(`Batch add operation recorded with ID: ${operationId}, ${processedFiles.length} files`);
        return {
            success: true,
            operationId,
            totalProcessed: processedFiles.length,
            originalCount: fileList.length,
            validCount: processedFiles.length
        };
    }, context, files, options);
}

/**
 * 从索引中删除文件
 * @param {Object} context - 上下文对象
 * @param {string} fileId - 文件 ID
 */
export async function removeFileFromIndex(context, fileId) {
    return indexOperationMiddleware(async (ctx, id) => {
        // 验证输入
        if (!id || typeof id !== 'string') {
            throw new Error('Invalid fileId');
        }

        // 记录删除操作
        const operationId = await recordOperation(ctx, 'remove', {
            fileId: id
        });

        console.log(`File ${id} remove operation recorded with ID: ${operationId}`);
        return { success: true, operationId };
    }, context, fileId);
}

/**
 * 批量删除文件
 * @param {Object} context - 上下文对象
 * @param {Array} fileIds - 文件 ID 数组
 */
export async function batchRemoveFilesFromIndex(context, fileIds) {
    return indexOperationMiddleware(async (ctx, ids) => {
        // 验证输入
        if (!Array.isArray(ids) || ids.length === 0) {
            return {
                success: true,
                operationId: null,
                totalProcessed: 0,
                message: 'No fileIds to process'
            };
        }

        // 去重和过滤空值
        const uniqueFileIds = [...new Set(ids.filter(id => id && typeof id === 'string'))];
        
        if (uniqueFileIds.length === 0) {
            return {
                success: true,
                operationId: null,
                totalProcessed: 0,
                message: 'No valid file IDs to process'
            };
        }

        // 记录批量删除操作
        const operationId = await recordOperation(ctx, 'batch_remove', {
            fileIds: uniqueFileIds
        });

        console.log(`Batch remove operation recorded with ID: ${operationId}, ${uniqueFileIds.length} files`);
        return {
            success: true,
            operationId,
            totalProcessed: uniqueFileIds.length,
            originalCount: ids.length,
            uniqueCount: uniqueFileIds.length
        };
    }, context, fileIds);
}

/**
 * 移动文件（修改文件ID）
 * @param {Object} context - 上下文对象，包含 env 和其他信息
 * @param {string} originalFileId - 原文件 ID
 * @param {string} newFileId - 新文件 ID
 * @param {Object} newMetadata - 新的元数据，如果为null则获取原文件的metadata
 * @returns {Object} 返回操作结果 { success, operationId?, error? }
 */
export async function moveFileInIndex(context, originalFileId, newFileId, newMetadata = null) {
    return indexOperationMiddleware(async (ctx, originalId, newId, metadata) => {
        const { env } = ctx;
        const db = getDatabase(env);

        // 验证输入
        if (!originalId || !newId || typeof originalId !== 'string' || typeof newId !== 'string') {
            throw new Error('Invalid file IDs');
        }

        if (originalId === newId) {
            return {
                success: true,
                operationId: null,
                message: 'Original and new file IDs are the same'
            };
        }

        // 确定最终的metadata
        let finalMetadata = metadata;
        if (finalMetadata === null) {
            // 如果没有提供新metadata，尝试从数据库中获取
            try {
                const fileData = await db.getWithMetadata(newId);
                finalMetadata = fileData.metadata || {};
            } catch (error) {
                console.warn(`Failed to get metadata for new file ${newId}:`, error);
                finalMetadata = {};
            }
        }

        // 记录移动操作
        const operationId = await recordOperation(ctx, 'move', {
            originalFileId: originalId,
            newFileId: newId,
            metadata: finalMetadata
        });

        console.log(`File move operation from ${originalId} to ${newId} recorded with ID: ${operationId}`);
        return { success: true, operationId };
    }, context, originalFileId, newFileId, newMetadata);
}

/**
 * 批量移动文件
 * @param {Object} context - 上下文对象，包含 env 和其他信息
 * @param {Array} moveOperations - 移动操作数组，每个元素包含 { originalFileId, newFileId, metadata? }
 * @returns {Object} 返回操作结果 { operationId, totalProcessed }
 */
export async function batchMoveFilesInIndex(context, moveOperations) {
    return indexOperationMiddleware(async (ctx, operations) => {
        const { env } = ctx;
        const db = getDatabase(env);

        // 验证输入
        if (!Array.isArray(operations) || operations.length === 0) {
            return {
                success: true,
                operationId: null,
                totalProcessed: 0,
                message: 'No move operations to process'
            };
        }

        // 处理每个移动操作的metadata
        const processedOperations = [];
        for (const operation of operations) {
            const { originalFileId, newFileId, metadata } = operation;

            // 验证文件ID
            if (!originalFileId || !newFileId || 
                typeof originalFileId !== 'string' || 
                typeof newFileId !== 'string') {
                console.warn('Skipping invalid move operation:', operation);
                continue;
            }

            if (originalFileId === newFileId) {
                console.warn('Skipping move operation with same IDs:', operation);
                continue;
            }

            // 确定最终的metadata
            let finalMetadata = metadata;
            if (finalMetadata === null || finalMetadata === undefined) {
                // 如果没有提供新metadata，尝试从数据库中获取
                try {
                    const fileData = await db.getWithMetadata(newFileId);
                    finalMetadata = fileData.metadata || {};
                } catch (error) {
                    console.warn(`Failed to get metadata for new file ${newFileId}:`, error);
                    finalMetadata = {};
                }
            }

            processedOperations.push({
                originalFileId,
                newFileId,
                metadata: finalMetadata
            });
        }

        if (processedOperations.length === 0) {
            return {
                success: true,
                operationId: null,
                totalProcessed: 0,
                message: 'No valid move operations to process'
            };
        }

        // 记录批量移动操作
        const operationId = await recordOperation(ctx, 'batch_move', {
            operations: processedOperations
        });

        console.log(`Batch move operation recorded with ID: ${operationId}, ${processedOperations.length} operations`);
        return {
            success: true,
            operationId,
            totalProcessed: processedOperations.length,
            originalCount: operations.length,
            validCount: processedOperations.length
        };
    }, context, moveOperations);
}

/**
 * 合并所有挂起的操作到索引中
 * @param {Object} context - 上下文对象
 * @param {Object} options - 选项
 * @param {boolean} options.cleanupAfterMerge - 合并后是否清理操作记录，默认为 true
 * @returns {Object} 合并结果
 */
export async function mergeOperationsToIndex(context, options = {}) {
    return indexOperationMiddleware(async (ctx, opts) => {
        const { request } = ctx;
        const { cleanupAfterMerge = true } = opts;
        
        console.log('Starting operations merge...');
        
        // 获取当前索引
        const currentIndex = await getIndex(ctx);
        if (currentIndex.success === false) {
            console.error('Failed to get current index for merge');
            return {
                success: false,
                error: 'Failed to get current index'
            };
        }

        // 获取所有待处理的操作
        const operationsResult = await getAllPendingOperations(ctx, currentIndex.lastOperationId);

        const operations = operationsResult.operations;
        const isALLOperations = operationsResult.isAll;

        if (operations.length === 0) {
            console.log('No pending operations to merge');
            return {
                success: true,
                processedOperations: 0,
                message: 'No pending operations'
            };
        }

        console.log(`Found ${operations.length} pending operations to merge. Is all operations: ${isALLOperations}`);

        // 按时间戳排序操作，确保按正确顺序应用
        operations.sort((a, b) => a.timestamp - b.timestamp);

        // 创建索引的副本进行操作
        const workingIndex = currentIndex;
        let operationsProcessed = 0;
        let addedCount = 0;
        let removedCount = 0;
        let movedCount = 0;
        let updatedCount = 0;
        const processedOperationIds = [];

        // 应用每个操作
        for (const operation of operations) {
            try {
                switch (operation.type) {
                    case 'add':
                        const addResult = applyAddOperation(workingIndex, operation.data);
                        if (addResult.added) addedCount++;
                        if (addResult.updated) updatedCount++;
                        break;
                        
                    case 'remove':
                        if (applyRemoveOperation(workingIndex, operation.data)) {
                            removedCount++;
                        }
                        break;
                        
                    case 'move':
                        if (applyMoveOperation(workingIndex, operation.data)) {
                            movedCount++;
                        }
                        break;
                        
                    case 'batch_add':
                        const batchAddResult = applyBatchAddOperation(workingIndex, operation.data);
                        addedCount += batchAddResult.addedCount;
                        updatedCount += batchAddResult.updatedCount;
                        break;
                        
                    case 'batch_remove':
                        removedCount += applyBatchRemoveOperation(workingIndex, operation.data);
                        break;
                        
                    case 'batch_move':
                        movedCount += applyBatchMoveOperation(workingIndex, operation.data);
                        break;
                        
                    default:
                        console.warn(`Unknown operation type: ${operation.type}`);
                        continue;
                }
                
                operationsProcessed++;
                processedOperationIds.push(operation.id);

                // 增加协作点
                if (operationsProcessed % 3 === 0) {
                    await new Promise(resolve => setTimeout(resolve, 0));
                }
                
            } catch (error) {
                console.error(`Error applying operation ${operation.id}:`, error);
            }
        }

        // 如果有任何修改，保存索引
        if (operationsProcessed > 0) {
            workingIndex.lastUpdated = Date.now();
            workingIndex.totalCount = workingIndex.files.length;
            
            // 记录最后处理的操作ID
            if (processedOperationIds.length > 0) {
                workingIndex.lastOperationId = processedOperationIds[processedOperationIds.length - 1];
            }

            // 保存更新后的索引（使用分块格式）
            const saveSuccess = await saveChunkedIndex(ctx, workingIndex);
            if (!saveSuccess) {
                console.error('Failed to save chunked index');
                return {
                    success: false,
                    error: 'Failed to save index'
                };
            }

            console.log(`Index updated: ${addedCount} added, ${updatedCount} updated, ${removedCount} removed, ${movedCount} moved`);
        }

        // 清理已处理的操作记录
        if (cleanupAfterMerge && processedOperationIds.length > 0) {
            await cleanupOperations(ctx, processedOperationIds);
        }

        // 如果未处理完所有操作，调用 merge-operations API 递归处理
        if (!isALLOperations) {
            console.log('There are remaining operations, will process them in subsequent calls.');

            if (request) {
                const headers = new Headers(request.headers);
                const originUrl = new URL(request.url);
                const mergeUrl = `${originUrl.protocol}//${originUrl.host}/api/manage/list?action=merge-operations`;

                await fetch(mergeUrl, { method: 'GET', headers });
            }

            return {
                success: false,
                error: 'There are remaining operations, will process them in subsequent calls.',
                processedOperations: operationsProcessed,
                addedCount,
                updatedCount,
                removedCount,
                movedCount
            };
        }

        const result = {
            success: true,
            processedOperations: operationsProcessed,
            addedCount,
            updatedCount,
            removedCount,
            movedCount,
            totalFiles: workingIndex.totalCount
        };

        console.log('Operations merge completed:', result);
        return result;
    }, context, options);
}

/**
 * 读取文件索引，支持搜索和分页
 * @param {Object} context - 上下文对象
 * @param {Object} options - 查询选项
 * @param {string} options.search - 搜索关键字
 * @param {string} options.directory - 目录过滤
 * @param {number} options.start - 起始位置
 * @param {number} options.count - 返回数量，-1 表示返回所有
 * @param {string} options.channel - 渠道过滤
 * @param {string} options.listType - 列表类型过滤
 * @param {Array<string>} options.includeTags - 必须包含的标签数组
 * @param {Array<string>} options.excludeTags - 必须排除的标签数组
 * @param {boolean} options.countOnly - 仅返回总数
 * @param {boolean} options.includeSubdirFiles - 是否包含子目录下的文件
 * @param {string} options.sortBy - 排序字段
 * @param {string} options.sortOrder - 排序顺序
 */
export async function readIndex(context, options = {}) {
    return indexOperationMiddleware(async (ctx, opts) => {
        const {
            search = '',
            directory = '',
            start = 0,
            count = 50,
            channel = '',
            listType = '',
            includeTags = [],
            excludeTags = [],
            countOnly = false,
            includeSubdirFiles = false,
            sortBy = 'TimeStamp',
            sortOrder = 'desc'
        } = opts;
        
        // 处理目录满足无头有尾的格式，根目录为空
        const dirPrefix = directory === '' || directory.endsWith('/') ? directory : directory + '/';

        // 处理挂起的操作
        const mergeResult = await mergeOperationsToIndex(ctx);
        if (!mergeResult.success && mergeResult.error !== 'No pending operations') {
            console.warn('Merge operations failed but continuing with read:', mergeResult.error);
        }

        // 获取当前索引
        const index = await getIndex(ctx);
        if (!index.success) {
            throw new Error('Failed to get index');
        }

        // 创建查询缓存键
        const cacheKey = JSON.stringify({
            search, directory, channel, listType,
            includeTags, excludeTags, sortBy, sortOrder
        });
        
        // 检查查询缓存
        if (!countOnly && queryCache[cacheKey]) {
            const cached = queryCache[cacheKey];
            if (Date.now() - cached.timestamp < QUERY_CACHE_TTL) {
                // 从缓存中获取并分页
                const resultFiles = cached.data.slice(start, start + count);
                return {
                    ...cached.metadata,
                    files: resultFiles,
                    returnedCount: resultFiles.length,
                    fromCache: true,
                    success: true
                };
            }
        }

        let filteredFiles = index.files;

        // 应用所有过滤器
        filteredFiles = applyFilters(filteredFiles, {
            directory: dirPrefix,
            channel,
            listType,
            includeTags,
            excludeTags,
            search
        });

        // 排序
        filteredFiles = sortFiles(filteredFiles, sortBy, sortOrder);

        // 如果只需要总数
        if (countOnly) {
            return {
                totalCount: filteredFiles.length,
                indexLastUpdated: index.lastUpdated,
                success: true
            };
        }

        // 计算统计信息
        const stats = calculateDirectoryStats(filteredFiles, dirPrefix, includeSubdirFiles);

        // 分页处理
        const resultFiles = filteredFiles.slice(start, start + count);

        // 缓存结果
        if (filteredFiles.length > 0 && !countOnly) {
            queryCache[cacheKey] = {
                data: filteredFiles,
                metadata: {
                    ...stats,
                    indexLastUpdated: index.lastUpdated
                },
                timestamp: Date.now()
            };
            
            // 限制缓存大小
            limitCacheSize(queryCache, MAX_QUERY_CACHE_SIZE);
        }

        return {
            files: resultFiles,
            ...stats,
            returnedCount: resultFiles.length,
            success: true,
            fromCache: false
        };
    }, context, options);
}

/**
 * 重建索引（从数据库中的所有文件重新构建索引）
 * @param {Object} context - 上下文对象
 * @param {Function} progressCallback - 进度回调函数
 */
export async function rebuildIndex(context, progressCallback = null) {
    return indexOperationMiddleware(async (ctx, progressCb) => {
        const { env, waitUntil } = ctx;
        const db = getDatabase(env);

        console.log('Starting index rebuild...');
        
        let cursor = null;
        let processedCount = 0;
        const newIndex = {
            files: [],
            lastUpdated: Date.now(),
            totalCount: 0,
            lastOperationId: null
        };

        // 分批读取所有文件
        while (true) {
            const response = await db.list({
                limit: KV_LIST_LIMIT,
                cursor: cursor
            });

            cursor = response.cursor;

            for (const item of response.keys) {
                // 跳过管理相关的键
                if (item.name.startsWith('manage@') || item.name.startsWith('chunk_')) {
                    continue;
                }

                // 跳过没有元数据的文件
                if (!item.metadata || !item.metadata.TimeStamp) {
                    continue;
                }

                // 构建文件索引项
                const fileItem = {
                    id: item.name,
                    metadata: item.metadata || {}
                };

                newIndex.files.push(fileItem);
                processedCount++;

                // 报告进度
                if (progressCb && processedCount % 100 === 0) {
                    progressCb(processedCount);
                }
            }

            if (!cursor) break;
            
            // 添加协作点
            await new Promise(resolve => setTimeout(resolve, 10));
        }

        // 按时间戳倒序排序
        newIndex.files.sort((a, b) => b.metadata.TimeStamp - a.metadata.TimeStamp);

        newIndex.totalCount = newIndex.files.length;

        // 保存新索引（使用分块格式）
        const saveSuccess = await saveChunkedIndex(ctx, newIndex);
        if (!saveSuccess) {
            console.error('Failed to save chunked index during rebuild');
            return {
                success: false,
                error: 'Failed to save rebuilt index',
                processedCount
            };
        }

        // 清除旧的操作记录和多余索引
        waitUntil(deleteAllOperations(ctx));
        waitUntil(clearChunkedIndex(ctx, true));

        // 清除缓存
        clearIndexCache();
        clearQueryCache();

        console.log(`Index rebuild completed. Processed ${processedCount} files, indexed ${newIndex.totalCount} files.`);
        return {
            success: true,
            processedCount,
            indexedCount: newIndex.totalCount
        };
    }, context, progressCallback);
}

/**
 * 获取索引信息
 * @param {Object} context - 上下文对象
 */
export async function getIndexInfo(context) {
    try {
        const index = await getIndex(context);

        // 检查索引是否成功获取
        if (index.success === false) {
            return {
                success: false,
                error: 'Failed to retrieve index',
                message: 'Index is not available or corrupted'
            }
        }

        // 统计各渠道文件数量
        const channelStats = {};
        const directoryStats = {};
        const typeStats = {};
        
        index.files.forEach(file => {
            // 渠道统计
            let channel = file.metadata.Channel || 'Telegraph';
            if (channel === 'TelegramNew') {
                channel = 'Telegram';
            }
            channelStats[channel] = (channelStats[channel] || 0) + 1;

            // 目录统计
            const dir = file.metadata.Directory || extractDirectory(file.id) || '/';
            directoryStats[dir] = (directoryStats[dir] || 0) + 1;
            
            // 类型统计
            let listType = file.metadata.ListType || 'None';
            const label = file.metadata.Label || 'None';
            if (listType !== 'White' && label === 'adult') {
                listType = 'Block';
            }
            typeStats[listType] = (typeStats[listType] || 0) + 1;
        });

        return {
            success: true,
            totalFiles: index.totalCount,
            lastUpdated: index.lastUpdated,
            channelStats,
            directoryStats,
            typeStats,
            oldestFile: index.files[index.files.length - 1],
            newestFile: index.files[0]
        };
    } catch (error) {
        console.error('Error getting index info:', error);
        return {
            success: false,
            error: error.message
        };
    }
}

/**
 * 获取索引元数据（轻量级，只读取 meta，不读取整个索引）
 * 用于容量检查等场景，避免读取整个索引
 * @param {Object} context - 上下文对象
 * @returns {Object} 索引元数据，包含 totalCount, totalSizeMB, channelStats 等
 */
export async function getIndexMeta(context) {
    const { env } = context;
    const db = getDatabase(env);

    try {
        const metadataStr = await db.get(INDEX_META_KEY);
        if (!metadataStr) {
            return {
                success: false,
                totalCount: 0,
                totalSizeMB: 0,
                channelStats: {}
            };
        }

        const metadata = JSON.parse(metadataStr);
        return {
            success: true,
            totalCount: metadata.totalCount || 0,
            totalSizeMB: metadata.totalSizeMB || 0,
            channelStats: metadata.channelStats || {},
            lastUpdated: metadata.lastUpdated
        };
    } catch (error) {
        console.error('Error getting index meta:', error);
        return {
            success: false,
            error: error.message,
            totalCount: 0,
            totalSizeMB: 0,
            channelStats: {}
        };
    }
}

/**
 * 获取索引统计信息
 * @returns {Object} 统计信息
 */
export function getIndexStats() {
    const avgDuration = indexStats.totalOperations > 0 
        ? indexStats.totalDuration / indexStats.totalOperations 
        : 0;
    
    return {
        ...indexStats,
        averageDuration: avgDuration,
        errorRate: indexStats.totalOperations > 0 
            ? (indexStats.errors / indexStats.totalOperations) * 100 
            : 0
    };
}

/**
 * 清除索引缓存
 */
export function clearIndexCache() {
    indexCache.data = null;
    indexCache.timestamp = 0;
}

/**
 * 清除查询缓存
 */
export function clearQueryCache() {
    Object.keys(queryCache).forEach(key => {
        delete queryCache[key];
    });
}

/**
 * 清除所有缓存
 */
export function clearAllCaches() {
    clearIndexCache();
    clearQueryCache();
}

/* ============= 原子操作相关函数 ============= */

/**
 * 生成唯一的操作ID
 */
function generateOperationId() {
    const timestamp = Date.now();
    const random = Math.random().toString(36).substring(2, 9);
    return `${timestamp}_${random}`;
}

/**
 * 记录原子操作
 * @param {Object} context - 上下文对象，包含 env 和其他信息
 * @param {string} type - 操作类型
 * @param {Object} data - 操作数据
 */
async function recordOperation(context, type, data) {
    const { env } = context;
    const db = getDatabase(env);

    const operationId = generateOperationId();
    const operation = {
        type,
        timestamp: Date.now(),
        data
    };
    
    const operationKey = OPERATION_KEY_PREFIX + operationId;
    await db.put(operationKey, JSON.stringify(operation));

    return operationId;
}

/**
 * 获取所有待处理的操作
 * @param {Object} context - 上下文对象
 * @param {string} lastOperationId - 最后处理的操作ID
 */
async function getAllPendingOperations(context, lastOperationId = null) {
    const { env } = context;
    const db = getDatabase(env);

    const operations = [];

    let cursor = null;
    const MAX_OPERATION_COUNT = 30; // 单次获取的最大操作数量
    let isALL = true; // 是否获取了所有操作
    let operationCount = 0;

    try {
        while (true) {
            const response = await db.list({
                prefix: OPERATION_KEY_PREFIX,
                limit: KV_LIST_LIMIT,
                cursor: cursor
            });
            
            for (const item of response.keys) {
                // 如果指定了lastOperationId，跳过已处理的操作
                if (lastOperationId && item.name <= OPERATION_KEY_PREFIX + lastOperationId) {
                    continue;
                }
                
                if (operationCount >= MAX_OPERATION_COUNT) {
                    isALL = false; // 达到最大操作数量，停止获取
                    break;
                }

                try {
                    const operationData = await db.get(item.name);
                    if (operationData) {
                        const operation = JSON.parse(operationData);
                        operation.id = item.name.substring(OPERATION_KEY_PREFIX.length);
                        operations.push(operation);
                        operationCount++;
                    }
                } catch (error) {
                    isALL = false;
                    console.warn(`Failed to parse operation ${item.name}:`, error);
                }
            }
            
            cursor = response.cursor;
            if (!cursor || operationCount >= MAX_OPERATION_COUNT) break;
        }
    } catch (error) {
        console.error('Error getting pending operations:', error);
    }
    
    return {
        operations,
        isAll: isALL,
    }
}

/**
 * 应用添加操作
 * @param {Object} index - 索引对象
 * @param {Object} data - 操作数据
 */
function applyAddOperation(index, data) {
    const { fileId, metadata } = data;
    
    // 检查文件是否已存在
    const existingIndex = index.files.findIndex(file => file.id === fileId);
    
    const fileItem = {
        id: fileId,
        metadata: metadata || {}
    };
    
    if (existingIndex !== -1) {
        // 更新现有文件
        index.files[existingIndex] = fileItem;
        return { added: false, updated: true };
    } else {
        // 添加新文件
        insertFileInOrder(index.files, fileItem);
        return { added: true, updated: false };
    }
}

/**
 * 应用删除操作
 * @param {Object} index - 索引对象
 * @param {Object} data - 操作数据
 */
function applyRemoveOperation(index, data) {
    const { fileId } = data;
    const initialLength = index.files.length;
    index.files = index.files.filter(file => file.id !== fileId);
    return index.files.length < initialLength;
}

/**
 * 应用移动操作
 * @param {Object} index - 索引对象
 * @param {Object} data - 操作数据
 */
function applyMoveOperation(index, data) {
    const { originalFileId, newFileId, metadata } = data;
    
    const originalIndex = index.files.findIndex(file => file.id === originalFileId);
    if (originalIndex === -1) {
        return false; // 原文件不存在
    }
    
    // 更新文件ID和元数据
    index.files[originalIndex] = {
        id: newFileId,
        metadata: metadata || index.files[originalIndex].metadata
    };
    
    return true;
}

/**
 * 应用批量添加操作 - 使用映射提高查找效率
 * @param {Object} index - 索引对象
 * @param {Object} data - 操作数据
 */
function applyBatchAddOperation(index, data) {
    const { files, options } = data;
    const { skipExisting = false } = options || {};
    
    let addedCount = 0;
    let updatedCount = 0;
    
    // 创建现有文件ID的映射以提高查找效率
    const existingFilesMap = new Map();
    index.files.forEach((file, idx) => {
        existingFilesMap.set(file.id, idx);
    });
    
    // 分离新增和更新的文件
    const newFiles = [];
    const updateFiles = [];
    
    for (const fileData of files) {
        const { fileId, metadata } = fileData;
        const fileItem = {
            id: fileId,
            metadata: metadata || {}
        };
        
        const existingIndex = existingFilesMap.get(fileId);
        
        if (existingIndex !== undefined) {
            if (!skipExisting) {
                updateFiles.push({ index: existingIndex, file: fileItem });
                updatedCount++;
            }
        } else {
            newFiles.push(fileItem);
            addedCount++;
        }
    }
    
    // 批量更新现有文件
    updateFiles.forEach(({ index, file }) => {
        index.files[index] = file;
    });
    
    // 批量插入新文件（保持排序）
    if (newFiles.length > 0) {
        // 先对新文件按时间戳排序
        newFiles.sort((a, b) => {
            const aTime = a.metadata.TimeStamp || 0;
            const bTime = b.metadata.TimeStamp || 0;
            return bTime - aTime; // 倒序
        });
        
        // 合并两个有序数组
        index.files = mergeSortedArrays(index.files, newFiles);
    }
    
    return { addedCount, updatedCount };
}

/**
 * 应用批量删除操作
 * @param {Object} index - 索引对象
 * @param {Object} data - 操作数据
 */
function applyBatchRemoveOperation(index, data) {
    const { fileIds } = data;
    const fileIdSet = new Set(fileIds);
    const initialLength = index.files.length;
    
    index.files = index.files.filter(file => !fileIdSet.has(file.id));
    
    return initialLength - index.files.length;
}

/**
 * 应用批量移动操作
 * @param {Object} index - 索引对象
 * @param {Object} data - 操作数据
 */
function applyBatchMoveOperation(index, data) {
    const { operations } = data;
    let movedCount = 0;
    
    // 创建现有文件ID的映射以提高查找效率
    const existingFilesMap = new Map();
    index.files.forEach((file, idx) => {
        existingFilesMap.set(file.id, idx);
    });
    
    for (const operation of operations) {
        const { originalFileId, newFileId, metadata } = operation;
        
        const originalIndex = existingFilesMap.get(originalFileId);
        if (originalIndex !== undefined) {
            // 更新映射
            existingFilesMap.delete(originalFileId);
            existingFilesMap.set(newFileId, originalIndex);
            
            // 更新文件信息
            index.files[originalIndex] = {
                id: newFileId,
                metadata: metadata || index.files[originalIndex].metadata
            };
            
            movedCount++;
        }
    }
    
    return movedCount;
}

/**
 * 并发清理指定的原子操作记录
 * @param {Object} context - 上下文对象
 * @param {Array} operationIds - 要清理的操作ID数组
 * @param {number} concurrency - 并发数量，默认为10
 */
async function cleanupOperations(context, operationIds, concurrency = 10) {
    const { env } = context;
    const db = getDatabase(env);

    try {
        console.log(`Cleaning up ${operationIds.length} processed operations with concurrency ${concurrency}...`);
        
        let deletedCount = 0;
        let errorCount = 0;
        
        // 创建删除任务数组
        const deleteTasks = operationIds.map(operationId => {
            const operationKey = OPERATION_KEY_PREFIX + operationId;
            return async () => {
                try {
                    await db.delete(operationKey);
                    deletedCount++;
                } catch (error) {
                    console.error(`Error deleting operation ${operationId}:`, error);
                    errorCount++;
                }
            };
        });
        
        // 使用并发控制执行删除操作
        await promiseLimit(deleteTasks, concurrency);

        console.log(`Successfully cleaned up ${deletedCount} operations, ${errorCount} operations failed.`);
        return {
            success: true,
            deletedCount: deletedCount,
            errorCount: errorCount,
        };

    } catch (error) {
        console.error('Error cleaning up operations:', error);
        return {
            success: false,
            error: error.message
        };
    }
}

/**
 * 删除所有原子操作记录
 * @param {Object} context - 上下文对象，包含 env 和其他信息
 * @returns {Object} 删除结果 { success, deletedCount, errors?, totalFound? }
 */
export async function deleteAllOperations(context) {
    return indexOperationMiddleware(async (ctx) => {
        const { request, env } = ctx;
        const db = getDatabase(env);
        
        console.log('Starting to delete all atomic operations...');
        
        // 获取所有原子操作
        const allOperationIds = [];
        let cursor = null;
        let totalFound = 0;
        
        // 首先收集所有操作键
        while (true) {
            const response = await db.list({
                prefix: OPERATION_KEY_PREFIX,
                limit: KV_LIST_LIMIT,
                cursor: cursor
            });
            
            for (const item of response.keys) {
                allOperationIds.push(item.name.substring(OPERATION_KEY_PREFIX.length));
                totalFound++;
            }
            
            cursor = response.cursor;
            if (!cursor) break;
        }
        
        if (totalFound === 0) {
            console.log('No atomic operations found to delete');
            return {
                success: true,
                deletedCount: 0,
                totalFound: 0,
                message: 'No operations to delete'
            };
        }
        
        console.log(`Found ${totalFound} atomic operations to delete`);

        // 限制单次删除的数量
        const MAX_DELETE_BATCH = 40;
        const toDeleteOperationIds = allOperationIds.slice(0, MAX_DELETE_BATCH);
   
        // 批量删除原子操作
        const cleanupResult = await cleanupOperations(ctx, toDeleteOperationIds);

        // 剩余未删除的操作，调用 delete-operations API 进行递归删除
        if (allOperationIds.length > MAX_DELETE_BATCH || cleanupResult.errorCount > 0) {
            console.warn(`Too many operations (${allOperationIds.length}), only deleting first ${cleanupResult.deletedCount}. The remaining operations will be deleted in subsequent calls.`);
            // 复制请求头，用于鉴权
            if (request) {
                const headers = new Headers(request.headers);
                const originUrl = new URL(request.url);
                const deleteUrl = `${originUrl.protocol}//${originUrl.host}/api/manage/list?action=delete-operations`
                
                await fetch(deleteUrl, {
                    method: 'GET',
                    headers: headers
                });
            }

            return {
                success: false,
                error: 'There are remaining operations, will process them in subsequent calls.',
                deletedCount: cleanupResult.deletedCount,
                totalFound: totalFound,
                remainingCount: allOperationIds.length - cleanupResult.deletedCount
            };
        } else {
            console.log(`Delete all operations completed`);
            return {
                success: true,
                deletedCount: cleanupResult.deletedCount,
                totalFound: totalFound,
                message: 'All operations deleted successfully'
            };
        }
    }, context);
}

/* ============= 工具函数 ============= */

/**
 * 获取索引（带缓存）
 * @param {Object} context - 上下文对象
 */
async function getIndex(context) {
    const { waitUntil } = context;
    
    // 检查缓存是否有效
    const now = Date.now();
    if (indexCache.data && now - indexCache.timestamp < CACHE_TTL) {
        return { ...indexCache.data };
    }
    
    try {
        // 首先尝试加载分块索引
        const index = await loadChunkedIndex(context);
        if (index.success) {
            // 更新缓存
            indexCache.data = index;
            indexCache.timestamp = now;
            return index;
        } else {
            // 如果加载失败，触发重建索引
            waitUntil(rebuildIndex(context));
        }
    } catch (error) {
        console.warn('Error reading index, creating new one:', error);
        waitUntil(rebuildIndex(context));
    }
    
    // 返回空的索引结构
    return {
        files: [],
        lastUpdated: Date.now(),
        totalCount: 0,
        lastOperationId: null,
        success: false,
    };
}

/**
 * 从文件路径提取目录（内部函数）
 * @param {string} filePath - 文件路径
 */
function extractDirectory(filePath) {
    const lastSlashIndex = filePath.lastIndexOf('/');
    if (lastSlashIndex === -1) {
        return ''; // 根目录
    }
    return filePath.substring(0, lastSlashIndex + 1); // 包含最后的斜杠
}

/**
 * 将文件按时间戳倒序插入到已排序的数组中
 * @param {Array} sortedFiles - 已按时间戳倒序排序的文件数组
 * @param {Object} fileItem - 要插入的文件项
 */
function insertFileInOrder(sortedFiles, fileItem) {
    const fileTimestamp = fileItem.metadata.TimeStamp || 0;
    
    // 如果数组为空或新文件时间戳比第一个文件更新，直接插入到开头
    if (sortedFiles.length === 0 || fileTimestamp >= (sortedFiles[0].metadata.TimeStamp || 0)) {
        sortedFiles.unshift(fileItem);
        return;
    }
    
    // 如果新文件时间戳比最后一个文件更旧，直接添加到末尾
    if (fileTimestamp <= (sortedFiles[sortedFiles.length - 1].metadata.TimeStamp || 0)) {
        sortedFiles.push(fileItem);
        return;
    }
    
    // 使用二分查找找到正确的插入位置
    let left = 0;
    let right = sortedFiles.length;
    
    while (left < right) {
        const mid = Math.floor((left + right) / 2);
        const midTimestamp = sortedFiles[mid].metadata.TimeStamp || 0;
        
        if (fileTimestamp >= midTimestamp) {
            right = mid;
        } else {
            left = mid + 1;
        }
    }
    
    // 在找到的位置插入文件
    sortedFiles.splice(left, 0, fileItem);
}

/**
 * 并发控制工具函数 - 限制同时执行的Promise数量
 * @param {Array} tasks - 任务数组，每个任务是一个返回Promise的函数
 * @param {number} concurrency - 并发数量
 * @param {Object} options - 选项
 * @param {boolean} options.failFast - 是否快速失败，默认为 false
 * @returns {Promise<Array>} 所有任务的结果数组
 */
async function promiseLimit(tasks, concurrency = BATCH_SIZE, options = {}) {
    const { failFast = false } = options;
    const results = [];
    const executing = [];
    let errorOccurred = false;
    
    for (let i = 0; i < tasks.length; i++) {
        if (errorOccurred && failFast) {
            results[i] = { error: 'Operation cancelled due to previous error' };
            continue;
        }
        
        const task = tasks[i];
        const promise = Promise.resolve().then(() => task()).then(result => {
            results[i] = { success: true, result };
            return result;
        }).catch(error => {
            results[i] = { success: false, error: error.message };
            if (failFast) {
                errorOccurred = true;
            }
            return null;
        }).finally(() => {
            const index = executing.indexOf(promise);
            if (index >= 0) {
                executing.splice(index, 1);
            }
        });
        
        executing.push(promise);
        
        if (executing.length >= concurrency) {
            await Promise.race(executing);
        }
    }
    
    // 等待所有剩余的Promise完成
    await Promise.all(executing);
    
    // 如果有错误且不是快速失败，检查所有结果
    if (!failFast && results.some(r => !r.success)) {
        const errors = results.filter(r => !r.success).map(r => r.error);
        throw new Error(`Some operations failed: ${errors.join(', ')}`);
    }
    
    return results;
}

/**
 * 保存分块索引到数据库
 * @param {Object} context - 上下文对象，包含 env
 * @param {Object} index - 完整的索引对象
 * @returns {Promise<boolean>} 是否保存成功
 */
async function saveChunkedIndex(context, index) {
    const { env } = context;
    const db = getDatabase(env);
    
    try {
        const files = index.files || [];
        const chunks = [];
        
        // 将文件数组分块
        for (let i = 0; i < files.length; i += INDEX_CHUNK_SIZE) {
            const chunk = files.slice(i, i + INDEX_CHUNK_SIZE);
            chunks.push(chunk);
        }
        
        // 计算各渠道容量统计
        const channelStats = {};
        let totalSizeMB = 0;
        
        for (const file of files) {
            const channelName = file.metadata?.ChannelName;
            const fileSize = parseFloat(file.metadata?.FileSize) || 0;
            
            totalSizeMB += fileSize;
            
            if (channelName) {
                if (!channelStats[channelName]) {
                    channelStats[channelName] = { usedMB: 0, fileCount: 0 };
                }
                channelStats[channelName].usedMB += fileSize;
                channelStats[channelName].fileCount += 1;
            }
        }
        
        // 保存索引元数据（包含容量统计）
        const metadata = {
            lastUpdated: index.lastUpdated,
            totalCount: index.totalCount,
            totalSizeMB: Math.round(totalSizeMB * 100) / 100,
            channelStats,
            lastOperationId: index.lastOperationId,
            chunkCount: chunks.length,
            chunkSize: INDEX_CHUNK_SIZE
        };
        
        await db.put(INDEX_META_KEY, JSON.stringify(metadata));
        
        // 保存各个分块
        const savePromises = chunks.map((chunk, chunkId) => {
            const chunkKey = `${INDEX_KEY}_${chunkId}`;
            return db.put(chunkKey, JSON.stringify(chunk));
        });
        
        await Promise.all(savePromises);
        
        console.log(`Saved chunked index: ${chunks.length} chunks, ${files.length} total files, ${totalSizeMB.toFixed(2)} MB`);
        return true;
        
    } catch (error) {
        console.error('Error saving chunked index:', error);
        return false;
    }
}

/**
 * 从数据库加载分块索引
 * @param {Object} context - 上下文对象，包含 env
 * @returns {Promise<Object>} 完整的索引对象
 */
async function loadChunkedIndex(context) {
    const { env } = context;
    const db = getDatabase(env);

    try {
        // 首先获取元数据
        const metadataStr = await db.get(INDEX_META_KEY);
        if (!metadataStr) {
            throw new Error('Index metadata not found');
        }
        
        const metadata = JSON.parse(metadataStr);
        const files = [];
        
        // 并行加载所有分块
        const loadPromises = [];
        for (let chunkId = 0; chunkId < metadata.chunkCount; chunkId++) {
            const chunkKey = `${INDEX_KEY}_${chunkId}`;
            loadPromises.push(
                db.get(chunkKey).then(chunkStr => {
                    if (chunkStr) {
                        return JSON.parse(chunkStr);
                    }
                    return [];
                })
            );
        }
        
        const chunks = await Promise.all(loadPromises);
        
        // 合并所有分块
        chunks.forEach(chunk => {
            if (Array.isArray(chunk)) {
                files.push(...chunk);
            }
        });
        
        const index = {
            files,
            lastUpdated: metadata.lastUpdated,
            totalCount: metadata.totalCount,
            lastOperationId: metadata.lastOperationId,
            success: true
        };
        
        console.log(`Loaded chunked index: ${metadata.chunkCount} chunks, ${files.length} total files`);
        return index;
        
    } catch (error) {
        console.error('Error loading chunked index:', error);
        // 返回空的索引结构
        return {
            files: [],
            lastUpdated: Date.now(),
            totalCount: 0,
            lastOperationId: null,
            success: false,
        };
    }
}

/**
 * 清理分块索引
 * @param {Object} context - 上下文对象，包含 env
 * @param {boolean} onlyNonUsed - 是否仅清理未使用的分块索引，默认为 false
 * @returns {Promise<boolean>} 是否清理成功
 */
export async function clearChunkedIndex(context, onlyNonUsed = false) {
    return indexOperationMiddleware(async (ctx, onlyNonUsedFlag) => {
        const { env } = ctx;
        const db = getDatabase(env);
        
        console.log('Starting chunked index cleanup...');
        
        // 获取元数据
        const metadataStr = await db.get(INDEX_META_KEY);
        let chunkCount = 0;
        
        if (metadataStr) {
            const metadata = JSON.parse(metadataStr);
            chunkCount = metadata.chunkCount || 0;

            if (!onlyNonUsedFlag) {
                // 删除元数据
                await db.delete(INDEX_META_KEY).catch(() => {});
            }
        }

        // 删除分块
        const recordedChunks = []; // 现有的索引分块键
        let cursor = null;
        while (true) {
            const response = await db.list({
                prefix: INDEX_KEY,
                limit: KV_LIST_LIMIT,
                cursor: cursor
            });
            
            for (const item of response.keys) {
                recordedChunks.push(item.name);
            }

            cursor = response.cursor;
            if (!cursor) break;
        }

        const reservedChunks = [];
        if (onlyNonUsedFlag) {
            // 如果仅清理未使用的分块索引，保留当前在使用的分块
            for (let chunkId = 0; chunkId < chunkCount; chunkId++) {
                reservedChunks.push(`${INDEX_KEY}_${chunkId}`);
            }
        }

        const deletePromises = [];
        for (let chunkKey of recordedChunks) {
            if (reservedChunks.includes(chunkKey) || !chunkKey.startsWith(INDEX_KEY + '_')) {
                // 保留的分块和非分块键不删除
                continue;
            }

            deletePromises.push(
                db.delete(chunkKey).catch(() => {})
            );
        }

        if (recordedChunks.includes(INDEX_KEY)) {
            deletePromises.push(
                db.delete(INDEX_KEY).catch(() => {})
            );
        }

        await Promise.all(deletePromises);
        
        console.log(`Chunked index cleanup completed. Attempted to delete ${chunkCount} chunks.`);
        return {
            success: true,
            deletedChunks: deletePromises.length,
            reservedChunks: reservedChunks.length
        };
    }, context, onlyNonUsed);
}

/**
 * 获取索引的存储统计信息
 * @param {Object} context - 上下文对象，包含 env
 * @returns {Object} 存储统计信息
 */
export async function getIndexStorageStats(context) {
    const { env } = context;
    const db = getDatabase(env);

    try {
        // 获取元数据
        const metadataStr = await db.get(INDEX_META_KEY);
        if (!metadataStr) {
            return {
                success: false,
                error: 'No chunked index metadata found',
                isChunked: false
            };
        }
        
        const metadata = JSON.parse(metadataStr);
        
        // 检查各个分块的存在情况
        const chunkChecks = [];
        for (let chunkId = 0; chunkId < metadata.chunkCount; chunkId++) {
            const chunkKey = `${INDEX_KEY}_${chunkId}`;
            chunkChecks.push(
                db.get(chunkKey).then(data => ({
                    chunkId,
                    exists: !!data,
                    size: data ? data.length : 0
                }))
            );
        }
        
        const chunkResults = await Promise.all(chunkChecks);
        
        const stats = {
            success: true,
            isChunked: true,
            metadata,
            chunks: chunkResults,
            totalChunks: metadata.chunkCount,
            existingChunks: chunkResults.filter(c => c.exists).length,
            totalSize: chunkResults.reduce((sum, c) => sum + c.size, 0)
        };
        
        return stats;
        
    } catch (error) {
        console.error('Error getting index storage stats:', error);
        return {
            success: false,
            error: error.message,
            isChunked: false
        };
    }
}

/**
 * 应用所有过滤器
 * @param {Array} files - 文件数组
 * @param {Object} filters - 过滤条件
 * @returns {Array} 过滤后的文件数组
 */
function applyFilters(files, filters) {
    const { directory, channel, listType, includeTags, excludeTags, search } = filters;
    
    return files.filter(file => {
        // 目录过滤
        if (directory) {
            const fileDir = file.metadata.Directory || extractDirectory(file.id);
            if (!fileDir.startsWith(directory)) {
                return false;
            }
        }

        // 渠道过滤
        if (channel) {
            if (file.metadata.Channel?.toLowerCase() !== channel.toLowerCase()) {
                return false;
            }
        }

        // 列表类型过滤
        if (listType) {
            if (file.metadata.ListType !== listType) {
                return false;
            }
        }

        // 标签过滤
        if (includeTags.length > 0 || excludeTags.length > 0) {
            const fileTags = (file.metadata.Tags || []).map(t => t.toLowerCase());
            
            // 必须包含的标签
            if (includeTags.length > 0) {
                const hasAll = includeTags.every(tag => 
                    fileTags.includes(tag.toLowerCase())
                );
                if (!hasAll) return false;
            }
            
            // 必须排除的标签
            if (excludeTags.length > 0) {
                const hasAny = excludeTags.some(tag => 
                    fileTags.includes(tag.toLowerCase())
                );
                if (hasAny) return false;
            }
        }

        // 搜索过滤
        if (search) {
            const searchLower = search.toLowerCase();
            const matches = 
                file.metadata.FileName?.toLowerCase().includes(searchLower) ||
                file.id.toLowerCase().includes(searchLower);
            if (!matches) return false;
        }

        return true;
    });
}

/**
 * 排序文件数组
 * @param {Array} files - 文件数组
 * @param {string} sortBy - 排序字段
 * @param {string} sortOrder - 排序顺序
 * @returns {Array} 排序后的文件数组
 */
function sortFiles(files, sortBy, sortOrder) {
    return [...files].sort((a, b) => {
        let aValue, bValue;
        
        switch (sortBy) {
            case 'FileName':
                aValue = a.metadata.FileName?.toLowerCase() || '';
                bValue = b.metadata.FileName?.toLowerCase() || '';
                break;
            case 'Size':
                aValue = parseFloat(a.metadata.FileSize) || 0;
                bValue = parseFloat(b.metadata.FileSize) || 0;
                break;
            case 'Channel':
                aValue = a.metadata.Channel?.toLowerCase() || '';
                bValue = b.metadata.Channel?.toLowerCase() || '';
                break;
            case 'TimeStamp':
            default:
                aValue = a.metadata.TimeStamp || 0;
                bValue = b.metadata.TimeStamp || 0;
        }
        
        if (aValue < bValue) return sortOrder === 'asc' ? -1 : 1;
        if (aValue > bValue) return sortOrder === 'asc' ? 1 : -1;
        return 0;
    });
}

/**
 * 计算目录统计信息
 * @param {Array} files - 文件数组
 * @param {string} dirPrefix - 目录前缀
 * @param {boolean} includeSubdirFiles - 是否包含子目录文件
 * @returns {Object} 统计信息
 */
function calculateDirectoryStats(files, dirPrefix, includeSubdirFiles) {
    // 计算当前目录下的直接文件
    const directFiles = files.filter(file => {
        const fileDir = file.metadata.Directory || extractDirectory(file.id);
        return fileDir === dirPrefix;
    });
    const directFileCount = directFiles.length;

    // 提取目录信息
    const directories = new Set();
    files.forEach(file => {
        const fileDir = file.metadata.Directory || extractDirectory(file.id);
        if (fileDir && fileDir.startsWith(dirPrefix)) {
            const relativePath = fileDir.substring(dirPrefix.length);
            const firstSlashIndex = relativePath.indexOf('/');
            if (firstSlashIndex !== -1) {
                const subDir = dirPrefix + relativePath.substring(0, firstSlashIndex);
                directories.add(subDir);
            }
        }
    });

    // 直接子文件夹数目
    const directFolderCount = directories.size;

    return {
        directories: Array.from(directories),
        totalCount: files.length,
        directFileCount: directFileCount,
        directFolderCount: directFolderCount
    };
}

/**
 * 限制缓存大小
 * @param {Object} cache - 缓存对象
 * @param {number} maxSize - 最大大小
 */
function limitCacheSize(cache, maxSize) {
    const keys = Object.keys(cache);
    if (keys.length > maxSize) {
        // 按时间戳排序，删除最旧的
        const sortedKeys = keys.sort((a, b) => cache[a].timestamp - cache[b].timestamp);
        const keysToRemove = sortedKeys.slice(0, keys.length - maxSize);
        
        keysToRemove.forEach(key => {
            delete cache[key];
        });
    }
}

/**
 * 合并两个已排序的文件数组（按时间戳倒序）
 * @param {Array} arr1 - 第一个有序数组
 * @param {Array} arr2 - 第二个有序数组
 * @returns {Array} 合并后的有序数组
 */
function mergeSortedArrays(arr1, arr2) {
    const result = [];
    let i = 0, j = 0;
    
    while (i < arr1.length && j < arr2.length) {
        const time1 = arr1[i].metadata.TimeStamp || 0;
        const time2 = arr2[j].metadata.TimeStamp || 0;
        
        if (time1 >= time2) {
            result.push(arr1[i++]);
        } else {
            result.push(arr2[j++]);
        }
    }
    
    // 添加剩余元素
    while (i < arr1.length) result.push(arr1[i++]);
    while (j < arr2.length) result.push(arr2[j++]);
    
    return result;
}
