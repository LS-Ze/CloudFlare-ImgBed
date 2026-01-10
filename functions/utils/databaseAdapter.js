/**
 * 数据库适配器
 * 提供统一的接口，可以在KV和D1之间切换
 */

import { D1Database, createD1Database } from './d1Database.js';

/**
 * @typedef {Object} DatabaseAdapterOptions
 * @property {boolean} [enableCache=true] - 是否启用缓存
 * @property {number} [cacheTTL=300000] - 缓存有效期（毫秒）
 * @property {number} [maxCacheSize=1000] - 最大缓存大小
 * @property {boolean} [debug=false] - 是否启用调试模式
 * @property {number} [maxRetries=3] - 最大重试次数
 * @property {number} [retryDelay=1000] - 重试延迟（毫秒）
 */

/**
 * @typedef {Object} DatabaseConfig
 * @property {boolean} hasD1 - 是否配置了D1
 * @property {boolean} hasKV - 是否配置了KV
 * @property {boolean} usingD1 - 是否使用D1
 * @property {boolean} usingKV - 是否使用KV
 * @property {boolean} configured - 是否配置了数据库
 * @property {string} type - 数据库类型（'d1' 或 'kv'）
 */

/**
 * 缓存存储
 */
const adapterCache = new Map();

/**
 * 创建数据库适配器
 * @param {Object} env - 环境变量
 * @param {DatabaseAdapterOptions} [options={}] - 适配器选项
 * @returns {Object} 数据库适配器实例
 */
export function createDatabaseAdapter(env, options = {}) {
    try {
        // 检查是否配置了数据库
        const config = checkDatabaseConfig(env);
        
        if (!config.configured) {
            console.error('No database configured. Please configure either KV (env.img_url) or D1 (env.img_d1).');
            return null;
        }

        // 创建缓存键
        const cacheKey = `${config.type}:${JSON.stringify(options)}`;
        
        // 检查缓存
        if (adapterCache.has(cacheKey)) {
            const cached = adapterCache.get(cacheKey);
            if (Date.now() < cached.expires) {
                return cached.adapter;
            }
            adapterCache.delete(cacheKey);
        }

        let adapter;
        
        if (config.usingD1) {
            // 使用D1数据库
            adapter = createD1Database(env.img_d1, options);
            console.log('Created D1 database adapter');
        } else if (config.usingKV) {
            // 使用KV存储
            adapter = new KVAdapter(env.img_url, options);
            console.log('Created KV database adapter');
        } else {
            console.error('No supported database configured');
            return null;
        }

        // 缓存适配器实例
        adapterCache.set(cacheKey, {
            adapter,
            expires: Date.now() + 300000 // 缓存5分钟
        });

        return adapter;
    } catch (error) {
        console.error('Error creating database adapter:', error);
        return null;
    }
}

/**
 * KV适配器类
 * 保持与原有KV接口的兼容性，并添加增强功能
 */
class KVAdapter {
    /**
     * 创建KV适配器
     * @param {Object} kv - KV存储实例
     * @param {DatabaseAdapterOptions} [options={}] - 适配器选项
     */
    constructor(kv, options = {}) {
        if (!kv || typeof kv.get !== 'function') {
            throw new Error('Invalid KV instance');
        }

        this.kv = kv;
        this.options = {
            enableCache: true,
            cacheTTL: 300000, // 5分钟
            maxCacheSize: 1000,
            debug: false,
            maxRetries: 3,
            retryDelay: 1000,
            ...options
        };

        // 初始化缓存
        this.cache = this.options.enableCache ? new Map() : null;
        
        // 初始化性能监控
        this.performance = {
            queries: [],
            errors: []
        };

        // 绑定方法
        this.put = this.put.bind(this);
        this.get = this.get.bind(this);
        this.getWithMetadata = this.getWithMetadata.bind(this);
        this.delete = this.delete.bind(this);
        this.list = this.list.bind(this);
    }

    /**
     * 记录性能指标
     * @param {string} operation - 操作名称
     * @param {number} duration - 持续时间（毫秒）
     */
    recordPerformance(operation, duration) {
        this.performance.queries.push({
            operation,
            duration,
            timestamp: Date.now()
        });

        // 限制性能记录数量
        if (this.performance.queries.length > 1000) {
            this.performance.queries.shift();
        }
    }

    /**
     * 记录错误
     * @param {string} operation - 操作名称
     * @param {Error} error - 错误对象
     */
    recordError(operation, error) {
        this.performance.errors.push({
            operation,
            message: error.message,
            stack: error.stack,
            timestamp: Date.now()
        });

        // 限制错误记录数量
        if (this.performance.errors.length > 100) {
            this.performance.errors.shift();
        }
    }

    /**
     * 获取性能统计信息
     * @returns {Object} 性能统计
     */
    getPerformanceStats() {
        const stats = {
            totalQueries: this.performance.queries.length,
            totalErrors: this.performance.errors.length,
            operations: {}
        };

        // 按操作类型统计
        this.performance.queries.forEach(query => {
            if (!stats.operations[query.operation]) {
                stats.operations[query.operation] = {
                    count: 0,
                    avgDuration: 0,
                    minDuration: Infinity,
                    maxDuration: 0,
                    totalDuration: 0
                };
            }

            const opStats = stats.operations[query.operation];
            opStats.count++;
            opStats.totalDuration += query.duration;
            opStats.avgDuration = opStats.totalDuration / opStats.count;
            opStats.minDuration = Math.min(opStats.minDuration, query.duration);
            opStats.maxDuration = Math.max(opStats.maxDuration, query.duration);
        });

        // 错误统计
        stats.errorStats = {};
        this.performance.errors.forEach(error => {
            if (!stats.errorStats[error.operation]) {
                stats.errorStats[error.operation] = {
                    count: 0,
                    messages: new Set()
                };
            }

            const errorStats = stats.errorStats[error.operation];
            errorStats.count++;
            errorStats.messages.add(error.message);
        });

        // 转换Set为数组
        Object.values(stats.errorStats).forEach(errorStats => {
            errorStats.messages = Array.from(errorStats.messages);
        });

        return stats;
    }

    /**
     * 带重试的执行函数
     * @param {string} operation - 操作名称
     * @param {Function} func - 执行函数
     * @param {number} [retries=0] - 当前重试次数
     * @returns {Promise<any>} 执行结果
     */
    async executeWithRetry(operation, func, retries = 0) {
        const startTime = performance.now();
        
        try {
            const result = await func();
            const duration = performance.now() - startTime;
            
            this.recordPerformance(operation, duration);
            
            if (this.options.debug) {
                console.log(`[KVAdapter] ${operation} completed in ${duration.toFixed(2)}ms`);
            }
            
            return result;
        } catch (error) {
            const duration = performance.now() - startTime;
            this.recordError(operation, error);
            
            if (this.options.debug) {
                console.error(`[KVAdapter] ${operation} failed in ${duration.toFixed(2)}ms:`, error);
            }
            
            // 重试逻辑
            if (retries < this.options.maxRetries && this.isRetryableError(error)) {
                const delay = this.options.retryDelay * Math.pow(2, retries); // 指数退避
                console.warn(`Retrying ${operation} (${retries + 1}/${this.options.maxRetries}) after ${delay}ms: ${error.message}`);
                await new Promise(resolve => setTimeout(resolve, delay));
                return this.executeWithRetry(operation, func, retries + 1);
            }
            
            throw error;
        }
    }

    /**
     * 检查错误是否可重试
     * @param {Error} error - 错误对象
     * @returns {boolean} 是否可重试
     */
    isRetryableError(error) {
        const retryableStatusCodes = [408, 429, 500, 502, 503, 504];
        const retryableErrorNames = ['AbortError', 'TypeError'];
        
        if (retryableErrorNames.includes(error.name)) {
            return true;
        }
        
        if (error.message && retryableStatusCodes.some(code => 
            error.message.includes(`status: ${code}`)
        )) {
            return true;
        }
        
        return false;
    }

    /**
     * 缓存数据
     * @param {string} key - 缓存键
     * @param {any} data - 缓存数据
     * @param {number} [ttl=null] - 过期时间（毫秒）
     */
    cacheData(key, data, ttl = null) {
        if (!this.cache) return;

        // 确保缓存不超过最大大小
        if (this.cache.size >= this.options.maxCacheSize) {
            // LRU 缓存策略 - 删除最旧的缓存项
            const oldestKey = this.cache.keys().next().value;
            this.cache.delete(oldestKey);
        }

        const expires = Date.now() + (ttl || this.options.cacheTTL);
        this.cache.set(key, { data, expires });
    }

    /**
     * 获取缓存数据
     * @param {string} key - 缓存键
     * @returns {any|null} 缓存数据或null
     */
    getCachedData(key) {
        if (!this.cache || !this.cache.has(key)) return null;

        const cacheEntry = this.cache.get(key);
        if (Date.now() > cacheEntry.expires) {
            this.cache.delete(key);
            return null;
        }

        return cacheEntry.data;
    }

    /**
     * 清除缓存
     * @param {string} [key] - 特定键，不提供则清除所有缓存
     */
    clearCache(key) {
        if (!this.cache) return;

        if (key) {
            this.cache.delete(key);
        } else {
            this.cache.clear();
        }
    }

    // ==================== 基本KV操作 ====================

    /**
     * 保存数据
     * @param {string} key - 键
     * @param {string} value - 值
     * @param {Object} [options={}] - 选项
     * @returns {Promise<void>}
     */
    async put(key, value, options = {}) {
        return this.executeWithRetry('put', async () => {
            // 参数验证
            if (!key || typeof key !== 'string') {
                throw new Error('Invalid key');
            }

            const result = await this.kv.put(key, value, options);
            
            // 更新缓存
            this.clearCache(`kv:${key}`);
            this.clearCache(`kvWithMetadata:${key}`);
            
            return result;
        });
    }

    /**
     * 获取数据
     * @param {string} key - 键
     * @param {Object} [options={}] - 选项
     * @param {boolean} [options.bypassCache=false] - 是否绕过缓存
     * @returns {Promise<string|null>} 值或null
     */
    async get(key, options = {}) {
        return this.executeWithRetry('get', async () => {
            // 参数验证
            if (!key || typeof key !== 'string') {
                throw new Error('Invalid key');
            }

            const { bypassCache = false } = options;
            
            // 检查缓存
            const cacheKey = `kv:${key}`;
            if (!bypassCache) {
                const cached = this.getCachedData(cacheKey);
                if (cached !== null) return cached;
            }

            const result = await this.kv.get(key, options);
            
            // 缓存结果
            if (result !== null) {
                this.cacheData(cacheKey, result);
            }
            
            return result;
        });
    }

    /**
     * 获取数据和元数据
     * @param {string} key - 键
     * @param {Object} [options={}] - 选项
     * @param {boolean} [options.bypassCache=false] - 是否绕过缓存
     * @returns {Promise<Object|null>} 值和元数据或null
     */
    async getWithMetadata(key, options = {}) {
        return this.executeWithRetry('getWithMetadata', async () => {
            // 参数验证
            if (!key || typeof key !== 'string') {
                throw new Error('Invalid key');
            }

            const { bypassCache = false } = options;
            
            // 检查缓存
            const cacheKey = `kvWithMetadata:${key}`;
            if (!bypassCache) {
                const cached = this.getCachedData(cacheKey);
                if (cached !== null) return cached;
            }

            const result = await this.kv.getWithMetadata(key, options);
            
            // 缓存结果
            if (result !== null) {
                this.cacheData(cacheKey, result);
            }
            
            return result;
        });
    }

    /**
     * 删除数据
     * @param {string} key - 键
     * @param {Object} [options={}] - 选项
     * @returns {Promise<void>}
     */
    async delete(key, options = {}) {
        return this.executeWithRetry('delete', async () => {
            // 参数验证
            if (!key || typeof key !== 'string') {
                throw new Error('Invalid key');
            }

            const result = await this.kv.delete(key, options);
            
            // 清除缓存
            this.clearCache(`kv:${key}`);
            this.clearCache(`kvWithMetadata:${key}`);
            
            return result;
        });
    }

    /**
     * 列出数据
     * @param {Object} [options={}] - 选项
     * @returns {Promise<Object>} 列表结果
     */
    async list(options = {}) {
        return this.executeWithRetry('list', async () => {
            return await this.kv.list(options);
        });
    }

    // ==================== 文件操作 ====================

    /**
     * 保存文件记录
     * @param {string} fileId - 文件ID
     * @param {string} value - 文件值
     * @param {Object} [options={}] - 选项
     * @returns {Promise<void>}
     */
    async putFile(fileId, value, options = {}) {
        return this.put(fileId, value, options);
    }

    /**
     * 获取文件记录
     * @param {string} fileId - 文件ID
     * @param {Object} [options={}] - 选项
     * @returns {Promise<Object|null>} 文件记录或null
     */
    async getFile(fileId, options = {}) {
        return this.getWithMetadata(fileId, options);
    }

    /**
     * 获取文件记录包含元数据
     * @param {string} fileId - 文件ID
     * @param {Object} [options={}] - 选项
     * @returns {Promise<Object|null>} 文件记录或null
     */
    async getFileWithMetadata(fileId, options = {}) {
        return this.getWithMetadata(fileId, options);
    }

    /**
     * 删除文件记录
     * @param {string} fileId - 文件ID
     * @param {Object} [options={}] - 选项
     * @returns {Promise<void>}
     */
    async deleteFile(fileId, options = {}) {
        return this.delete(fileId, options);
    }

    /**
     * 列出文件
     * @param {Object} [options={}] - 选项
     * @returns {Promise<Object>} 文件列表
     */
    async listFiles(options = {}) {
        return this.list(options);
    }

    // ==================== 设置操作 ====================

    /**
     * 保存设置
     * @param {string} key - 设置键
     * @param {string} value - 设置值
     * @param {Object} [options={}] - 选项
     * @returns {Promise<void>}
     */
    async putSetting(key, value, options = {}) {
        return this.put(key, value, options);
    }

    /**
     * 获取设置
     * @param {string} key - 设置键
     * @param {Object} [options={}] - 选项
     * @returns {Promise<string|null>} 设置值或null
     */
    async getSetting(key, options = {}) {
        return this.get(key, options);
    }

    /**
     * 删除设置
     * @param {string} key - 设置键
     * @param {Object} [options={}] - 选项
     * @returns {Promise<void>}
     */
    async deleteSetting(key, options = {}) {
        return this.delete(key, options);
    }

    /**
     * 列出设置
     * @param {Object} [options={}] - 选项
     * @returns {Promise<Object>} 设置列表
     */
    async listSettings(options = {}) {
        return this.list(options);
    }

    // ==================== 索引操作 ====================

    /**
     * 保存索引操作记录
     * @param {string} operationId - 操作ID
     * @param {Object} operation - 操作对象
     * @param {Object} [options={}] - 选项
     * @returns {Promise<void>}
     */
    async putIndexOperation(operationId, operation, options = {}) {
        return this.executeWithRetry('putIndexOperation', async () => {
            // 参数验证
            if (!operationId || typeof operationId !== 'string') {
                throw new Error('Invalid operationId');
            }
            if (!operation || !operation.type || !operation.timestamp) {
                throw new Error('Invalid operation object');
            }

            const key = `manage@index@operation_${operationId}`;
            const result = await this.put(key, JSON.stringify(operation), options);
            
            // 更新缓存
            this.clearCache(`indexOperation:${operationId}`);
            
            return result;
        });
    }

    /**
     * 获取索引操作记录
     * @param {string} operationId - 操作ID
     * @param {Object} [options={}] - 选项
     * @param {boolean} [options.bypassCache=false] - 是否绕过缓存
     * @returns {Promise<Object|null>} 操作记录或null
     */
    async getIndexOperation(operationId, options = {}) {
        return this.executeWithRetry('getIndexOperation', async () => {
            // 参数验证
            if (!operationId || typeof operationId !== 'string') {
                throw new Error('Invalid operationId');
            }

            const { bypassCache = false } = options;
            
            // 检查缓存
            const cacheKey = `indexOperation:${operationId}`;
            if (!bypassCache) {
                const cached = this.getCachedData(cacheKey);
                if (cached !== null) return cached;
            }

            const key = `manage@index@operation_${operationId}`;
            const result = await this.get(key, options);
            
            if (!result) return null;
            
            const operation = JSON.parse(result);
            
            // 缓存结果
            this.cacheData(cacheKey, operation);
            
            return operation;
        });
    }

    /**
     * 删除索引操作记录
     * @param {string} operationId - 操作ID
     * @param {Object} [options={}] - 选项
     * @returns {Promise<void>}
     */
    async deleteIndexOperation(operationId, options = {}) {
        return this.executeWithRetry('deleteIndexOperation', async () => {
            // 参数验证
            if (!operationId || typeof operationId !== 'string') {
                throw new Error('Invalid operationId');
            }

            const key = `manage@index@operation_${operationId}`;
            const result = await this.delete(key, options);
            
            // 清除缓存
            this.clearCache(`indexOperation:${operationId}`);
            
            return result;
        });
    }

    /**
     * 列出索引操作记录
     * @param {Object} [options={}] - 选项
     * @param {number} [options.limit=1000] - 限制数量
     * @param {boolean} [options.processed] - 是否处理过
     * @param {string} [options.order='asc'] - 排序方式
     * @returns {Promise<Array>} 操作记录列表
     */
    async listIndexOperations(options = {}) {
        return this.executeWithRetry('listIndexOperations', async () => {
            options = {
                limit: 1000,
                processed: undefined,
                order: 'asc',
                ...options
            };
            
            const { limit, processed, order } = options;
            
            const listOptions = {
                prefix: 'manage@index@operation_',
                limit
            };
            
            const result = await this.list(listOptions);
            
            // 转换格式以匹配D1Database的返回格式
            const operations = [];
            for (const item of result.keys) {
                const operationId = item.name.replace('manage@index@operation_', '');
                const operation = await this.getIndexOperation(operationId);
                
                if (operation) {
                    operations.push({
                        id: operationId,
                        type: operation.type,
                        timestamp: operation.timestamp,
                        data: operation.data,
                        processed: operation.processed || false
                    });
                }
            }
            
            // 排序
            operations.sort((a, b) => {
                if (order.toLowerCase() === 'desc') {
                    return b.timestamp - a.timestamp;
                }
                return a.timestamp - b.timestamp;
            });
            
            return operations;
        });
    }

    /**
     * 标记索引操作已处理
     * @param {string} operationId - 操作ID
     * @param {boolean} [processed=true] - 是否处理
     * @returns {Promise<void>}
     */
    async markIndexOperationProcessed(operationId, processed = true) {
        return this.executeWithRetry('markIndexOperationProcessed', async () => {
            // 参数验证
            if (!operationId || typeof operationId !== 'string') {
                throw new Error('Invalid operationId');
            }

            const operation = await this.getIndexOperation(operationId);
            if (!operation) {
                throw new Error(`Operation ${operationId} not found`);
            }

            operation.processed = processed;
            await this.putIndexOperation(operationId, operation);
            
            return true;
        });
    }

    // ==================== 事务和批量操作 ====================

    /**
     * 执行事务
     * @param {Function} transactionFunction - 事务函数
     * @returns {Promise<any>} 事务结果
     */
    async transaction(transactionFunction) {
        return this.executeWithRetry('transaction', async () => {
            if (typeof transactionFunction !== 'function') {
                throw new Error('Transaction function is required');
            }

            // KV不支持事务，直接执行
            return transactionFunction(this);
        });
    }

    /**
     * 批量操作
     * @param {Array} operations - 操作数组
     * @returns {Promise<Object>} 操作结果
     */
    async batch(operations) {
        return this.executeWithRetry('batch', async () => {
            if (!Array.isArray(operations) || operations.length === 0) {
                throw new Error('Operations array is required');
            }

            // KV不支持真正的批量操作，顺序执行
            const results = [];
            
            for (const op of operations) {
                try {
                    switch (op.type) {
                        case 'put':
                            await this.put(op.key, op.value, op.options);
                            results.push({ success: true });
                            break;
                        case 'delete':
                            await this.delete(op.key, op.options);
                            results.push({ success: true });
                            break;
                        default:
                            results.push({ 
                                success: false, 
                                error: `Unsupported operation type: ${op.type}` 
                            });
                    }
                } catch (error) {
                    results.push({ 
                        success: false, 
                        error: error.message 
                    });
                }
            }

            return { results };
        });
    }
}

/**
 * 获取数据库实例的便捷函数
 * 这个函数可以在整个应用中使用，确保一致的数据库访问
 * @param {Object} env - 环境变量
 * @param {DatabaseAdapterOptions} [options={}] - 适配器选项
 * @returns {Object} 数据库实例
 */
export function getDatabase(env, options = {}) {
    const adapter = createDatabaseAdapter(env, options);
    if (!adapter) {
        throw new Error('Database not configured. Please configure D1 database (env.img_d1) or KV storage (env.img_url).');
    }
    return adapter;
}

/**
 * 检查数据库配置
 * @param {Object} env - 环境变量
 * @returns {DatabaseConfig} 配置信息
 */
export function checkDatabaseConfig(env) {
    const hasD1 = env.img_d1 && typeof env.img_d1.prepare === 'function';
    const hasKV = env.img_url && typeof env.img_url.get === 'function';

    return {
        hasD1: hasD1,
        hasKV: hasKV,
        usingD1: hasD1,
        usingKV: !hasD1 && hasKV,
        configured: hasD1 || hasKV,
        type: hasD1 ? 'd1' : hasKV ? 'kv' : 'none'
    };
}

/**
 * 清除数据库适配器缓存
 */
export function clearDatabaseAdapterCache() {
    adapterCache.clear();
    console.log('Database adapter cache cleared');
}
