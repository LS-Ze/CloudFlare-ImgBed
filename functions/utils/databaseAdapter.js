/**
 * 数据库适配器
 * 提供统一的接口，可以在KV和D1之间切换
 */

import { D1Database } from './d1Database.js';

// 统一错误类
export class DatabaseAdapterError extends Error {
    constructor(message, code = 'DATABASE_ADAPTER_ERROR', status = 500, details = {}) {
        super(message);
        this.name = 'DatabaseAdapterError';
        this.code = code;
        this.status = status;
        this.details = details;
        this.timestamp = Date.now();
    }
}

// 缓存配置
const CACHE_CONFIG = {
    ADAPTER_INSTANCE: { ttl: 300, maxSize: 10 }, // 5分钟缓存，最多10个适配器实例
    DATABASE_CONFIG: { ttl: 300, maxSize: 10 } // 5分钟缓存，最多10个配置
};

// 内存缓存
const memoryCache = new Map();

/**
 * 创建数据库适配器
 * @param {Object} env - 环境变量
 * @returns {Object} 数据库适配器实例
 */
export function createDatabaseAdapter(env) {
    try {
        // 尝试从缓存获取
        const cacheKey = JSON.stringify(env);
        const cachedAdapter = cacheGet('ADAPTER_INSTANCE', cacheKey);
        if (cachedAdapter) {
            return cachedAdapter;
        }

        // 检查是否配置了数据库
        if (env.img_url && typeof env.img_url.get === 'function') {
            // 使用KV存储
            const adapter = new KVAdapter(env.img_url);
            cacheSet('ADAPTER_INSTANCE', cacheKey, adapter);
            return adapter;
        } else if (env.img_d1 && typeof env.img_d1.prepare === 'function') {
            // 使用D1数据库
            const adapter = new D1Database(env.img_d1);
            cacheSet('ADAPTER_INSTANCE', cacheKey, adapter);
            return adapter;
        } else {
            console.error('No database configured. Please configure either KV (env.img_url) or D1 (env.img_d1).');
            return null;
        }
    } catch (error) {
        console.error('Failed to create database adapter:', error);
        return null;
    }
}

// 错误处理装饰器
function handleAdapterError(target, propertyKey, descriptor) {
    const originalMethod = descriptor.value;
    
    descriptor.value = async function(...args) {
        try {
            return await originalMethod.apply(this, args);
        } catch (error) {
            const errorDetails = {
                method: propertyKey,
                args: args,
                error: error.message,
                stack: error.stack,
                adapterType: this.constructor.name
            };
            
            console.error(`Database adapter error in ${propertyKey}:`, errorDetails);
            
            throw new DatabaseAdapterError(
                `Database adapter operation failed: ${error.message}`,
                'ADAPTER_OPERATION_FAILED',
                500,
                errorDetails
            );
        }
    };
    
    return descriptor;
}

/**
 * KV适配器类
 * 保持与原有KV接口的兼容性
 */
class KVAdapter {
    constructor(kv) {
        this.kv = kv;
        this.type = 'kv';
    }

    // 直接代理到KV的方法
    @handleAdapterError
    async put(key, value, options) {
        options = options || {};
        return await this.kv.put(key, value, options);
    }

    @handleAdapterError
    async get(key, options) {
        options = options || {};
        return await this.kv.get(key, options);
    }

    @handleAdapterError
    async getWithMetadata(key, options) {
        options = options || {};
        return await this.kv.getWithMetadata(key, options);
    }

    @handleAdapterError
    async delete(key, options) {
        options = options || {};
        return await this.kv.delete(key, options);
    }

    @handleAdapterError
    async list(options) {
        options = options || {};
        return await this.kv.list(options);
    }

    // 为了兼容性，添加一些别名方法
    @handleAdapterError
    async putFile(fileId, value, options) {
        return await this.put(fileId, value, options);
    }

    @handleAdapterError
    async getFile(fileId, options) {
        const result = await this.getWithMetadata(fileId, options);
        return result;
    }

    @handleAdapterError
    async getFileWithMetadata(fileId, options) {
        return await this.getWithMetadata(fileId, options);
    }

    @handleAdapterError
    async deleteFile(fileId, options) {
        return await this.delete(fileId, options);
    }

    @handleAdapterError
    async listFiles(options) {
        return await this.list(options);
    }

    @handleAdapterError
    async putSetting(key, value, options) {
        return await this.put(key, value, options);
    }

    @handleAdapterError
    async getSetting(key, options) {
        return await this.get(key, options);
    }

    @handleAdapterError
    async deleteSetting(key, options) {
        return await this.delete(key, options);
    }

    @handleAdapterError
    async listSettings(options) {
        return await this.list(options);
    }

    @handleAdapterError
    async putIndexOperation(operationId, operation, options) {
        const key = `manage@index@operation_${operationId}`;
        return await this.put(key, JSON.stringify(operation), options);
    }

    @handleAdapterError
    async getIndexOperation(operationId, options) {
        const key = `manage@index@operation_${operationId}`;
        const result = await this.get(key, options);
        return result ? JSON.parse(result) : null;
    }

    @handleAdapterError
    async deleteIndexOperation(operationId, options) {
        const key = `manage@index@operation_${operationId}`;
        return await this.delete(key, options);
    }

    @handleAdapterError
    async listIndexOperations(options) {
        const listOptions = Object.assign({}, options, {
            prefix: 'manage@index@operation_'
        });
        const result = await this.list(listOptions);
        
        // 转换格式以匹配D1Database的返回格式
        const operations = [];
        for (const item of result.keys) {
            const operationData = await this.get(item.name);
            if (operationData) {
                try {
                    const operation = JSON.parse(operationData);
                    operations.push({
                        id: item.name.replace('manage@index@operation_', ''),
                        type: operation.type,
                        timestamp: operation.timestamp,
                        data: operation.data,
                        processed: false // KV中没有这个字段，默认为false
                    });
                } catch (error) {
                    console.warn(`Failed to parse index operation ${item.name}:`, error);
                }
            }
        }
        
        return operations;
    }

    /**
     * 标记索引操作已处理
     */
    @handleAdapterError
    async markIndexOperationProcessed(operationId, options) {
        const key = `manage@index@operation_${operationId}`;
        const operationData = await this.get(key, options);
        
        if (operationData) {
            try {
                const operation = JSON.parse(operationData);
                operation.processed = true;
                return await this.put(key, JSON.stringify(operation), options);
            } catch (error) {
                console.error(`Failed to update index operation ${operationId}:`, error);
                throw error;
            }
        }
        
        return null;
    }

    /**
     * 执行事务
     */
    @handleAdapterError
    async transaction(operations) {
        if (!Array.isArray(operations) || operations.length === 0) {
            throw new DatabaseAdapterError('Invalid operations array', 'INVALID_OPERATIONS', 400);
        }
        
        const results = [];
        
        for (const op of operations) {
            switch (op.type) {
                case 'put':
                    results.push(await this.put(op.key, op.value, op.options));
                    break;
                case 'delete':
                    results.push(await this.delete(op.key, op.options));
                    break;
                default:
                    throw new DatabaseAdapterError(`Unsupported operation type: ${op.type}`, 'UNSUPPORTED_OPERATION', 400);
            }
        }
        
        return results;
    }

    /**
     * 批量操作
     */
    @handleAdapterError
    async batch(operations) {
        return this.transaction(operations);
    }

    /**
     * 清除缓存
     */
    @handleAdapterError
    async clearCache() {
        // KV本身没有缓存，所以这个方法是一个空实现
        return true;
    }
}

/**
 * 获取数据库实例的便捷函数
 * 这个函数可以在整个应用中使用，确保一致的数据库访问
 * @param {Object} env - 环境变量
 * @returns {Object} 数据库实例
 */
export function getDatabase(env) {
    try {
        const adapter = createDatabaseAdapter(env);
        if (!adapter) {
            throw new DatabaseAdapterError(
                'Database not configured. Please configure D1 database (env.img_d1) or KV storage (env.img_url).',
                'DATABASE_NOT_CONFIGURED',
                500
            );
        }
        return adapter;
    } catch (error) {
        console.error('Failed to get database instance:', error);
        throw error;
    }
}

/**
 * 检查数据库配置
 * @param {Object} env - 环境变量
 * @returns {Object} 配置信息
 */
export function checkDatabaseConfig(env) {
    try {
        // 尝试从缓存获取
        const cacheKey = JSON.stringify(env);
        const cachedConfig = cacheGet('DATABASE_CONFIG', cacheKey);
        if (cachedConfig) {
            return cachedConfig;
        }

        const hasD1 = env.img_d1 && typeof env.img_d1.prepare === 'function';
        const hasKV = env.img_url && typeof env.img_url.get === 'function';

        const config = {
            hasD1: hasD1,
            hasKV: hasKV,
            usingD1: hasD1,
            usingKV: !hasD1 && hasKV,
            configured: hasD1 || hasKV,
            adapterType: hasD1 ? 'd1' : (hasKV ? 'kv' : 'none')
        };

        cacheSet('DATABASE_CONFIG', cacheKey, config);
        return config;
    } catch (error) {
        console.error('Failed to check database config:', error);
        return {
            hasD1: false,
            hasKV: false,
            usingD1: false,
            usingKV: false,
            configured: false,
            adapterType: 'none'
        };
    }
}

/**
 * 清除数据库适配器缓存
 */
export function clearDatabaseAdapterCache() {
    memoryCache.clear();
    console.log('Database adapter cache cleared');
}

/**
 * 缓存工具函数
 */
function cacheGet(type, key) {
    const cacheKey = `${type}_${key}`;
    const entry = memoryCache.get(cacheKey);
    
    if (entry && Date.now() - entry.timestamp < CACHE_CONFIG[type].ttl * 1000) {
        return entry.value;
    }
    
    if (entry) {
        memoryCache.delete(cacheKey);
    }
    
    return null;
}

function cacheSet(type, key, value) {
    const cacheKey = `${type}_${key}`;
    
    // 限制缓存大小
    const cacheTypeKeys = Array.from(memoryCache.keys()).filter(k => k.startsWith(`${type}_`));
    if (cacheTypeKeys.length >= CACHE_CONFIG[type].maxSize) {
        // 删除最旧的缓存项
        const oldestKey = cacheTypeKeys.sort((a, b) => 
            memoryCache.get(a).timestamp - memoryCache.get(b).timestamp
        )[0];
        memoryCache.delete(oldestKey);
    }
    
    memoryCache.set(cacheKey, {
        value,
        timestamp: Date.now()
    });
}

function cacheDelete(type, key) {
    const cacheKey = `${type}_${key}`;
    memoryCache.delete(cacheKey);
}
