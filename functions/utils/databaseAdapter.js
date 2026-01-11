/**
 * 数据库适配器
 * 提供统一的接口，可以在KV、D1和KVROCKS之间切换
 */

import { D1Database } from './d1Database.js';
import { createRedis } from 'redis-on-workers';

/**
 * 创建数据库适配器
 * @param {Object} env - 环境变量
 * @returns {Object} 数据库适配器实例
 */
export function createDatabaseAdapter(env) {
    // 检查是否配置了数据库
    if (env.img_kvrocks && typeof env.img_kvrocks === 'string') {
        // 使用KVROCKS存储
        return new KVROCKSAdapter(env.img_kvrocks);
    } else if (env.img_d1 && typeof env.img_d1.prepare === 'function') {
        // 使用D1数据库
        return new D1Database(env.img_d1);
    } else if (env.img_url && typeof env.img_url.get === 'function') {
        // 使用KV存储
        return new KVAdapter(env.img_url);
    } else {
        console.error('No database configured. Please configure either KV (env.img_url), D1 (env.img_d1) or KVROCKS (env.img_kvrocks).');
        // 返回一个模拟适配器避免崩溃
        return new MockAdapter();
    }
}

/**
 * KVROCKS适配器类
 * 连接外部KVROCKS存储
 */
class KVROCKSAdapter {
    constructor(connectionString) {
        this.connectionString = connectionString;
        this.redis = null;
        this.connected = false;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 5;
        this.reconnectDelay = 1000; // 1秒
        this.connectionLock = null; // 防止重复连接
    }

    /**
     * 建立连接（带重试机制）
     */
    async connect() {
        // 如果正在连接中，等待现有的连接
        if (this.connectionLock) {
            return this.connectionLock;
        }
        
        // 如果已经连接，测试连接是否仍然有效
        if (this.connected && this.redis) {
            try {
                // 测试现有连接
                await this.redis.send('PING');
                return this.redis;
            } catch (error) {
                console.warn('Existing connection failed, reconnecting...', error.message);
                this.connected = false;
                this.redis = null;
            }
        }

        // 创建新的连接锁
        this.connectionLock = this._connectWithRetry();
        try {
            const result = await this.connectionLock;
            return result;
        } finally {
            this.connectionLock = null;
        }
    }

    /**
     * 带重试的内部连接方法
     */
    async _connectWithRetry() {
        let lastError;
        
        while (this.reconnectAttempts < this.maxReconnectAttempts) {
            try {
                this.redis = createRedis(this.connectionString);
                // 测试连接
                await this.redis.send('PING');
                this.connected = true;
                this.reconnectAttempts = 0; // 重置重试计数器
                console.log('Connected to KVROCKS successfully');
                return this.redis;
            } catch (error) {
                lastError = error;
                this.reconnectAttempts++;
                console.error(`Failed to connect to KVROCKS (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts}):`, error.message);
                
                if (this.reconnectAttempts >= this.maxReconnectAttempts) {
                    break;
                }
                
                // 指数退避重试
                const delay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1);
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }
        
        throw new Error(`Failed to connect to KVROCKS after ${this.maxReconnectAttempts} attempts: ${lastError.message}`);
    }

    /**
     * 关闭连接
     */
    async disconnect() {
        if (this.connected && this.redis) {
            try {
                await this.redis.close();
                this.connected = false;
                console.log('Disconnected from KVROCKS');
            } catch (error) {
                console.error('Error disconnecting from KVROCKS:', error);
            }
        }
    }

    /**
     * 执行Redis事务
     * @param {Function} transaction - 事务函数，接收一个pushCommand函数用于添加命令
     * @returns {Array} 事务结果
     */
    async executeTransaction(transaction) {
        const redis = await this.connect();
        
        try {
            // 开始事务
            await redis.send('MULTI');
            
            // 收集事务中的所有命令
            const commands = [];
            
            // 创建一个函数来收集命令而不是立即执行
            const collectCommand = (...args) => {
                commands.push(args);
                // 在MULTI模式下，命令会排队等待EXEC
                return redis.send(...args);
            };
            
            // 执行事务函数，收集命令
            await transaction(collectCommand);
            
            // 执行事务并获取结果
            const results = await redis.send('EXEC');
            
            // 检查事务是否被中止
            if (results === null) {
                throw new Error('Transaction aborted - watched key was modified');
            }
            
            return results;
        } catch (error) {
            // 尝试回滚事务
            try {
                await redis.send('DISCARD');
            } catch (discardError) {
                console.warn('Failed to discard transaction:', discardError.message);
            }
            
            // 检查是否是连接错误
            if (error.message.includes('Connection') || error.message.includes('closed') || error.message.includes('disconnected')) {
                this.connected = false;
                console.warn('Connection lost during transaction, will reconnect on next operation');
            }
            
            throw error;
        }
    }

    /**
     * 保存键值对（使用事务确保原子性）
     */
    async put(key, value, options) {
        options = options || {};
        
        const maxRetries = 2;
        let lastError;
        
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                // 使用事务确保主键和元数据的一致性
                const results = await this.executeTransaction(async (send) => {
                    // 保存主值
                    await send('SET', key, value);
                    
                    // 处理元数据
                    if (options.metadata) {
                        const metadataKey = `${key}:metadata`;
                        await send('SET', metadataKey, JSON.stringify(options.metadata));
                    }
                    
                    // 处理过期时间
                    if (options.expiration || options.expirationTtl) {
                        const ttl = options.expirationTtl || Math.max(0, Math.floor((options.expiration - Date.now()) / 1000));
                        if (ttl > 0) {
                            await send('EXPIRE', key, ttl);
                            if (options.metadata) {
                                const metadataKey = `${key}:metadata`;
                                await send('EXPIRE', metadataKey, ttl);
                            }
                        }
                    }
                });
                
                // 检查结果，每个命令应该返回 'OK' 或类似
                for (let i = 0; i < results.length; i++) {
                    if (results[i] === null || results[i] === false) {
                        console.warn(`Transaction command ${i} returned unexpected result:`, results[i]);
                    }
                }
                
                return { success: true };
            } catch (error) {
                lastError = error;
                console.error(`KVROCKS put error (attempt ${attempt}/${maxRetries}):`, error.message);
                
                if (attempt < maxRetries) {
                    // 等待短暂时间后重试
                    await new Promise(resolve => setTimeout(resolve, 100 * attempt));
                }
            }
        }
        
        throw lastError;
    }

    /**
     * 获取值
     */
    async get(key, options) {
        const maxRetries = 2;
        let lastError;
        
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                const redis = await this.connect();
                const value = await redis.send('GET', key);
                return value === null ? null : value;
            } catch (error) {
                lastError = error;
                console.error(`KVROCKS get error (attempt ${attempt}/${maxRetries}):`, error.message);
                
                if (attempt < maxRetries) {
                    // 等待短暂时间后重试
                    await new Promise(resolve => setTimeout(resolve, 100 * attempt));
                }
            }
        }
        
        throw lastError;
    }

    /**
     * 获取值和元数据
     */
    async getWithMetadata(key, options) {
        const maxRetries = 2;
        let lastError;
        
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                const redis = await this.connect();
                
                // 使用管道同时获取主值和元数据，提高性能
                const [value, metadataJson] = await Promise.all([
                    redis.send('GET', key),
                    redis.send('GET', `${key}:metadata`)
                ]);
                
                if (value === null) {
                    return null;
                }
                
                return {
                    value: value,
                    metadata: metadataJson ? JSON.parse(metadataJson) : {}
                };
            } catch (error) {
                lastError = error;
                console.error(`KVROCKS getWithMetadata error (attempt ${attempt}/${maxRetries}):`, error.message);
                
                if (attempt < maxRetries) {
                    // 等待短暂时间后重试
                    await new Promise(resolve => setTimeout(resolve, 100 * attempt));
                }
            }
        }
        
        throw lastError;
    }

    /**
     * 删除键
     */
    async delete(key, options) {
        const maxRetries = 2;
        let lastError;
        
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                await this.executeTransaction(async (send) => {
                    await send('DEL', key);
                    await send('DEL', `${key}:metadata`);
                });
                
                return { success: true };
            } catch (error) {
                lastError = error;
                console.error(`KVROCKS delete error (attempt ${attempt}/${maxRetries}):`, error.message);
                
                if (attempt < maxRetries) {
                    // 等待短暂时间后重试
                    await new Promise(resolve => setTimeout(resolve, 100 * attempt));
                }
            }
        }
        
        throw lastError;
    }

    /**
     * 列出键
     */
    async list(options) {
        options = options || {};
        const prefix = options.prefix || '';
        const limit = options.limit || 1000;
        const cursor = options.cursor || '0';
        
        const maxRetries = 2;
        let lastError;
        
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                const redis = await this.connect();
                const pattern = prefix ? `${prefix}*` : '*';
                const [newCursor, keys] = await redis.send('SCAN', cursor, 'MATCH', pattern, 'COUNT', limit);
                
                // 过滤掉元数据键
                const dataKeys = keys.filter(key => !key.endsWith(':metadata'));
                
                // 并行获取所有元数据，提高性能
                const metadataPromises = dataKeys.map(async key => {
                    try {
                        const metadataJson = await redis.send('GET', `${key}:metadata`);
                        return {
                            name: key,
                            metadata: metadataJson ? JSON.parse(metadataJson) : {}
                        };
                    } catch (metadataError) {
                        console.warn(`Failed to get metadata for key ${key}:`, metadataError.message);
                        return {
                            name: key,
                            metadata: {}
                        };
                    }
                });
                
                const keysWithMetadata = await Promise.all(metadataPromises);
                
                return {
                    keys: keysWithMetadata,
                    cursor: newCursor !== '0' ? newCursor : null,
                    list_complete: newCursor === '0'
                };
            } catch (error) {
                lastError = error;
                console.error(`KVROCKS list error (attempt ${attempt}/${maxRetries}):`, error.message);
                
                if (attempt < maxRetries) {
                    // 等待短暂时间后重试
                    await new Promise(resolve => setTimeout(resolve, 100 * attempt));
                }
            }
        }
        
        throw lastError;
    }

    // 为了兼容性，添加一些别名方法
    async putFile(fileId, value, options) {
        return await this.put(fileId, value, options);
    }

    async getFile(fileId, options) {
        const result = await this.getWithMetadata(fileId, options);
        return result;
    }

    async getFileWithMetadata(fileId, options) {
        return await this.getWithMetadata(fileId, options);
    }

    async deleteFile(fileId, options) {
        return await this.delete(fileId, options);
    }

    async listFiles(options) {
        return await this.list(options);
    }

    async putSetting(key, value, options) {
        return await this.put(key, value, options);
    }

    async getSetting(key, options) {
        return await this.get(key, options);
    }

    async deleteSetting(key, options) {
        return await this.delete(key, options);
    }

    async listSettings(options) {
        return await this.list(options);
    }

    async putIndexOperation(operationId, operation, options) {
        const key = 'manage@index@operation_' + operationId;
        return await this.put(key, JSON.stringify(operation), options);
    }

    async getIndexOperation(operationId, options) {
        const key = 'manage@index@operation_' + operationId;
        const result = await this.get(key, options);
        return result ? JSON.parse(result) : null;
    }

    async deleteIndexOperation(operationId, options) {
        const key = 'manage@index@operation_' + operationId;
        return await this.delete(key, options);
    }

    async listIndexOperations(options) {
        const listOptions = Object.assign({}, options, {
            prefix: 'manage@index@operation_'
        });
        const result = await this.list(listOptions);
        
        // 转换格式以匹配D1Database的返回格式
        const operations = [];
        for (const item of result.keys) {
            try {
                const operationData = await this.get(item.name);
                if (operationData) {
                    const operation = JSON.parse(operationData);
                    operations.push({
                        id: item.name.replace('manage@index@operation_', ''),
                        type: operation.type,
                        timestamp: operation.timestamp,
                        data: operation.data,
                        processed: false // KVROCKS中没有这个字段，默认为false
                    });
                }
            } catch (error) {
                console.warn(`Failed to process operation ${item.name}:`, error.message);
            }
        }
        
        return operations;
    }
}

/**
 * KV适配器类
 * 保持与原有KV接口的兼容性
 */
class KVAdapter {
    constructor(kv) {
        this.kv = kv;
    }

    // 直接代理到KV的方法
    async put(key, value, options) {
        options = options || {};
        return await this.kv.put(key, value, options);
    }

    async get(key, options) {
        options = options || {};
        return await this.kv.get(key, options);
    }

    async getWithMetadata(key, options) {
        options = options || {};
        return await this.kv.getWithMetadata(key, options);
    }

    async delete(key, options) {
        options = options || {};
        return await this.kv.delete(key, options);
    }

    async list(options) {
        options = options || {};
        return await this.kv.list(options);
    }

    // 为了兼容性，添加一些别名方法
    async putFile(fileId, value, options) {
        return await this.put(fileId, value, options);
    }

    async getFile(fileId, options) {
        const result = await this.getWithMetadata(fileId, options);
        return result;
    }

    async getFileWithMetadata(fileId, options) {
        return await this.getWithMetadata(fileId, options);
    }

    async deleteFile(fileId, options) {
        return await this.delete(fileId, options);
    }

    async listFiles(options) {
        return await this.list(options);
    }

    async putSetting(key, value, options) {
        return await this.put(key, value, options);
    }

    async getSetting(key, options) {
        return await this.get(key, options);
    }

    async deleteSetting(key, options) {
        return await this.delete(key, options);
    }

    async listSettings(options) {
        return await this.list(options);
    }

    async putIndexOperation(operationId, operation, options) {
        const key = 'manage@index@operation_' + operationId;
        return await this.put(key, JSON.stringify(operation), options);
    }

    async getIndexOperation(operationId, options) {
        const key = 'manage@index@operation_' + operationId;
        const result = await this.get(key, options);
        return result ? JSON.parse(result) : null;
    }

    async deleteIndexOperation(operationId, options) {
        const key = 'manage@index@operation_' + operationId;
        return await this.delete(key, options);
    }

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
                const operation = JSON.parse(operationData);
                operations.push({
                    id: item.name.replace('manage@index@operation_', ''),
                    type: operation.type,
                    timestamp: operation.timestamp,
                    data: operation.data,
                    processed: false // KV中没有这个字段，默认为false
                });
            }
        }
        
        return operations;
    }
}

/**
 * 模拟适配器 - 当没有数据库配置时使用，避免应用崩溃
 */
class MockAdapter {
    constructor() {
        console.warn('Using mock database adapter - no actual storage will be used');
        this.storage = new Map();
    }

    async put(key, value, options) {
        console.log(`Mock put: ${key}`);
        this.storage.set(key, { value, metadata: options?.metadata || {} });
        return { success: true };
    }

    async get(key, options) {
        console.log(`Mock get: ${key}`);
        const item = this.storage.get(key);
        return item ? item.value : null;
    }

    async getWithMetadata(key, options) {
        console.log(`Mock getWithMetadata: ${key}`);
        const item = this.storage.get(key);
        return item || null;
    }

    async delete(key, options) {
        console.log(`Mock delete: ${key}`);
        this.storage.delete(key);
        return { success: true };
    }

    async list(options) {
        console.log(`Mock list with prefix: ${options?.prefix || ''}`);
        const prefix = options?.prefix || '';
        const keys = Array.from(this.storage.keys())
            .filter(key => key.startsWith(prefix))
            .map(key => ({
                name: key,
                metadata: this.storage.get(key).metadata
            }));
        
        return {
            keys,
            cursor: null,
            list_complete: true
        };
    }

    // 兼容性方法
    async putFile(fileId, value, options) { return this.put(fileId, value, options); }
    async getFile(fileId, options) { return this.getWithMetadata(fileId, options); }
    async getFileWithMetadata(fileId, options) { return this.getWithMetadata(fileId, options); }
    async deleteFile(fileId, options) { return this.delete(fileId, options); }
    async listFiles(options) { return this.list(options); }
    async putSetting(key, value, options) { return this.put(key, value, options); }
    async getSetting(key, options) { return this.get(key, options); }
    async deleteSetting(key, options) { return this.delete(key, options); }
    async listSettings(options) { return this.list(options); }
    async putIndexOperation(operationId, operation, options) {
        const key = 'manage@index@operation_' + operationId;
        return this.put(key, JSON.stringify(operation), options);
    }
    async getIndexOperation(operationId, options) {
        const key = 'manage@index@operation_' + operationId;
        const result = await this.get(key, options);
        return result ? JSON.parse(result) : null;
    }
    async deleteIndexOperation(operationId, options) {
        const key = 'manage@index@operation_' + operationId;
        return this.delete(key, options);
    }
    async listIndexOperations(options) {
        const listOptions = Object.assign({}, options, {
            prefix: 'manage@index@operation_'
        });
        const result = await this.list(listOptions);
        
        const operations = [];
        for (const item of result.keys) {
            const operationData = await this.get(item.name);
            if (operationData) {
                const operation = JSON.parse(operationData);
                operations.push({
                    id: item.name.replace('manage@index@operation_', ''),
                    type: operation.type,
                    timestamp: operation.timestamp,
                    data: operation.data,
                    processed: false
                });
            }
        }
        
        return operations;
    }
}

/**
 * 获取数据库实例的便捷函数
 * 这个函数可以在整个应用中使用，确保一致的数据库访问
 * @param {Object} env - 环境变量
 * @returns {Object} 数据库实例
 */
export function getDatabase(env) {
    const adapter = createDatabaseAdapter(env);
    if (!adapter) {
        throw new Error('Database not configured. Please configure D1 database (env.img_d1), KV storage (env.img_url) or KVROCKS (env.img_kvrocks).');
    }
    return adapter;
}

/**
 * 检查数据库配置
 * @param {Object} env - 环境变量
 * @returns {Object} 配置信息
 */
export function checkDatabaseConfig(env) {
    const hasKVROCKS = env.img_kvrocks && typeof env.img_kvrocks === 'string';
    const hasD1 = env.img_d1 && typeof env.img_d1.prepare === 'function';
    const hasKV = env.img_url && typeof env.img_url.get === 'function';

    return {
        hasKVROCKS: hasKVROCKS,
        hasD1: hasD1,
        hasKV: hasKV,
        usingKVROCKS: hasKVROCKS,
        usingD1: !hasKVROCKS && hasD1,
        usingKV: !hasKVROCKS && !hasD1 && hasKV,
        configured: hasKVROCKS || hasD1 || hasKV
    };
}
