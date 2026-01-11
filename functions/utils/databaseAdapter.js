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
        return null;
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
        this.connecting = false; // 连接中标志，防止并发连接
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 5;
        this.reconnectDelay = 1000; // 1秒
        this.lastPingTime = 0;
        this.pingInterval = 5000; // 5秒内只允许一次PING
        this.debugMode = false; // 关闭调试模式以提高性能
        
        // 连接状态缓存
        this.connectionCache = {
            lastCheck: 0,
            isAlive: false,
            cacheDuration: 1000 // 缓存1秒
        };
        
        // 事务状态
        this.inTransaction = false;
    }

    /**
     * 建立连接（带重试机制和并发控制）
     */
    async connect() {
        // 检查连接缓存
        const now = Date.now();
        if (now - this.connectionCache.lastCheck < this.connectionCache.cacheDuration && 
            this.connectionCache.isAlive) {
            return this.redis;
        }

        // 防止并发连接
        if (this.connecting) {
            if (this.debugMode) console.log('Already connecting, waiting...');
            // 等待连接完成
            while (this.connecting) {
                await new Promise(resolve => setTimeout(resolve, 10));
            }
            return this.redis;
        }

        if (this.connected && this.redis) {
            try {
                // 检查PING频率
                if (now - this.lastPingTime > this.pingInterval) {
                    if (this.debugMode) console.log('Testing existing connection with PING');
                    await this.redis.send('PING');
                    this.lastPingTime = now;
                    
                    // 更新连接缓存
                    this.connectionCache = {
                        lastCheck: now,
                        isAlive: true,
                        cacheDuration: this.connectionCache.cacheDuration
                    };
                    
                    if (this.debugMode) console.log('Existing connection is alive');
                }
                return this.redis;
            } catch (error) {
                console.warn('Existing connection failed, reconnecting...', error.message);
                this.connected = false;
                this.redis = null;
                this.connectionCache.isAlive = false;
            }
        }

        this.connecting = true;
        this.reconnectAttempts = 0;

        try {
            while (this.reconnectAttempts < this.maxReconnectAttempts) {
                try {
                    if (this.debugMode) console.log(`Attempting to connect to KVROCKS (attempt ${this.reconnectAttempts + 1})`);
                    this.redis = createRedis(this.connectionString);
                    
                    // 测试连接
                    if (this.debugMode) console.log('Testing new connection with PING');
                    await this.redis.send('PING');
                    this.connected = true;
                    this.lastPingTime = now;
                    this.inTransaction = false; // 重置事务状态
                    
                    // 更新连接缓存
                    this.connectionCache = {
                        lastCheck: now,
                        isAlive: true,
                        cacheDuration: this.connectionCache.cacheDuration
                    };
                    
                    console.log('Connected to KVROCKS successfully');
                    return this.redis;
                } catch (error) {
                    this.reconnectAttempts++;
                    console.error(`Failed to connect to KVROCKS (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts}):`, error.message);
                    
                    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
                        this.connectionCache.isAlive = false;
                        throw new Error(`Failed to connect to KVROCKS after ${this.maxReconnectAttempts} attempts: ${error.message}`);
                    }
                    
                    // 指数退避重试
                    const delay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1);
                    if (this.debugMode) console.log(`Waiting ${delay}ms before next reconnect attempt`);
                    await new Promise(resolve => setTimeout(resolve, delay));
                }
            }
        } finally {
            this.connecting = false;
        }
    }

    /**
     * 关闭连接
     */
    async disconnect() {
        if (this.debugMode) console.log('KVROCKSAdapter.disconnect() called');
        
        if (this.connected && this.redis) {
            try {
                // 如果在事务中，先回滚
                if (this.inTransaction) {
                    await this.redis.send('DISCARD');
                    this.inTransaction = false;
                }
                
                await this.redis.close();
                this.connected = false;
                this.connectionCache.isAlive = false;
                console.log('Disconnected from KVROCKS');
            } catch (error) {
                console.error('Error disconnecting from KVROCKS:', error);
            }
        }
    }

    /**
     * 执行Redis命令（内部使用，不对外暴露）
     */
    async _executeCommand(command, ...args) {
        const redis = await this.connect();
        try {
            if (this.debugMode) console.log(`Executing command: ${command} ${args.join(' ')}`);
            
            // 跟踪事务状态
            if (command.toUpperCase() === 'MULTI') {
                this.inTransaction = true;
            } else if (command.toUpperCase() === 'EXEC' || command.toUpperCase() === 'DISCARD') {
                this.inTransaction = false;
            }
            
            return await redis.send(command, ...args);
        } catch (error) {
            console.error(`Command execution error: ${command} ${args.join(' ')}`, error);
            this.connected = false;
            this.connectionCache.isAlive = false;
            this.inTransaction = false; // 重置事务状态
            throw error;
        }
    }

    /**
     * 开始事务
     */
    async _beginTransaction() {
        if (this.inTransaction) {
            throw new Error('Already in a transaction');
        }
        await this._executeCommand('MULTI');
    }

    /**
     * 提交事务
     */
    async _commitTransaction() {
        if (!this.inTransaction) {
            throw new Error('Not in a transaction');
        }
        return await this._executeCommand('EXEC');
    }

    /**
     * 回滚事务
     */
    async _rollbackTransaction() {
        if (!this.inTransaction) {
            throw new Error('Not in a transaction');
        }
        return await this._executeCommand('DISCARD');
    }

    /**
     * 保存键值对（使用事务确保原子性）
     */
    async put(key, value, options) {
        if (this.debugMode) console.log(`KVROCKSAdapter.put("${key}") called`);
        
        options = options || {};
        
        // 验证参数
        if (!key || key.trim() === '') {
            const error = new Error('Key is required');
            console.error(error.message);
            throw error;
        }
        if (value === undefined || value === null) {
            const error = new Error('Value is required');
            console.error(error.message);
            throw error;
        }
        
        try {
            // 开始事务
            await this._beginTransaction();
            
            // 保存主值
            await this._executeCommand('SET', key, value);
            
            // 处理元数据
            if (options.metadata) {
                const metadataKey = `${key}:metadata`;
                await this._executeCommand('SET', metadataKey, JSON.stringify(options.metadata));
            }
            
            // 处理过期时间
            if (options.expiration || options.expirationTtl) {
                const ttl = options.expirationTtl || Math.max(0, Math.floor((options.expiration - Date.now()) / 1000));
                if (ttl > 0) {
                    await this._executeCommand('EXPIRE', key, ttl);
                    if (options.metadata) {
                        const metadataKey = `${key}:metadata`;
                        await this._executeCommand('EXPIRE', metadataKey, ttl);
                    }
                }
            }
            
            // 提交事务
            const results = await this._commitTransaction();
            
            // 检查结果
            if (results === null) {
                throw new Error('Transaction aborted - watched key was modified');
            }
            
            if (results.length === 0) {
                throw new Error('No commands were executed in transaction');
            }
            
            // 检查每个命令的执行结果
            for (let i = 0; i < results.length; i++) {
                const result = results[i];
                if (result === null || result === false || 
                    (typeof result === 'string' && result.startsWith('ERR '))) {
                    throw new Error(`Transaction command failed at index ${i}: ${result}`);
                }
            }
            
            if (this.debugMode) console.log(`Successfully put key "${key}" in transaction`);
            return { success: true };
        } catch (error) {
            // 尝试回滚事务
            try {
                if (this.inTransaction) {
                    await this._rollbackTransaction();
                }
            } catch (rollbackError) {
                console.warn('Failed to rollback transaction:', rollbackError.message);
            }
            
            console.error(`KVROCKS put error for key "${key}":`, error);
            
            // 连接可能已断开，重置连接状态
            this.connected = false;
            this.connectionCache.isAlive = false;
            this.inTransaction = false;
            
            // 重试一次（防止无限递归）
            if (this.reconnectAttempts < 1) {
                try {
                    this.reconnectAttempts++;
                    if (this.debugMode) console.log(`Retrying put for key "${key}"`);
                    return await this.put(key, value, options);
                } catch (retryError) {
                    console.error(`KVROCKS put retry failed for key "${key}":`, retryError);
                    throw retryError;
                }
            } else {
                throw error;
            }
        }
    }

    /**
     * 获取值
     */
    async get(key, options) {
        if (this.debugMode) console.log(`KVROCKSAdapter.get("${key}") called`);
        
        if (!key || key.trim() === '') {
            const error = new Error('Key is required');
            console.error(error.message);
            throw error;
        }
        
        return await this._executeCommand('GET', key);
    }

    /**
     * 获取值和元数据
     */
    async getWithMetadata(key, options) {
        if (this.debugMode) console.log(`KVROCKSAdapter.getWithMetadata("${key}") called`);
        
        if (!key || key.trim() === '') {
            const error = new Error('Key is required');
            console.error(error.message);
            throw error;
        }
        
        try {
            // 开始事务
            await this._beginTransaction();
            
            // 获取主值和元数据
            await this._executeCommand('GET', key);
            await this._executeCommand('GET', `${key}:metadata`);
            
            // 提交事务
            const results = await this._commitTransaction();
            
            // 检查结果
            if (results === null) {
                throw new Error('Transaction aborted - watched key was modified');
            }
            
            const [value, metadataJson] = results;
            
            if (value === null) {
                if (this.debugMode) console.log(`Key "${key}" not found`);
                return null;
            }
            
            const result = {
                value: value,
                metadata: metadataJson ? JSON.parse(metadataJson) : {}
            };
            
            if (this.debugMode) console.log(`Got value and metadata for key "${key}" in transaction`);
            return result;
        } catch (error) {
            // 尝试回滚事务
            try {
                if (this.inTransaction) {
                    await this._rollbackTransaction();
                }
            } catch (rollbackError) {
                console.warn('Failed to rollback transaction:', rollbackError.message);
            }
            
            console.error(`KVROCKS getWithMetadata error for key "${key}":`, error);
            this.connected = false;
            this.connectionCache.isAlive = false;
            this.inTransaction = false;
            throw error;
        }
    }

    /**
     * 删除键（使用事务确保原子性）
     */
    async delete(key, options) {
        if (this.debugMode) console.log(`KVROCKSAdapter.delete("${key}") called`);
        
        if (!key || key.trim() === '') {
            const error = new Error('Key is required');
            console.error(error.message);
            throw error;
        }
        
        try {
            // 开始事务
            await this._beginTransaction();
            
            // 删除主值和元数据
            await this._executeCommand('DEL', key);
            await this._executeCommand('DEL', `${key}:metadata`);
            
            // 提交事务
            const results = await this._commitTransaction();
            
            // 检查结果
            if (results === null) {
                throw new Error('Transaction aborted - watched key was modified');
            }
            
            if (this.debugMode) console.log(`Successfully deleted key "${key}" in transaction`);
            return { success: true };
        } catch (error) {
            // 尝试回滚事务
            try {
                if (this.inTransaction) {
                    await this._rollbackTransaction();
                }
            } catch (rollbackError) {
                console.warn('Failed to rollback transaction:', rollbackError.message);
            }
            
            console.error(`KVROCKS delete error for key "${key}":`, error);
            this.connected = false;
            this.connectionCache.isAlive = false;
            this.inTransaction = false;
            
            // 重试一次（防止无限递归）
            if (this.reconnectAttempts < 1) {
                try {
                    this.reconnectAttempts++;
                    if (this.debugMode) console.log(`Retrying delete for key "${key}"`);
                    return await this.delete(key, options);
                } catch (retryError) {
                    console.error(`KVROCKS delete retry failed for key "${key}":`, retryError);
                    throw retryError;
                }
            } else {
                throw error;
            }
        }
    }

    /**
     * 列出键
     */
    async list(options) {
        if (this.debugMode) console.log('KVROCKSAdapter.list() called');
        
        options = options || {};
        const prefix = options.prefix || '';
        const limit = options.limit || 1000;
        const cursor = options.cursor || '0';
        
        try {
            const pattern = prefix ? `${prefix}*` : '*';
            if (this.debugMode) console.log(`Scanning keys with pattern "${pattern}", limit ${limit}`);
            
            const [newCursor, keys] = await this._executeCommand('SCAN', cursor, 'MATCH', pattern, 'COUNT', limit);
            
            if (this.debugMode) console.log(`Found ${keys.length} keys in scan`);
            
            // 过滤掉元数据键
            const dataKeys = keys.filter(key => !key.endsWith(':metadata'));
            
            if (this.debugMode) console.log(`Filtered to ${dataKeys.length} data keys (excluding metadata)`);
            
            // 获取元数据
            const keysWithMetadata = await Promise.all(dataKeys.map(async key => {
                try {
                    const metadataJson = await this._executeCommand('GET', `${key}:metadata`);
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
            }));
            
            if (this.debugMode) console.log(`Successfully retrieved metadata for ${keysWithMetadata.length} keys`);
            
            return {
                keys: keysWithMetadata,
                cursor: newCursor !== '0' ? newCursor : null,
                list_complete: newCursor === '0'
            };
        } catch (error) {
            console.error('KVROCKS list error:', error);
            this.connected = false;
            this.connectionCache.isAlive = false;
            throw error;
        }
    }

    // 为了兼容性，添加一些别名方法
    async putFile(fileId, value, options) {
        if (this.debugMode) console.log(`KVROCKSAdapter.putFile("${fileId}") called`);
        return await this.put(fileId, value, options);
    }

    async getFile(fileId, options) {
        if (this.debugMode) console.log(`KVROCKSAdapter.getFile("${fileId}") called`);
        const result = await this.getWithMetadata(fileId, options);
        return result;
    }

    async getFileWithMetadata(fileId, options) {
        if (this.debugMode) console.log(`KVROCKSAdapter.getFileWithMetadata("${fileId}") called`);
        return await this.getWithMetadata(fileId, options);
    }

    async deleteFile(fileId, options) {
        if (this.debugMode) console.log(`KVROCKSAdapter.deleteFile("${fileId}") called`);
        return await this.delete(fileId, options);
    }

    async listFiles(options) {
        if (this.debugMode) console.log('KVROCKSAdapter.listFiles() called');
        return await this.list(options);
    }

    async putSetting(key, value, options) {
        if (this.debugMode) console.log(`KVROCKSAdapter.putSetting("${key}") called`);
        return await this.put(key, value, options);
    }

    async getSetting(key, options) {
        if (this.debugMode) console.log(`KVROCKSAdapter.getSetting("${key}") called`);
        return await this.get(key, options);
    }

    async deleteSetting(key, options) {
        if (this.debugMode) console.log(`KVROCKSAdapter.deleteSetting("${key}") called`);
        return await this.delete(key, options);
    }

    async listSettings(options) {
        if (this.debugMode) console.log('KVROCKSAdapter.listSettings() called');
        return await this.list(options);
    }

    async putIndexOperation(operationId, operation, options) {
        const key = 'manage@index@operation_' + operationId;
        if (this.debugMode) console.log(`KVROCKSAdapter.putIndexOperation("${operationId}") called`);
        return await this.put(key, JSON.stringify(operation), options);
    }

    async getIndexOperation(operationId, options) {
        const key = 'manage@index@operation_' + operationId;
        if (this.debugMode) console.log(`KVROCKSAdapter.getIndexOperation("${operationId}") called`);
        const result = await this.get(key, options);
        return result ? JSON.parse(result) : null;
    }

    async deleteIndexOperation(operationId, options) {
        const key = 'manage@index@operation_' + operationId;
        if (this.debugMode) console.log(`KVROCKSAdapter.deleteIndexOperation("${operationId}") called`);
        return await this.delete(key, options);
    }

    async listIndexOperations(options) {
        if (this.debugMode) console.log('KVROCKSAdapter.listIndexOperations() called');
        
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
        
        if (this.debugMode) console.log(`Found ${operations.length} index operations`);
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
 * 获取数据库实例的便捷函数
 * 这个函数可以在整个应用中使用，确保一致的数据库访问
 * @param {Object} env - 环境变量
 * @returns {Object} 数据库实例
 */
export function getDatabase(env) {
    var adapter = createDatabaseAdapter(env);
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
    var hasKVROCKS = env.img_kvrocks && typeof env.img_kvrocks === 'string';
    var hasD1 = env.img_d1 && typeof env.img_d1.prepare === 'function';
    var hasKV = env.img_url && typeof env.img_url.get === 'function';

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
