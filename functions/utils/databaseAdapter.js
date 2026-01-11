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
            const pushCommand = (...args) => {
                commands.push(args);
                // 在MULTI模式下，我们只将命令排队，不立即执行
                // redis.send 在 MULTI 模式下会自动将命令排队
                return redis.send(...args);
            };
            
            // 执行事务函数，收集命令
            await transaction(pushCommand);
            
            // 执行事务并获取结果
            const results = await redis.send('EXEC');
            
            // 检查事务是否被中止
            if (results === null) {
                throw new Error('Transaction aborted - watched key was modified');
            }
            
            // 验证所有命令都成功执行
            for (let i = 0; i < results.length; i++) {
                const result = results[i];
                if (result instanceof Error) {
                    console.error(`Transaction command ${i} failed:`, result.message);
                    throw new Error(`Transaction command ${i} failed: ${result.message}`);
                }
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
                const results = await this.executeTransaction(async (pushCommand) => {
                    // 保存主值
                    pushCommand('SET', key, value);
                    
                    // 处理元数据
                    if (options.metadata) {
                        const metadataKey = `${key}:metadata`;
                        pushCommand('SET', metadataKey, JSON.stringify(options.metadata));
                    }
                    
                    // 处理过期时间
                    if (options.expiration || options.expirationTtl) {
                        const ttl = options.expirationTtl || Math.max(0, Math.floor((options.expiration - Date.now()) / 1000));
                        if (ttl > 0) {
                            pushCommand('EXPIRE', key, ttl);
                            if (options.metadata) {
                                const metadataKey = `${key}:metadata`;
                                pushCommand('EXPIRE', metadataKey, ttl);
                            }
                        }
                    }
                });
                
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
     * 不使用事务的简单保存方法（替代方案）
     */
    async putSimple(key, value, options) {
        options = options || {};
        
        const maxRetries = 2;
        let lastError;
        
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                const redis = await this.connect();
                
                // 保存主值
                await redis.send('SET', key, value);
                
                // 处理元数据
                if (options.metadata) {
                    const metadataKey = `${key}:metadata`;
                    await redis.send('SET', metadataKey, JSON.stringify(options.metadata));
                }
                
                // 处理过期时间
                if (options.expiration || options.expirationTtl) {
                    const ttl = options.expirationTtl || Math.max(0, Math.floor((options.expiration - Date.now()) / 1000));
                    if (ttl > 0) {
                        await redis.send('EXPIRE', key, ttl);
                        if (options.metadata) {
                            const metadataKey = `${key}:metadata`;
                            await redis.send('EXPIRE', metadataKey, ttl);
                        }
                    }
                }
                
                return { success: true };
            } catch (error) {
                lastError = error;
                console.error(`KVROCKS putSimple error (attempt ${attempt}/${maxRetries}):`, error.message);
                
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
                await this.executeTransaction(async (pushCommand) => {
                    pushCommand('DEL', key);
                    pushCommand('DEL', `${key}:metadata`);
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
     * 不使用事务的简单删除方法（替代方案）
     */
    async deleteSimple(key, options) {
        const maxRetries = 2;
        let lastError;
        
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                const redis = await this.connect();
                await Promise.all([
                    redis.send('DEL', key),
                    redis.send('DEL', `${key}:metadata`)
                ]);
                
                return { success: true };
            } catch (error) {
                lastError = error;
                console.error(`KVROCKS deleteSimple error (attempt ${attempt}/${maxRetries}):`, error.message);
                
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
        // 对于索引操作，我们不需要事务，使用简单方法
        return await this.putSimple(key, JSON.stringify(operation), options);
    }

    async getIndexOperation(operationId, options) {
        const key = 'manage@index@operation_' + operationId;
        const result = await this.get(key, options);
        return result ? JSON.parse(result) : null;
    }

    async deleteIndexOperation(operationId, options) {
        const key = 'manage@index@operation_' + operationId;
        return await this.deleteSimple(key, options);
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

// ... 其他类保持不变（KVAdapter, MockAdapter 等）
