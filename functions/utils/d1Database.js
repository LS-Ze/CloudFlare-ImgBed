/**
 * D1 数据库操作工具类
 * 提供与 KV 接口兼容的数据库操作，同时支持 D1 特有的功能
 */

/**
 * @typedef {Object} DatabaseOptions
 * @property {boolean} [enableCache=true] - 是否启用缓存
 * @property {number} [cacheTTL=300000] - 缓存有效期（毫秒）
 * @property {number} [maxCacheSize=1000] - 最大缓存大小
 * @property {boolean} [debug=false] - 是否启用调试模式
 */

/**
 * @typedef {Object} FileMetadata
 * @property {string} [FileName] - 文件名
 * @property {string} [FileType] - 文件类型
 * @property {number} [FileSize] - 文件大小
 * @property {string} [UploadIP] - 上传IP
 * @property {string} [UploadAddress] - 上传地址
 * @property {string} [ListType] - 列表类型
 * @property {number} [TimeStamp] - 时间戳
 * @property {string} [Label] - 标签
 * @property {string} [Directory] - 目录
 * @property {string} [Channel] - 渠道
 * @property {string} [ChannelName] - 渠道名称
 * @property {string} [TgFileId] - Telegram文件ID
 * @property {string} [TgChatId] - Telegram聊天ID
 * @property {string} [TgBotToken] - Telegram机器人令牌
 * @property {boolean} [IsChunked] - 是否分块
 */

/**
 * @typedef {Object} ListOptions
 * @property {string} [prefix] - 前缀
 * @property {number} [limit=1000] - 限制数量
 * @property {string} [cursor] - 游标
 * @property {string} [order='asc'] - 排序方式
 * @property {string} [sortBy='id'] - 排序字段
 */

class D1Database {
    /**
     * 创建 D1 数据库实例
     * @param {Object} db - D1 数据库实例
     * @param {DatabaseOptions} [options={}] - 数据库选项
     */
    constructor(db, options = {}) {
        if (!db) {
            throw new Error('D1 database instance is required');
        }

        this.db = db;
        this.options = {
            enableCache: true,
            cacheTTL: 300000, // 5分钟
            maxCacheSize: 1000,
            debug: false,
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
        this.putFile = this.putFile.bind(this);
        this.getFile = this.getFile.bind(this);
        this.deleteFile = this.deleteFile.bind(this);
        this.listFiles = this.listFiles.bind(this);
    }

    /**
     * 记录性能指标
     * @param {string} operation - 操作名称
     * @param {number} duration - 持续时间（毫秒）
     * @param {string} [query] - SQL查询
     */
    recordPerformance(operation, duration, query = '') {
        this.performance.queries.push({
            operation,
            duration,
            timestamp: Date.now(),
            query
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
     * @param {string} [query] - SQL查询
     */
    recordError(operation, error, query = '') {
        this.performance.errors.push({
            operation,
            message: error.message,
            stack: error.stack,
            timestamp: Date.now(),
            query
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
     * 执行SQL查询并处理结果
     * @param {string} operation - 操作名称
     * @param {Function} queryFunction - 查询函数
     * @returns {Promise<any>} 查询结果
     */
    async executeQuery(operation, queryFunction) {
        const startTime = performance.now();
        
        try {
            const result = await queryFunction();
            const duration = performance.now() - startTime;
            
            this.recordPerformance(operation, duration);
            
            if (this.options.debug) {
                console.log(`[D1Database] ${operation} completed in ${duration.toFixed(2)}ms`);
            }
            
            return result;
        } catch (error) {
            const duration = performance.now() - startTime;
            this.recordError(operation, error);
            
            if (this.options.debug) {
                console.error(`[D1Database] ${operation} failed in ${duration.toFixed(2)}ms:`, error);
            }
            
            throw error;
        }
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

    // ==================== 文件操作 ====================

    /**
     * 保存文件记录 (替代 KV.put)
     * @param {string} fileId - 文件ID
     * @param {string} value - 文件值
     * @param {Object} [options={}] - 选项
     * @param {FileMetadata} [options.metadata={}] - 文件元数据
     * @returns {Promise<Object>} 操作结果
     */
    putFile(fileId, value, options = {}) {
        return this.executeQuery('putFile', async () => {
            // 参数验证
            if (!fileId || typeof fileId !== 'string') {
                throw new Error('Invalid fileId');
            }

            value = value || '';
            options = options || {};
            const metadata = options.metadata || {};
            
            // 从metadata中提取字段用于索引
            const extractedFields = this.extractMetadataFields(metadata);
            
            const stmt = this.db.prepare(
                'INSERT OR REPLACE INTO files (' +
                'id, value, metadata, file_name, file_type, file_size, ' +
                'upload_ip, upload_address, list_type, timestamp, ' +
                'label, directory, channel, channel_name, ' +
                'tg_file_id, tg_chat_id, tg_bot_token, is_chunked' +
                ') VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            );
            
            const result = await stmt.bind(
                fileId,
                value,
                JSON.stringify(metadata),
                extractedFields.fileName,
                extractedFields.fileType,
                extractedFields.fileSize,
                extractedFields.uploadIP,
                extractedFields.uploadAddress,
                extractedFields.listType,
                extractedFields.timestamp,
                extractedFields.label,
                extractedFields.directory,
                extractedFields.channel,
                extractedFields.channelName,
                extractedFields.tgFileId,
                extractedFields.tgChatId,
                extractedFields.tgBotToken,
                extractedFields.isChunked
            ).run();

            // 更新缓存
            this.clearCache(`file:${fileId}`);
            this.clearCache(`fileWithMetadata:${fileId}`);
            
            return result;
        });
    }

    /**
     * 获取文件记录 (替代 KV.get)
     * @param {string} fileId - 文件ID
     * @param {Object} [options={}] - 选项
     * @param {boolean} [options.bypassCache=false] - 是否绕过缓存
     * @returns {Promise<Object|null>} 文件记录或null
     */
    getFile(fileId, options = {}) {
        return this.executeQuery('getFile', async () => {
            // 参数验证
            if (!fileId || typeof fileId !== 'string') {
                throw new Error('Invalid fileId');
            }

            const { bypassCache = false } = options;
            
            // 检查缓存
            const cacheKey = `file:${fileId}`;
            if (!bypassCache) {
                const cached = this.getCachedData(cacheKey);
                if (cached) return cached;
            }

            const stmt = this.db.prepare('SELECT * FROM files WHERE id = ?');
            const result = await stmt.bind(fileId).first();
            
            if (!result) return null;
            
            const file = {
                value: result.value,
                metadata: JSON.parse(result.metadata || '{}')
            };
            
            // 缓存结果
            this.cacheData(cacheKey, file);
            
            return file;
        });
    }

    /**
     * 获取文件记录包含元数据 (替代 KV.getWithMetadata)
     * @param {string} fileId - 文件ID
     * @param {Object} [options={}] - 选项
     * @param {boolean} [options.bypassCache=false] - 是否绕过缓存
     * @returns {Promise<Object|null>} 文件记录或null
     */
    getFileWithMetadata(fileId, options = {}) {
        return this.getFile(fileId, options);
    }

    /**
     * 删除文件记录 (替代 KV.delete)
     * @param {string} fileId - 文件ID
     * @returns {Promise<Object>} 操作结果
     */
    deleteFile(fileId) {
        return this.executeQuery('deleteFile', async () => {
            // 参数验证
            if (!fileId || typeof fileId !== 'string') {
                throw new Error('Invalid fileId');
            }

            const stmt = this.db.prepare('DELETE FROM files WHERE id = ?');
            const result = await stmt.bind(fileId).run();
            
            // 清除缓存
            this.clearCache(`file:${fileId}`);
            this.clearCache(`fileWithMetadata:${fileId}`);
            
            return result;
        });
    }

    /**
     * 列出文件 (替代 KV.list)
     * @param {ListOptions} [options={}] - 选项
     * @returns {Promise<Object>} 文件列表
     */
    listFiles(options = {}) {
        return this.executeQuery('listFiles', async () => {
            options = {
                prefix: '',
                limit: 1000,
                cursor: null,
                order: 'asc',
                sortBy: 'id',
                ...options
            };
            
            const { prefix, limit, cursor, order, sortBy } = options;
            
            // 验证排序字段
            const validSortFields = ['id', 'timestamp', 'file_size', 'file_name'];
            const sortField = validSortFields.includes(sortBy) ? sortBy : 'id';
            
            // 验证排序方向
            const sortOrder = order.toLowerCase() === 'desc' ? 'DESC' : 'ASC';
            
            let query = `SELECT id, metadata FROM files`;
            const params = [];
            
            if (prefix) {
                query += ' WHERE id LIKE ?';
                params.push(prefix + '%');
            }
            
            if (cursor) {
                query += prefix ? ' AND' : ' WHERE';
                query += ` ${sortField} ${sortOrder === 'ASC' ? '>' : '<'} ?`;
                params.push(cursor);
            }
            
            query += ` ORDER BY ${sortField} ${sortOrder} LIMIT ?`;
            params.push(limit + 1);
            
            const stmt = this.db.prepare(query);
            const response = params.length > 0 
                ? await stmt.bind(...params).all()
                : await stmt.all();
            
            const results = response.results || [];
            const hasMore = results.length > limit;
            const files = hasMore ? results.slice(0, -1) : results;

            const keys = files.map(row => ({
                name: row.id,
                metadata: JSON.parse(row.metadata || '{}')
            }));
            
            return {
                keys,
                cursor: hasMore && keys.length > 0 ? keys[keys.length - 1][sortField] : null,
                list_complete: !hasMore
            };
        });
    }

    // ==================== 设置操作 ====================

    /**
     * 保存设置 (替代 KV.put)
     * @param {string} key - 设置键
     * @param {string} value - 设置值
     * @param {string} [category] - 分类
     * @returns {Promise<Object>} 操作结果
     */
    putSetting(key, value, category) {
        return this.executeQuery('putSetting', async () => {
            // 参数验证
            if (!key || typeof key !== 'string') {
                throw new Error('Invalid setting key');
            }

            if (value === undefined || value === null) {
                throw new Error('Setting value cannot be null or undefined');
            }

            if (!category && key.startsWith('manage@sysConfig@')) {
                category = key.split('@')[2];
            }
            
            const stmt = this.db.prepare(
                'INSERT OR REPLACE INTO settings (key, value, category) VALUES (?, ?, ?)'
            );
            
            const result = await stmt.bind(key, value, category).run();
            
            // 更新缓存
            this.clearCache(`setting:${key}`);
            
            return result;
        });
    }

    /**
     * 获取设置 (替代 KV.get)
     * @param {string} key - 设置键
     * @param {Object} [options={}] - 选项
     * @param {boolean} [options.bypassCache=false] - 是否绕过缓存
     * @returns {Promise<string|null>} 设置值或null
     */
    getSetting(key, options = {}) {
        return this.executeQuery('getSetting', async () => {
            // 参数验证
            if (!key || typeof key !== 'string') {
                throw new Error('Invalid setting key');
            }

            const { bypassCache = false } = options;
            
            // 检查缓存
            const cacheKey = `setting:${key}`;
            if (!bypassCache) {
                const cached = this.getCachedData(cacheKey);
                if (cached !== null) return cached;
            }

            const stmt = this.db.prepare('SELECT value FROM settings WHERE key = ?');
            const result = await stmt.bind(key).first();
            
            const value = result ? result.value : null;
            
            // 缓存结果
            this.cacheData(cacheKey, value);
            
            return value;
        });
    }

    /**
     * 删除设置 (替代 KV.delete)
     * @param {string} key - 设置键
     * @returns {Promise<Object>} 操作结果
     */
    deleteSetting(key) {
        return this.executeQuery('deleteSetting', async () => {
            // 参数验证
            if (!key || typeof key !== 'string') {
                throw new Error('Invalid setting key');
            }

            const stmt = this.db.prepare('DELETE FROM settings WHERE key = ?');
            const result = await stmt.bind(key).run();
            
            // 清除缓存
            this.clearCache(`setting:${key}`);
            
            return result;
        });
    }

    /**
     * 列出设置 (替代 KV.list)
     * @param {ListOptions} [options={}] - 选项
     * @returns {Promise<Object>} 设置列表
     */
    listSettings(options = {}) {
        return this.executeQuery('listSettings', async () => {
            options = {
                prefix: '',
                limit: 1000,
                order: 'asc',
                sortBy: 'key',
                ...options
            };
            
            const { prefix, limit, order, sortBy } = options;
            
            // 验证排序字段
            const validSortFields = ['key', 'category'];
            const sortField = validSortFields.includes(sortBy) ? sortBy : 'key';
            
            // 验证排序方向
            const sortOrder = order.toLowerCase() === 'desc' ? 'DESC' : 'ASC';
            
            let query = `SELECT key, value, category FROM settings`;
            const params = [];
            
            if (prefix) {
                query += ' WHERE key LIKE ?';
                params.push(prefix + '%');
            }
            
            query += ` ORDER BY ${sortField} ${sortOrder} LIMIT ?`;
            params.push(limit);
            
            const stmt = this.db.prepare(query);
            const response = params.length > 0 
                ? await stmt.bind(...params).all()
                : await stmt.all();
            
            const results = response.results || [];
            const keys = results.map(row => ({
                name: row.key,
                value: row.value,
                category: row.category
            }));

            return { keys };
        });
    }

    // ==================== 索引操作 ====================

    /**
     * 保存索引操作记录
     * @param {string} operationId - 操作ID
     * @param {Object} operation - 操作对象
     * @param {string} operation.type - 操作类型
     * @param {number} operation.timestamp - 时间戳
     * @param {Object} operation.data - 操作数据
     * @returns {Promise<Object>} 操作结果
     */
    putIndexOperation(operationId, operation) {
        return this.executeQuery('putIndexOperation', async () => {
            // 参数验证
            if (!operationId || typeof operationId !== 'string') {
                throw new Error('Invalid operationId');
            }
            if (!operation || !operation.type || !operation.timestamp) {
                throw new Error('Invalid operation object');
            }

            const stmt = this.db.prepare(
                'INSERT OR REPLACE INTO index_operations (id, type, timestamp, data, processed) VALUES (?, ?, ?, ?, ?)'
            );
            
            const result = await stmt.bind(
                operationId,
                operation.type,
                operation.timestamp,
                JSON.stringify(operation.data),
                operation.processed || false
            ).run();
            
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
    getIndexOperation(operationId, options = {}) {
        return this.executeQuery('getIndexOperation', async () => {
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

            const stmt = this.db.prepare('SELECT * FROM index_operations WHERE id = ?');
            const result = await stmt.bind(operationId).first();
            
            if (!result) return null;
            
            const operation = {
                id: result.id,
                type: result.type,
                timestamp: result.timestamp,
                data: JSON.parse(result.data || '{}'),
                processed: result.processed || false
            };
            
            // 缓存结果
            this.cacheData(cacheKey, operation);
            
            return operation;
        });
    }

    /**
     * 删除索引操作记录
     * @param {string} operationId - 操作ID
     * @returns {Promise<Object>} 操作结果
     */
    deleteIndexOperation(operationId) {
        return this.executeQuery('deleteIndexOperation', async () => {
            // 参数验证
            if (!operationId || typeof operationId !== 'string') {
                throw new Error('Invalid operationId');
            }

            const stmt = this.db.prepare('DELETE FROM index_operations WHERE id = ?');
            const result = await stmt.bind(operationId).run();
            
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
    listIndexOperations(options = {}) {
        return this.executeQuery('listIndexOperations', async () => {
            options = {
                limit: 1000,
                processed: undefined,
                order: 'asc',
                ...options
            };
            
            const { limit, processed, order } = options;
            
            // 验证排序方向
            const sortOrder = order.toLowerCase() === 'desc' ? 'DESC' : 'ASC';
            
            let query = 'SELECT * FROM index_operations';
            const params = [];
            
            if (processed !== undefined) {
                query += ' WHERE processed = ?';
                params.push(processed);
            }
            
            query += ` ORDER BY timestamp ${sortOrder} LIMIT ?`;
            params.push(limit);
            
            const stmt = this.db.prepare(query);
            const response = params.length > 0 
                ? await stmt.bind(...params).all()
                : await stmt.all();
            
            const results = response.results || [];
            return results.map(row => ({
                id: row.id,
                type: row.type,
                timestamp: row.timestamp,
                data: JSON.parse(row.data || '{}'),
                processed: row.processed || false
            }));
        });
    }

    /**
     * 标记索引操作已处理
     * @param {string} operationId - 操作ID
     * @param {boolean} [processed=true] - 是否处理
     * @returns {Promise<Object>} 操作结果
     */
    markIndexOperationProcessed(operationId, processed = true) {
        return this.executeQuery('markIndexOperationProcessed', async () => {
            // 参数验证
            if (!operationId || typeof operationId !== 'string') {
                throw new Error('Invalid operationId');
            }

            const stmt = this.db.prepare(
                'UPDATE index_operations SET processed = ? WHERE id = ?'
            );
            
            const result = await stmt.bind(processed, operationId).run();
            
            // 清除缓存
            this.clearCache(`indexOperation:${operationId}`);
            
            return result;
        });
    }

    // ==================== 工具方法 ====================

    /**
     * 从metadata中提取字段用于索引
     * @param {FileMetadata} metadata - 元数据
     * @returns {Object} 提取的字段
     */
    extractMetadataFields(metadata) {
        if (!metadata) return {};

        return {
            fileName: metadata.FileName || null,
            fileType: metadata.FileType || null,
            fileSize: metadata.FileSize || null,
            uploadIP: metadata.UploadIP || null,
            uploadAddress: metadata.UploadAddress || null,
            listType: metadata.ListType || null,
            timestamp: metadata.TimeStamp || null,
            label: metadata.Label || null,
            directory: metadata.Directory || null,
            channel: metadata.Channel || null,
            channelName: metadata.ChannelName || null,
            tgFileId: metadata.TgFileId || null,
            tgChatId: metadata.TgChatId || null,
            tgBotToken: metadata.TgBotToken || null,
            isChunked: metadata.IsChunked || false
        };
    }

    /**
     * 验证文件ID
     * @param {string} fileId - 文件ID
     * @returns {boolean} 是否有效
     */
    validateFileId(fileId) {
        return typeof fileId === 'string' && fileId.length > 0 && fileId.length <= 256;
    }

    /**
     * 验证元数据
     * @param {FileMetadata} metadata - 元数据
     * @returns {boolean} 是否有效
     */
    validateMetadata(metadata) {
        if (!metadata || typeof metadata !== 'object') return false;
        
        // 检查元数据大小
        const metadataSize = JSON.stringify(metadata).length;
        return metadataSize <= 16384; // 限制为16KB
    }

    // ==================== 事务支持 ====================

    /**
     * 执行事务
     * @param {Function} transactionFunction - 事务函数
     * @returns {Promise<any>} 事务结果
     */
    async transaction(transactionFunction) {
        return this.executeQuery('transaction', async () => {
            if (typeof transactionFunction !== 'function') {
                throw new Error('Transaction function is required');
            }

            try {
                await this.db.batch([
                    this.db.prepare('BEGIN TRANSACTION').bind()
                ]);

                const result = await transactionFunction(this);
                
                await this.db.batch([
                    this.db.prepare('COMMIT').bind()
                ]);
                
                return result;
            } catch (error) {
                await this.db.batch([
                    this.db.prepare('ROLLBACK').bind()
                ]);
                throw error;
            }
        });
    }

    /**
     * 批量操作
     * @param {Array} operations - 操作数组
     * @returns {Promise<Object>} 操作结果
     */
    batch(operations) {
        return this.executeQuery('batch', async () => {
            if (!Array.isArray(operations) || operations.length === 0) {
                throw new Error('Operations array is required');
            }

            const preparedOperations = operations.map(op => {
                switch (op.type) {
                    case 'put':
                        return this.preparePutOperation(op);
                    case 'delete':
                        return this.prepareDeleteOperation(op);
                    default:
                        throw new Error(`Unsupported operation type: ${op.type}`);
                }
            });

            return this.db.batch(preparedOperations);
        });
    }

    /**
     * 准备put操作
     * @param {Object} op - 操作对象
     * @returns {Object} 准备好的操作
     */
    preparePutOperation(op) {
        const { key, value, options = {} } = op;
        
        if (key.startsWith('manage@sysConfig@')) {
            return this.db.prepare(
                'INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)'
            ).bind(key, value);
        } else if (key.startsWith('manage@index@operation_')) {
            const operationId = key.replace('manage@index@operation_', '');
            const operation = JSON.parse(value);
            return this.db.prepare(
                'INSERT OR REPLACE INTO index_operations (id, type, timestamp, data) VALUES (?, ?, ?, ?)'
            ).bind(
                operationId,
                operation.type,
                operation.timestamp,
                JSON.stringify(operation.data)
            );
        } else {
            const metadata = options.metadata || {};
            const extractedFields = this.extractMetadataFields(metadata);
            
            return this.db.prepare(
                'INSERT OR REPLACE INTO files (id, value, metadata) VALUES (?, ?, ?)'
            ).bind(
                key,
                value,
                JSON.stringify(metadata)
            );
        }
    }

    /**
     * 准备delete操作
     * @param {Object} op - 操作对象
     * @returns {Object} 准备好的操作
     */
    prepareDeleteOperation(op) {
        const { key } = op;
        
        if (key.startsWith('manage@sysConfig@')) {
            return this.db.prepare('DELETE FROM settings WHERE key = ?').bind(key);
        } else if (key.startsWith('manage@index@operation_')) {
            const operationId = key.replace('manage@index@operation_', '');
            return this.db.prepare('DELETE FROM index_operations WHERE id = ?').bind(operationId);
        } else {
            return this.db.prepare('DELETE FROM files WHERE id = ?').bind(key);
        }
    }

    // ==================== 通用方法 ====================

    /**
     * 通用的put方法，根据key类型自动选择存储位置
     * @param {string} key - 键
     * @param {string} value - 值
     * @param {Object} [options={}] - 选项
     * @returns {Promise<Object>} 操作结果
     */
    put(key, value, options = {}) {
        return this.executeQuery('put', async () => {
            // 参数验证
            if (!key || typeof key !== 'string') {
                throw new Error('Invalid key');
            }

            if (key.startsWith('manage@sysConfig@')) {
                return this.putSetting(key, value);
            } else if (key.startsWith('manage@index@operation_')) {
                const operationId = key.replace('manage@index@operation_', '');
                const operation = JSON.parse(value);
                return this.putIndexOperation(operationId, operation);
            } else {
                return this.putFile(key, value, options);
            }
        });
    }

    /**
     * 通用的get方法，根据key类型自动选择获取位置
     * @param {string} key - 键
     * @param {Object} [options={}] - 选项
     * @param {boolean} [options.bypassCache=false] - 是否绕过缓存
     * @returns {Promise<string|null>} 值或null
     */
    get(key, options = {}) {
        return this.executeQuery('get', async () => {
            // 参数验证
            if (!key || typeof key !== 'string') {
                throw new Error('Invalid key');
            }

            if (key.startsWith('manage@sysConfig@')) {
                return this.getSetting(key, options);
            } else if (key.startsWith('manage@index@operation_')) {
                const operationId = key.replace('manage@index@operation_', '');
                const operation = await this.getIndexOperation(operationId, options);
                return operation ? JSON.stringify(operation) : null;
            } else {
                const file = await this.getFile(key, options);
                return file ? file.value : null;
            }
        });
    }

    /**
     * 通用的getWithMetadata方法
     * @param {string} key - 键
     * @param {Object} [options={}] - 选项
     * @param {boolean} [options.bypassCache=false] - 是否绕过缓存
     * @returns {Promise<Object|null>} 值和元数据或null
     */
    getWithMetadata(key, options = {}) {
        return this.executeQuery('getWithMetadata', async () => {
            // 参数验证
            if (!key || typeof key !== 'string') {
                throw new Error('Invalid key');
            }

            if (key.startsWith('manage@sysConfig@')) {
                const value = await this.getSetting(key, options);
                return value ? { value, metadata: {} } : null;
            } else {
                return this.getFileWithMetadata(key, options);
            }
        });
    }

    /**
     * 通用的delete方法
     * @param {string} key - 键
     * @returns {Promise<Object>} 操作结果
     */
    delete(key) {
        return this.executeQuery('delete', async () => {
            // 参数验证
            if (!key || typeof key !== 'string') {
                throw new Error('Invalid key');
            }

            if (key.startsWith('manage@sysConfig@')) {
                return this.deleteSetting(key);
            } else if (key.startsWith('manage@index@operation_')) {
                const operationId = key.replace('manage@index@operation_', '');
                return this.deleteIndexOperation(operationId);
            } else {
                return this.deleteFile(key);
            }
        });
    }

    /**
     * 通用的list方法
     * @param {ListOptions} [options={}] - 选项
     * @returns {Promise<Object>} 列表结果
     */
    list(options = {}) {
        return this.executeQuery('list', async () => {
            options = options || {};
            const prefix = options.prefix || '';

            if (prefix.startsWith('manage@sysConfig@')) {
                return this.listSettings(options);
            } else if (prefix.startsWith('manage@index@operation_')) {
                const operations = await this.listIndexOperations(options);
                const keys = operations.map(op => ({
                    name: 'manage@index@operation_' + op.id
                }));
                return { keys };
            } else {
                return this.listFiles(options);
            }
        });
    }
}

// 导出构造函数
export { D1Database };

/**
 * 创建 D1 数据库实例的工厂函数
 * @param {Object} db - D1 数据库实例
 * @param {DatabaseOptions} [options={}] - 数据库选项
 * @returns {D1Database} D1 数据库实例
 */
export function createD1Database(db, options = {}) {
    return new D1Database(db, options);
}

/**
 * D1 数据库常量
 */
export const D1_CONSTANTS = {
    MAX_METADATA_SIZE: 16384, // 16KB
    MAX_KEY_LENGTH: 256,
    DEFAULT_CACHE_TTL: 300000, // 5分钟
    DEFAULT_LIMIT: 1000,
    VALID_SORT_FIELDS: ['id', 'timestamp', 'file_size', 'file_name']
};
