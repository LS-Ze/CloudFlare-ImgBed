/**
 * D1 数据库操作工具类
 */

// 统一错误类
export class DatabaseError extends Error {
    constructor(message, code = 'DATABASE_ERROR', status = 500, details = {}) {
        super(message);
        this.name = 'DatabaseError';
        this.code = code;
        this.status = status;
        this.details = details;
        this.timestamp = Date.now();
    }
}

// 缓存配置
const CACHE_CONFIG = {
    FILES: { ttl: 300, maxSize: 1000 }, // 5分钟缓存，最多1000个文件
    SETTINGS: { ttl: 300, maxSize: 100 }, // 5分钟缓存，最多100个设置
    INDEX_OPERATIONS: { ttl: 60, maxSize: 500 } // 1分钟缓存，最多500个索引操作
};

// 内存缓存
const memoryCache = new Map();

class D1Database {
    constructor(db) {
        this.db = db;
        
        // 手动包装所有方法的错误处理
        this.putFile = wrapWithErrorHandling(this.putFile.bind(this), 'putFile');
        this.getFile = wrapWithErrorHandling(this.getFile.bind(this), 'getFile');
        this.getFileWithMetadata = wrapWithErrorHandling(this.getFileWithMetadata.bind(this), 'getFileWithMetadata');
        this.deleteFile = wrapWithErrorHandling(this.deleteFile.bind(this), 'deleteFile');
        this.listFiles = wrapWithErrorHandling(this.listFiles.bind(this), 'listFiles');
        this.putSetting = wrapWithErrorHandling(this.putSetting.bind(this), 'putSetting');
        this.getSetting = wrapWithErrorHandling(this.getSetting.bind(this), 'getSetting');
        this.deleteSetting = wrapWithErrorHandling(this.deleteSetting.bind(this), 'deleteSetting');
        this.listSettings = wrapWithErrorHandling(this.listSettings.bind(this), 'listSettings');
        this.putIndexOperation = wrapWithErrorHandling(this.putIndexOperation.bind(this), 'putIndexOperation');
        this.getIndexOperation = wrapWithErrorHandling(this.getIndexOperation.bind(this), 'getIndexOperation');
        this.deleteIndexOperation = wrapWithErrorHandling(this.deleteIndexOperation.bind(this), 'deleteIndexOperation');
        this.listIndexOperations = wrapWithErrorHandling(this.listIndexOperations.bind(this), 'listIndexOperations');
        this.markIndexOperationProcessed = wrapWithErrorHandling(this.markIndexOperationProcessed.bind(this), 'markIndexOperationProcessed');
        this.put = wrapWithErrorHandling(this.put.bind(this), 'put');
        this.get = wrapWithErrorHandling(this.get.bind(this), 'get');
        this.getWithMetadata = wrapWithErrorHandling(this.getWithMetadata.bind(this), 'getWithMetadata');
        this.delete = wrapWithErrorHandling(this.delete.bind(this), 'delete');
        this.list = wrapWithErrorHandling(this.list.bind(this), 'list');
        this.transaction = wrapWithErrorHandling(this.transaction.bind(this), 'transaction');
        this.batch = wrapWithErrorHandling(this.batch.bind(this), 'batch');
    }
}

/**
 * 错误处理包装函数
 * @param {Function} method - 要包装的方法
 * @param {String} methodName - 方法名称
 * @returns {Function} 包装后的方法
 */
function wrapWithErrorHandling(method, methodName) {
    return async function(...args) {
        try {
            return await method.apply(this, args);
        } catch (error) {
            const errorDetails = {
                method: methodName,
                args: args,
                error: error.message,
                stack: error.stack
            };
            
            console.error(`Database error in ${methodName}:`, errorDetails);
            
            throw new DatabaseError(
                `Database operation failed: ${error.message}`,
                'DATABASE_OPERATION_FAILED',
                500,
                errorDetails
            );
        }
    };
}

// ==================== 文件操作 ====================

/**
 * 保存文件记录 (替代 KV.put)
 */
D1Database.prototype.putFile = async function(fileId, value, options) {
    this.validateFileId(fileId);
    
    value = value || '';
    options = options || {};
    var metadata = this.validateMetadata(options.metadata || {});
    
    // 从metadata中提取字段用于索引
    var extractedFields = this.extractMetadataFields(metadata);
    
    var stmt = this.db.prepare(
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
    cacheSet('FILES', fileId, { value, metadata });
    
    return result;
};

/**
 * 获取文件记录 (替代 KV.get)
 */
D1Database.prototype.getFile = async function(fileId) {
    this.validateFileId(fileId);
    
    // 尝试从缓存获取
    const cachedFile = cacheGet('FILES', fileId);
    if (cachedFile) {
        return {
            value: cachedFile.value,
            metadata: cachedFile.metadata
        };
    }
    
    var stmt = this.db.prepare('SELECT * FROM files WHERE id = ?');
    const result = await stmt.bind(fileId).first();
    
    if (!result) return null;
    
    const file = {
        value: result.value,
        metadata: JSON.parse(result.metadata || '{}')
    };
    
    // 缓存结果
    cacheSet('FILES', fileId, file);
    
    return file;
};

/**
 * 获取文件记录包含元数据 (替代 KV.getWithMetadata)
 */
D1Database.prototype.getFileWithMetadata = async function(fileId) {
    return this.getFile(fileId);
};

/**
 * 删除文件记录 (替代 KV.delete)
 */
D1Database.prototype.deleteFile = async function(fileId) {
    this.validateFileId(fileId);
    
    var stmt = this.db.prepare('DELETE FROM files WHERE id = ?');
    const result = await stmt.bind(fileId).run();
    
    // 清除缓存
    cacheDelete('FILES', fileId);
    
    return result;
};

/**
 * 列出文件 (替代 KV.list)
 */
D1Database.prototype.listFiles = async function(options) {
    options = options || {};
    var prefix = options.prefix || '';
    var limit = options.limit || 1000;
    var cursor = options.cursor || null;
    
    var query = 'SELECT id, metadata FROM files';
    var params = [];
    
    if (prefix) {
        query += ' WHERE id LIKE ?';
        params.push(prefix + '%');
    }
    
    if (cursor) {
        query += prefix ? ' AND' : ' WHERE';
        query += ' id > ?';
        params.push(cursor);
    }
    
    query += ' ORDER BY id LIMIT ?';
    params.push(limit + 1);
    
    var stmt = this.db.prepare(query);
    if (params.length > 0) {
        stmt = stmt.bind.apply(stmt, params);
    }
    
    const response = await stmt.all();
    var results = response.results || [];
    var hasMore = results.length > limit;
    if (hasMore) {
        results.pop();
    }

    var keys = results.map(function(row) {
        return {
            name: row.id,
            metadata: JSON.parse(row.metadata || '{}')
        };
    });
    
    return {
        keys: keys,
        cursor: hasMore && keys.length > 0 ? keys[keys.length - 1].name : null,
        list_complete: !hasMore
    };
};

// ==================== 设置操作 ====================

/**
 * 保存设置 (替代 KV.put)
 */
D1Database.prototype.putSetting = async function(key, value, category) {
    if (!key || typeof key !== 'string') {
        throw new DatabaseError('Invalid setting key', 'INVALID_SETTING_KEY', 400);
    }
    
    if (!category && key.startsWith('manage@sysConfig@')) {
        category = key.split('@')[2];
    }
    
    var stmt = this.db.prepare(
        'INSERT OR REPLACE INTO settings (key, value, category) VALUES (?, ?, ?)'
    );
    
    const result = await stmt.bind(key, value, category).run();
    
    // 更新缓存
    cacheSet('SETTINGS', key, value);
    
    return result;
};

/**
 * 获取设置 (替代 KV.get)
 */
D1Database.prototype.getSetting = async function(key) {
    if (!key || typeof key !== 'string') {
        throw new DatabaseError('Invalid setting key', 'INVALID_SETTING_KEY', 400);
    }
    
    // 尝试从缓存获取
    const cachedValue = cacheGet('SETTINGS', key);
    if (cachedValue !== null) {
        return cachedValue;
    }
    
    var stmt = this.db.prepare('SELECT value FROM settings WHERE key = ?');
    const result = await stmt.bind(key).first();
    
    const value = result ? result.value : null;
    
    // 缓存结果
    if (value !== null) {
        cacheSet('SETTINGS', key, value);
    }
    
    return value;
};

/**
 * 删除设置 (替代 KV.delete)
 */
D1Database.prototype.deleteSetting = async function(key) {
    if (!key || typeof key !== 'string') {
        throw new DatabaseError('Invalid setting key', 'INVALID_SETTING_KEY', 400);
    }
    
    var stmt = this.db.prepare('DELETE FROM settings WHERE key = ?');
    const result = await stmt.bind(key).run();
    
    // 清除缓存
    cacheDelete('SETTINGS', key);
    
    return result;
};

/**
 * 列出设置 (替代 KV.list)
 */
D1Database.prototype.listSettings = async function(options) {
    options = options || {};
    var prefix = options.prefix || '';
    var limit = options.limit || 1000;
    
    var query = 'SELECT key, value FROM settings';
    var params = [];
    
    if (prefix) {
        query += ' WHERE key LIKE ?';
        params.push(prefix + '%');
    }
    
    query += ' ORDER BY key LIMIT ?';
    params.push(limit);
    
    var stmt = this.db.prepare(query);
    if (params.length > 0) {
        stmt = stmt.bind.apply(stmt, params);
    }
    
    const response = await stmt.all();
    var results = response.results || [];
    var keys = results.map(function(row) {
        return {
            name: row.key,
            value: row.value
        };
    });

    return { keys: keys };
};

// ==================== 索引操作 ====================

/**
 * 保存索引操作记录
 */
D1Database.prototype.putIndexOperation = async function(operationId, operation) {
    if (!operationId || typeof operationId !== 'string') {
        throw new DatabaseError('Invalid operation ID', 'INVALID_OPERATION_ID', 400);
    }
    
    if (!operation || typeof operation !== 'object') {
        throw new DatabaseError('Invalid operation data', 'INVALID_OPERATION_DATA', 400);
    }
    
    if (!operation.type || !operation.timestamp || !operation.data) {
        throw new DatabaseError('Operation data is missing required fields', 'MISSING_OPERATION_FIELDS', 400);
    }
    
    var stmt = this.db.prepare(
        'INSERT OR REPLACE INTO index_operations (id, type, timestamp, data, processed) VALUES (?, ?, ?, ?, ?)'
    );
    
    const result = await stmt.bind(
        operationId,
        operation.type,
        operation.timestamp,
        JSON.stringify(operation.data),
        operation.processed || 0
    ).run();
    
    // 更新缓存
    cacheSet('INDEX_OPERATIONS', operationId, operation);
    
    return result;
};

/**
 * 获取索引操作记录
 */
D1Database.prototype.getIndexOperation = async function(operationId) {
    if (!operationId || typeof operationId !== 'string') {
        throw new DatabaseError('Invalid operation ID', 'INVALID_OPERATION_ID', 400);
    }
    
    // 尝试从缓存获取
    const cachedOperation = cacheGet('INDEX_OPERATIONS', operationId);
    if (cachedOperation) {
        return cachedOperation;
    }
    
    var stmt = this.db.prepare('SELECT * FROM index_operations WHERE id = ?');
    const result = await stmt.bind(operationId).first();
    
    if (!result) return null;
    
    const operation = {
        type: result.type,
        timestamp: result.timestamp,
        data: JSON.parse(result.data),
        processed: result.processed
    };
    
    // 缓存结果
    cacheSet('INDEX_OPERATIONS', operationId, operation);
    
    return operation;
};

/**
 * 删除索引操作记录
 */
D1Database.prototype.deleteIndexOperation = async function(operationId) {
    if (!operationId || typeof operationId !== 'string') {
        throw new DatabaseError('Invalid operation ID', 'INVALID_OPERATION_ID', 400);
    }
    
    var stmt = this.db.prepare('DELETE FROM index_operations WHERE id = ?');
    const result = await stmt.bind(operationId).run();
    
    // 清除缓存
    cacheDelete('INDEX_OPERATIONS', operationId);
    
    return result;
};

/**
 * 列出索引操作记录
 */
D1Database.prototype.listIndexOperations = async function(options) {
    options = options || {};
    var limit = options.limit || 1000;
    var processed = options.processed;
    
    var query = 'SELECT * FROM index_operations';
    var params = [];
    
    if (processed !== null && processed !== undefined) {
        query += ' WHERE processed = ?';
        params.push(processed ? 1 : 0);
    }
    
    query += ' ORDER BY timestamp LIMIT ?';
    params.push(limit);
    
    var stmt = this.db.prepare(query);
    if (params.length > 0) {
        stmt = stmt.bind.apply(stmt, params);
    }
    
    const response = await stmt.all();
    var results = response.results || [];
    
    return results.map(function(row) {
        return {
            id: row.id,
            type: row.type,
            timestamp: row.timestamp,
            data: JSON.parse(row.data),
            processed: row.processed
        };
    });
};

/**
 * 标记索引操作已处理
 */
D1Database.prototype.markIndexOperationProcessed = async function(operationId) {
    if (!operationId || typeof operationId !== 'string') {
        throw new DatabaseError('Invalid operation ID', 'INVALID_OPERATION_ID', 400);
    }
    
    var stmt = this.db.prepare(
        'UPDATE index_operations SET processed = 1 WHERE id = ?'
    );
    
    const result = await stmt.bind(operationId).run();
    
    // 更新缓存
    const operation = await this.getIndexOperation(operationId);
    if (operation) {
        operation.processed = 1;
        cacheSet('INDEX_OPERATIONS', operationId, operation);
    }
    
    return result;
};

// ==================== 工具方法 ====================

/**
 * 从metadata中提取字段用于索引
 */
D1Database.prototype.extractMetadataFields = function(metadata) {
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
};

/**
 * 验证文件ID
 */
D1Database.prototype.validateFileId = function(fileId) {
    if (!fileId || typeof fileId !== 'string') {
        throw new DatabaseError('Invalid file ID', 'INVALID_FILE_ID', 400);
    }
    
    // 文件ID格式验证（可以根据实际需求调整）
    if (fileId.length < 8 || fileId.length > 128) {
        throw new DatabaseError('File ID must be between 8 and 128 characters', 'INVALID_FILE_ID_LENGTH', 400);
    }
    
    // 可以添加更多验证规则
    return true;
};

/**
 * 验证元数据
 */
D1Database.prototype.validateMetadata = function(metadata) {
    if (!metadata || typeof metadata !== 'object') {
        return {};
    }
    
    // 过滤无效字段
    const validMetadata = {};
    const allowedFields = [
        'FileName', 'FileType', 'FileSize', 'UploadIP', 'UploadAddress',
        'ListType', 'TimeStamp', 'Label', 'Directory', 'Channel',
        'ChannelName', 'TgFileId', 'TgChatId', 'TgBotToken', 'IsChunked'
    ];
    
    for (const field of allowedFields) {
        if (metadata.hasOwnProperty(field)) {
            validMetadata[field] = metadata[field];
        }
    }
    
    return validMetadata;
};

// ==================== 通用方法 ====================

/**
 * 通用的put方法，根据key类型自动选择存储位置
 */
D1Database.prototype.put = async function(key, value, options) {
    options = options || {};

    if (key.startsWith('manage@sysConfig@')) {
        return this.putSetting(key, value);
    } else if (key.startsWith('manage@index@operation_')) {
        var operationId = key.replace('manage@index@operation_', '');
        var operation = typeof value === 'string' ? JSON.parse(value) : value;
        return this.putIndexOperation(operationId, operation);
    } else {
        return this.putFile(key, value, options);
    }
};

/**
 * 通用的get方法，根据key类型自动选择获取位置
 */
D1Database.prototype.get = async function(key) {
    if (key.startsWith('manage@sysConfig@')) {
        return this.getSetting(key);
    } else if (key.startsWith('manage@index@operation_')) {
        var operationId = key.replace('manage@index@operation_', '');
        const operation = await this.getIndexOperation(operationId);
        return operation ? JSON.stringify(operation) : null;
    } else {
        const file = await this.getFile(key);
        return file ? file.value : null;
    }
};

/**
 * 通用的getWithMetadata方法
 */
D1Database.prototype.getWithMetadata = async function(key) {
    if (key.startsWith('manage@sysConfig@')) {
        const value = await this.getSetting(key);
        return value ? { value: value, metadata: {} } : null;
    } else {
        return this.getFileWithMetadata(key);
    }
};

/**
 * 通用的delete方法
 */
D1Database.prototype.delete = async function(key) {
    if (key.startsWith('manage@sysConfig@')) {
        return this.deleteSetting(key);
    } else if (key.startsWith('manage@index@operation_')) {
        var operationId = key.replace('manage@index@operation_', '');
        return this.deleteIndexOperation(operationId);
    } else {
        return this.deleteFile(key);
    }
};

/**
 * 通用的list方法
 */
D1Database.prototype.list = async function(options) {
    options = options || {};
    var prefix = options.prefix || '';

    if (prefix.startsWith('manage@sysConfig@')) {
        return this.listSettings(options);
    } else if (prefix.startsWith('manage@index@operation_')) {
        const operations = await this.listIndexOperations(options);
        var keys = operations.map(function(op) {
            return {
                name: 'manage@index@operation_' + op.id
            };
        });
        return { keys: keys };
    } else {
        return this.listFiles(options);
    }
};

/**
 * 执行事务
 */
D1Database.prototype.transaction = async function(operations) {
    if (!Array.isArray(operations) || operations.length === 0) {
        throw new DatabaseError('Invalid operations array', 'INVALID_OPERATIONS', 400);
    }
    
    try {
        const results = [];
        
        for (const op of operations) {
            switch (op.type) {
                case 'put':
                    results.push(await this.put(op.key, op.value, op.options));
                    break;
                case 'delete':
                    results.push(await this.delete(op.key));
                    break;
                default:
                    throw new DatabaseError(`Unsupported operation type: ${op.type}`, 'UNSUPPORTED_OPERATION', 400);
            }
        }
        
        return results;
    } catch (error) {
        // 在D1中，事务是自动的，每个语句都是原子的
        // 如果需要真正的事务支持，需要使用D1的事务API
        throw error;
    }
};

/**
 * 批量操作
 */
D1Database.prototype.batch = async function(operations) {
    return this.transaction(operations);
};

/**
 * 清除缓存
 */
D1Database.prototype.clearCache = function(type) {
    if (type) {
        cacheClear(type);
    } else {
        memoryCache.clear();
    }
};

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

function cacheClear(type) {
    Array.from(memoryCache.keys())
        .filter(k => k.startsWith(`${type}_`))
        .forEach(key => memoryCache.delete(key));
}

// 导出构造函数和错误类
export { D1Database };
