/**
 * Discord API 封装类
 * 用于上传文件到 Discord 频道并获取文件
 */
export class DiscordAPI {
    /**
     * 创建 Discord API 实例
     * @param {string} botToken - Discord 机器人令牌
     * @param {Object} [options={}] - 配置选项
     * @param {number} [options.timeout=30000] - 请求超时时间（毫秒）
     * @param {number} [options.maxRetries=3] - 最大重试次数
     * @param {number} [options.retryDelay=1000] - 重试延迟（毫秒）
     * @param {boolean} [options.enableCache=true] - 是否启用缓存
     * @param {number} [options.cacheTTL=3600000] - 缓存有效期（毫秒）
     */
    constructor(botToken, options = {}) {
        // 参数验证
        if (!botToken || typeof botToken !== 'string') {
            throw new Error('Invalid bot token');
        }

        this.botToken = botToken;
        this.options = {
            timeout: 30000,
            maxRetries: 3,
            retryDelay: 1000,
            enableCache: true,
            cacheTTL: 3600000, // 1小时
            ...options
        };
        
        this.baseURL = 'https://discord.com/api/v10';
        this.defaultHeaders = {
            'Authorization': `Bot ${this.botToken}`,
            'User-Agent': 'DiscordBot (CloudFlare-ImgBed, 1.0)'
        };
        
        // 初始化缓存
        this.cache = this.options.enableCache ? new Map() : null;
        
        // 初始化性能监控
        this.performance = {
            uploadTimes: [],
            getMessageTimes: [],
            deleteMessageTimes: []
        };
    }

    /**
     * 带超时和重试的 fetch 函数
     * @param {string} url - 请求 URL
     * @param {Object} [options={}] - 请求选项
     * @param {number} [options.retries=0] - 当前重试次数
     * @returns {Promise<Response>} 请求响应
     */
    async fetchWithRetry(url, options = {}) {
        const { retries = 0, ...fetchOptions } = options;
        
        try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), this.options.timeout);
            
            const response = await fetch(url, {
                ...fetchOptions,
                signal: controller.signal
            });
            
            clearTimeout(timeoutId);
            
            // 429 速率限制处理
            if (response.status === 429) {
                const retryAfter = response.headers.get('Retry-After');
                const waitTime = retryAfter ? parseFloat(retryAfter) * 1000 : this.options.retryDelay * Math.pow(2, retries);
                
                if (retries < this.options.maxRetries) {
                    console.warn(`Discord 429 rate limit, waiting ${waitTime}ms before retry ${retries + 1}/${this.options.maxRetries}`);
                    await new Promise(resolve => setTimeout(resolve, waitTime));
                    return this.fetchWithRetry(url, { ...options, retries: retries + 1 });
                }
            }
            
            if (!response.ok) {
                const errorData = await response.json().catch(() => ({ message: response.statusText }));
                throw new Error(`Discord API error: ${response.status} - ${errorData.message}`);
            }
            
            return response;
            
        } catch (error) {
            if (error.name === 'AbortError') {
                throw new Error(`Request timed out after ${this.options.timeout}ms`);
            }
            
            // 重试逻辑
            if (retries < this.options.maxRetries && this.isRetryableError(error)) {
                const delay = this.options.retryDelay * Math.pow(2, retries); // 指数退避
                console.warn(`Retrying request (${retries + 1}/${this.options.maxRetries}) after ${delay}ms: ${error.message}`);
                await new Promise(resolve => setTimeout(resolve, delay));
                return this.fetchWithRetry(url, { ...options, retries: retries + 1 });
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
     * 发送文件到 Discord 频道
     * @param {File|Blob} file - 要发送的文件
     * @param {string} channelId - 频道 ID
     * @param {Object} [options={}] - 上传选项
     * @param {string} [options.fileName=''] - 文件名
     * @param {string} [options.content=''] - 消息内容
     * @param {Function} [options.progressCallback=null] - 进度回调函数
     * @param {AbortSignal} [options.signal=null] - 取消信号
     * @returns {Promise<Object>} API 响应结果
     */
    async sendFile(file, channelId, options = {}) {
        try {
            const {
                fileName = '',
                content = '',
                progressCallback = null,
                signal = null
            } = options;

            // 参数验证
            if (!file || !(file instanceof Blob)) {
                throw new Error('Invalid file');
            }
            if (!channelId || typeof channelId !== 'string') {
                throw new Error('Invalid channel ID');
            }

            // 检查文件大小限制
            const maxFileSize = this.getMaxFileSize();
            if (file.size > maxFileSize) {
                throw new Error(`File size exceeds Discord's limit of ${(maxFileSize / (1024 * 1024)).toFixed(2)}MB`);
            }

            const formData = new FormData();
            
            // Discord 使用 files[0] 作为文件字段名
            if (fileName) {
                formData.append('files[0]', file, fileName);
            } else {
                formData.append('files[0]', file);
            }

            // 添加消息内容（可选）
            if (content) {
                formData.append('content', content);
            }

            const startTime = performance.now();

            // 创建上传请求
            const uploadRequest = new Request(`${this.baseURL}/channels/${channelId}/messages`, {
                method: 'POST',
                headers: this.defaultHeaders,
                body: formData
            });

            // 添加进度跟踪
            const progressRequest = this.addProgressTracking(uploadRequest, file.size, progressCallback);
            
            // 执行上传
            const response = await this.fetchWithRetry(`${this.baseURL}/channels/${channelId}/messages`, {
                method: 'POST',
                headers: this.defaultHeaders,
                body: progressRequest.body,
                signal
            });

            const responseData = await response.json();
            const totalTime = performance.now() - startTime;
            this.performance.uploadTimes.push(totalTime);

            return responseData;
        } catch (error) {
            console.error('Discord sendFile error:', error.message);
            throw error;
        }
    }

    /**
     * 从响应中提取文件信息
     * @param {Object} responseData - Discord API 响应数据
     * @returns {Object|null} 文件信息对象或 null
     */
    getFileInfo(responseData) {
        try {
            if (!responseData || !responseData.id) {
                console.error('Invalid Discord response:', responseData);
                return null;
            }

            // Discord 消息中的附件在 attachments 数组中
            if (responseData.attachments && responseData.attachments.length > 0) {
                const attachment = responseData.attachments[0];
                return {
                    message_id: responseData.id,
                    attachment_id: attachment.id,
                    file_name: attachment.filename,
                    file_size: attachment.size,
                    content_type: attachment.content_type,
                    url: attachment.url,
                    proxy_url: attachment.proxy_url,
                    height: attachment.height,
                    width: attachment.width,
                    duration_secs: attachment.duration_secs,
                    flags: attachment.flags
                };
            }

            return null;
        } catch (error) {
            console.error('Error parsing Discord response:', error.message);
            return null;
        }
    }

    /**
     * 获取消息信息（用于获取文件 URL）
     * @param {string} channelId - 频道 ID
     * @param {string} messageId - 消息 ID
     * @param {Object} [options={}] - 选项
     * @param {boolean} [options.forceRefresh=false] - 是否强制刷新缓存
     * @param {AbortSignal} [options.signal=null] - 取消信号
     * @returns {Promise<Object|null>} 消息数据或 null
     */
    async getMessage(channelId, messageId, options = {}) {
        try {
            const { forceRefresh = false, signal = null } = options;

            // 参数验证
            if (!channelId || typeof channelId !== 'string') {
                throw new Error('Invalid channel ID');
            }
            if (!messageId || typeof messageId !== 'string') {
                throw new Error('Invalid message ID');
            }

            // 检查缓存
            const cacheKey = `message:${channelId}:${messageId}`;
            if (this.cache && !forceRefresh) {
                const cached = this.cache.get(cacheKey);
                if (cached && Date.now() < cached.expires) {
                    return cached.data;
                }
            }

            const startTime = performance.now();
            
            const response = await this.fetchWithRetry(`${this.baseURL}/channels/${channelId}/messages/${messageId}`, {
                method: 'GET',
                headers: this.defaultHeaders,
                signal
            });

            const messageData = await response.json();
            const totalTime = performance.now() - startTime;
            this.performance.getMessageTimes.push(totalTime);

            // 缓存结果
            if (this.cache) {
                this.cache.set(cacheKey, {
                    data: messageData,
                    expires: Date.now() + this.options.cacheTTL
                });
            }

            return messageData;
        } catch (error) {
            console.error('Error getting Discord message:', error.message);
            return null;
        }
    }

    /**
     * 获取文件 URL
     * @param {string} channelId - 频道 ID
     * @param {string} messageId - 消息 ID
     * @param {Object} [options={}] - 选项
     * @param {boolean} [options.forceRefresh=false] - 是否强制刷新缓存
     * @param {AbortSignal} [options.signal=null] - 取消信号
     * @returns {Promise<string|null>} 文件 URL 或 null
     */
    async getFileURL(channelId, messageId, options = {}) {
        try {
            const message = await this.getMessage(channelId, messageId, options);
            
            if (message && message.attachments && message.attachments.length > 0) {
                return message.attachments[0].url;
            }

            return null;
        } catch (error) {
            console.error('Error getting Discord file URL:', error.message);
            return null;
        }
    }

    /**
     * 获取文件内容
     * @param {string} channelId - 频道 ID
     * @param {string} messageId - 消息 ID
     * @param {Object} [options={}] - 选项
     * @param {boolean} [options.forceRefresh=false] - 是否强制刷新缓存
     * @param {Function} [options.progressCallback=null] - 进度回调函数
     * @param {AbortSignal} [options.signal=null] - 取消信号
     * @returns {Promise<Response>} 文件响应
     */
    async getFileContent(channelId, messageId, options = {}) {
        try {
            const { progressCallback = null, signal = null } = options;
            
            const fileURL = await this.getFileURL(channelId, messageId, options);
            
            if (!fileURL) {
                throw new Error(`File URL not found for messageId: ${messageId}`);
            }

            // 创建下载请求
            const downloadRequest = new Request(fileURL);
            
            // 添加进度跟踪
            const progressRequest = this.addProgressTracking(downloadRequest, null, progressCallback);
            
            // 执行下载
            const response = await this.fetchWithRetry(fileURL, {
                signal
            });

            return response;
        } catch (error) {
            console.error('Error getting Discord file content:', error.message);
            throw error;
        }
    }

    /**
     * 删除消息（用于删除文件）
     * @param {string} channelId - 频道 ID
     * @param {string} messageId - 消息 ID
     * @param {Object} [options={}] - 选项
     * @param {AbortSignal} [options.signal=null] - 取消信号
     * @returns {Promise<boolean>} 是否删除成功
     */
    async deleteMessage(channelId, messageId, options = {}) {
        try {
            const { signal = null } = options;

            // 参数验证
            if (!channelId || typeof channelId !== 'string') {
                throw new Error('Invalid channel ID');
            }
            if (!messageId || typeof messageId !== 'string') {
                throw new Error('Invalid message ID');
            }

            const startTime = performance.now();
            
            const response = await this.fetchWithRetry(`${this.baseURL}/channels/${channelId}/messages/${messageId}`, {
                method: 'DELETE',
                headers: this.defaultHeaders,
                signal
            });

            const totalTime = performance.now() - startTime;
            this.performance.deleteMessageTimes.push(totalTime);

            // Discord 删除成功返回 204 No Content
            if (response.status === 204 || response.ok) {
                // 清除缓存
                if (this.cache) {
                    const cacheKey = `message:${channelId}:${messageId}`;
                    this.cache.delete(cacheKey);
                }
                return true;
            }

            console.error('Discord deleteMessage error:', response.status, response.statusText);
            return false;
        } catch (error) {
            console.error('Error deleting Discord message:', error.message);
            return false;
        }
    }

    /**
     * 批量删除消息
     * @param {string} channelId - 频道 ID
     * @param {Array} messageIds - 消息 ID 数组
     * @param {Object} [options={}] - 选项
     * @param {number} [options.concurrency=5] - 并发删除数量
     * @param {AbortSignal} [options.signal=null] - 取消信号
     * @returns {Promise<Array>} 删除结果数组
     */
    async batchDeleteMessages(channelId, messageIds, options = {}) {
        try {
            const { concurrency = 5, signal = null } = options;

            // 参数验证
            if (!channelId || typeof channelId !== 'string') {
                throw new Error('Invalid channel ID');
            }
            if (!Array.isArray(messageIds) || messageIds.length === 0) {
                throw new Error('Invalid message IDs array');
            }

            const results = [];
            const executing = [];

            for (const messageId of messageIds) {
                const deleteMessage = async () => {
                    try {
                        const success = await this.deleteMessage(channelId, messageId, { signal });
                        return {
                            messageId,
                            success,
                            error: null
                        };
                    } catch (error) {
                        return {
                            messageId,
                            success: false,
                            error: error.message
                        };
                    }
                };

                const promise = deleteMessage();
                results.push(promise);
                executing.push(promise);
                
                if (executing.length >= concurrency) {
                    await Promise.race(executing);
                    executing.filter(p => p !== promise);
                }
            }

            return await Promise.all(results);
        } catch (error) {
            console.error('Error batch deleting Discord messages:', error.message);
            throw error;
        }
    }

    /**
     * 获取 Discord 最大文件大小限制
     * 免费用户: 8MB, Nitro 经典: 50MB, Nitro: 100MB
     * @returns {number} 最大文件大小（字节）
     */
    getMaxFileSize() {
        // 默认返回免费用户限制
        return 8 * 1024 * 1024; // 8MB
    }

    /**
     * 获取性能统计信息
     * @returns {Object} 性能统计
     */
    getPerformanceStats() {
        const calculateStats = (times) => {
            if (times.length === 0) {
                return { count: 0, avg: 0, min: 0, max: 0 };
            }
            
            return {
                count: times.length,
                avg: times.reduce((a, b) => a + b, 0) / times.length,
                min: Math.min(...times),
                max: Math.max(...times)
            };
        };

        return {
            upload: calculateStats(this.performance.uploadTimes),
            getMessage: calculateStats(this.performance.getMessageTimes),
            deleteMessage: calculateStats(this.performance.deleteMessageTimes)
        };
    }

    /**
     * 清除缓存
     */
    clearCache() {
        if (this.cache) {
            this.cache.clear();
        }
    }

    /**
     * 添加进度跟踪到请求
     * @param {Request} request - 请求对象
     * @param {number} [totalSize=null] - 总大小
     * @param {Function} progressCallback - 进度回调函数
     * @returns {Request} 带有进度跟踪的请求
     */
    addProgressTracking(request, totalSize = null, progressCallback) {
        if (!progressCallback || typeof progressCallback !== 'function') {
            return request;
        }

        // 创建一个新的 ReadableStream 来跟踪上传/下载进度
        const { body } = request;
        if (!body) {
            return request;
        }

        let loaded = 0;
        const reader = body.getReader();
        
        const stream = new ReadableStream({
            async start(controller) {
                while (true) {
                    const { done, value } = await reader.read();
                    
                    if (done) {
                        controller.close();
                        progressCallback(1); // 完成
                        break;
                    }
                    
                    loaded += value.byteLength;
                    const progress = totalSize ? Math.min(loaded / totalSize, 1) : null;
                    progressCallback(progress, loaded, totalSize);
                    
                    controller.enqueue(value);
                }
            }
        });

        return new Request(request, { body: stream });
    }
}

/**
 * 创建 Discord API 实例的工厂函数
 * @param {Object} config - 配置对象
 * @param {string} config.botToken - Discord 机器人令牌
 * @param {number} [config.timeout=30000] - 请求超时时间（毫秒）
 * @param {number} [config.maxRetries=3] - 最大重试次数
 * @param {number} [config.retryDelay=1000] - 重试延迟（毫秒）
 * @param {boolean} [config.enableCache=true] - 是否启用缓存
 * @param {number} [config.cacheTTL=3600000] - 缓存有效期（毫秒）
 * @returns {DiscordAPI} Discord API 实例
 */
export function createDiscordAPI(config) {
    if (!config || !config.botToken) {
        throw new Error('Missing required configuration: botToken is required');
    }
    
    return new DiscordAPI(config.botToken, {
        timeout: config.timeout || 30000,
        maxRetries: config.maxRetries || 3,
        retryDelay: config.retryDelay || 1000,
        enableCache: config.enableCache !== false,
        cacheTTL: config.cacheTTL || 3600000
    });
}

/**
 * Discord 常量
 */
export const DISCORD_CONSTANTS = {
    MAX_FILE_SIZE_FREE: 8 * 1024 * 1024, // 8MB
    MAX_FILE_SIZE_NITRO_CLASSIC: 50 * 1024 * 1024, // 50MB
    MAX_FILE_SIZE_NITRO: 100 * 1024 * 1024, // 100MB
    API_VERSION: 'v10',
    BASE_URL: 'https://discord.com/api/v10'
};
