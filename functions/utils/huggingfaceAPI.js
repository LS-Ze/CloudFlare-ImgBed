/**
 * Hugging Face Hub API 封装类
 * 手动实现 LFS 上传协议（Cloudflare Workers 兼容）
 * 
 * HuggingFace 要求二进制文件通过 LFS 协议上传
 * 流程：preupload -> LFS batch -> upload to LFS storage -> commit
 * 
 * 优化方案：
 * 1. 小文件（<20MB）：前端 → CF Workers → HuggingFace S3
 * 2. 大文件（>=20MB）：前端直接上传到 HuggingFace S3，CF Workers 只负责获取签名 URL 和提交
 * 
 * SHA256 由前端预计算传入，避免后端 CPU 超时
 */

export class HuggingFaceAPI {
    /**
     * 创建 HuggingFace API 实例
     * @param {string} token - HuggingFace API 令牌
     * @param {string} repo - 仓库名称，格式: username/repo-name
     * @param {Object} [options={}] - 配置选项
     * @param {boolean} [options.isPrivate=false] - 是否为私有仓库
     * @param {number} [options.timeout=30000] - 请求超时时间（毫秒）
     * @param {number} [options.maxRetries=3] - 最大重试次数
     * @param {number} [options.retryDelay=1000] - 重试延迟（毫秒）
     */
    constructor(token, repo, options = {}) {
        // 参数验证
        if (!token || typeof token !== 'string') {
            throw new Error('Invalid token');
        }
        if (!repo || typeof repo !== 'string' || !repo.includes('/')) {
            throw new Error('Invalid repo format. Expected: username/repo-name');
        }

        this.token = token;
        this.repo = repo;
        this.options = {
            isPrivate: false,
            timeout: 30000,
            maxRetries: 3,
            retryDelay: 1000,
            ...options
        };
        
        this.baseURL = 'https://huggingface.co';
        this.cache = new Map(); // 简单缓存
        
        // 初始化性能监控
        this.performance = {
            uploadTimes: [],
            commitTimes: [],
            preuploadTimes: [],
            lfsBatchTimes: []
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
            
            if (!response.ok) {
                const errorText = await response.text().catch(() => 'Unknown error');
                throw new Error(`HTTP error! status: ${response.status}, message: ${errorText}`);
            }
            
            return response;
            
        } catch (error) {
            if (error.name === 'AbortError') {
                throw new Error(`Request timed out after ${this.options.timeout}ms`);
            }
            
            // 重试逻辑
            if (retries < this.options.maxRetries && this.isRetryableError(error)) {
                const delay = this.options.retryDelay * Math.pow(2, retries); // 指数退避
                console.log(`Retrying request (${retries + 1}/${this.options.maxRetries}) after ${delay}ms: ${error.message}`);
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
     * 计算文件的 SHA256 哈希（仅在未提供预计算哈希时使用）
     * @param {Blob} blob - 文件 Blob
     * @returns {Promise<string>} hex string
     */
    async sha256(blob) {
        try {
            const cacheKey = `sha256:${blob.size}:${blob.type}`;
            if (this.cache.has(cacheKey)) {
                return this.cache.get(cacheKey);
            }
            
            const buffer = await blob.arrayBuffer();
            const hashBuffer = await crypto.subtle.digest('SHA-256', buffer);
            const hashArray = Array.from(new Uint8Array(hashBuffer));
            const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
            
            this.cache.set(cacheKey, hashHex);
            return hashHex;
        } catch (error) {
            console.error('SHA256 calculation error:', error);
            throw new Error(`Failed to calculate SHA256: ${error.message}`);
        }
    }

    /**
     * 检查仓库是否存在
     * @returns {Promise<boolean>} 仓库是否存在
     */
    async repoExists() {
        try {
            const cacheKey = `repoExists:${this.repo}`;
            if (this.cache.has(cacheKey)) {
                return this.cache.get(cacheKey);
            }
            
            const response = await this.fetchWithRetry(`${this.baseURL}/api/datasets/${this.repo}`, {
                headers: { 'Authorization': `Bearer ${this.token}` }
            });
            
            const exists = response.ok;
            this.cache.set(cacheKey, exists, 3600000); // 缓存1小时
            return exists;
        } catch (error) {
            console.error('Error checking repo:', error.message);
            return false;
        }
    }

    /**
     * 创建仓库（如果不存在）
     * @returns {Promise<boolean>} 是否成功创建或仓库已存在
     */
    async createRepoIfNotExists() {
        try {
            if (await this.repoExists()) {
                console.log('Repository exists:', this.repo);
                return true;
            }

            console.log('Creating repository:', this.repo);
            const [username, repoName] = this.repo.split('/');
            
            const response = await this.fetchWithRetry(`${this.baseURL}/api/repos/create`, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${this.token}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    name: repoName,
                    type: 'dataset',
                    private: this.options.isPrivate
                })
            });

            if (response.ok || response.status === 409) {
                console.log('Repository ready');
                this.cache.set(`repoExists:${this.repo}`, true); // 更新缓存
                return true;
            }
            
            const errorText = await response.text();
            throw new Error(`Failed to create repo: ${response.status} - ${errorText}`);
        } catch (error) {
            console.error('Error creating repo:', error.message);
            return false;
        }
    }

    /**
     * 步骤1: Preupload - 检查文件是否需要 LFS
     * @param {string} filePath - 文件路径
     * @param {number} fileSize - 文件大小
     * @param {string} fileSample - 文件前512字节的 base64
     * @returns {Promise<Object>} preupload 结果
     */
    async preupload(filePath, fileSize, fileSample) {
        try {
            const startTime = performance.now();
            
            const url = `${this.baseURL}/api/datasets/${this.repo}/preupload/main`;
            const response = await this.fetchWithRetry(url, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${this.token}`,
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    files: [{
                        path: filePath,
                        size: fileSize,
                        sample: fileSample
                    }]
                })
            });

            const result = await response.json();
            this.performance.preuploadTimes.push(performance.now() - startTime);
            
            return result;
        } catch (error) {
            console.error('Preupload error:', error.message);
            throw new Error(`Preupload failed: ${error.message}`);
        }
    }

    /**
     * 步骤2: LFS Batch - 获取上传 URL
     * @param {string} oid - 文件的 SHA256 哈希
     * @param {number} fileSize - 文件大小
     * @returns {Promise<Object>} LFS batch 结果
     */
    async lfsBatch(oid, fileSize) {
        try {
            const startTime = performance.now();
            
            const url = `${this.baseURL}/datasets/${this.repo}.git/info/lfs/objects/batch`;
            const response = await this.fetchWithRetry(url, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${this.token}`,
                    'Accept': 'application/vnd.git-lfs+json',
                    'Content-Type': 'application/vnd.git-lfs+json'
                },
                body: JSON.stringify({
                    operation: 'upload',
                    transfers: ['basic', 'multipart'],
                    hash_algo: 'sha_256',
                    ref: { name: 'main' },
                    objects: [{ oid, size: fileSize }]
                })
            });

            const result = await response.json();
            this.performance.lfsBatchTimes.push(performance.now() - startTime);
            
            return result;
        } catch (error) {
            console.error('LFS batch error:', error.message);
            throw new Error(`LFS batch failed: ${error.message}`);
        }
    }

    /**
     * 步骤3: 上传文件到 LFS 存储
     * @param {Object} uploadAction - 上传动作信息
     * @param {File|Blob} file - 文件
     * @param {string} oid - 文件的 SHA256 哈希
     * @param {Function} [progressCallback=null] - 进度回调函数
     * @param {AbortSignal} [signal=null] - 取消信号
     * @returns {Promise<boolean>} 是否上传成功
     */
    async uploadToLFS(uploadAction, file, oid, progressCallback = null, signal = null) {
        try {
            const { href, header } = uploadAction;

            // 检查是否是分片上传
            if (header?.chunk_size) {
                return await this.uploadMultipart(uploadAction, file, oid, progressCallback, signal);
            }

            // 基本上传
            console.log('Uploading to LFS (basic):', href);
            
            // 创建上传请求
            const uploadRequest = new Request(href, {
                method: 'PUT',
                headers: header || {},
                body: file
            });

            // 添加进度跟踪
            const progressRequest = this.addProgressTracking(uploadRequest, file.size, progressCallback);
            
            // 执行上传
            const response = await this.fetchWithRetry(href, {
                method: 'PUT',
                headers: header || {},
                body: progressRequest.body,
                signal
            });

            if (!response.ok) {
                const error = await response.text();
                throw new Error(`LFS upload failed: ${response.status} - ${error}`);
            }

            return true;
        } catch (error) {
            console.error('LFS upload error:', error.message);
            throw new Error(`LFS upload failed: ${error.message}`);
        }
    }

    /**
     * 分片上传（大文件）
     * @param {Object} uploadAction - 上传动作信息
     * @param {File|Blob} file - 文件
     * @param {string} oid - 文件的 SHA256 哈希
     * @param {Function} [progressCallback=null] - 进度回调函数
     * @param {AbortSignal} [signal=null] - 取消信号
     * @returns {Promise<boolean>} 是否上传成功
     */
    async uploadMultipart(uploadAction, file, oid, progressCallback = null, signal = null) {
        try {
            const { href: completionUrl, header } = uploadAction;
            const chunkSize = parseInt(header.chunk_size);
            
            // 获取所有分片的上传 URL
            const parts = Object.keys(header).filter(key => /^[0-9]+$/.test(key));
            console.log(`Multipart upload: ${parts.length} parts, chunk size: ${chunkSize}`);

            const completeParts = [];
            let totalUploaded = 0;

            // 并发上传分片（限制并发数）
            const concurrency = 5; // 限制并发上传数量
            const results = [];
            const executing = [];

            for (const part of parts) {
                const uploadPart = async () => {
                    const index = parseInt(part) - 1;
                    const start = index * chunkSize;
                    const end = Math.min(start + chunkSize, file.size);
                    const chunk = file.slice(start, end);
                    
                    console.log(`Uploading part ${part}/${parts.length}`);
                    
                    // 创建上传请求
                    const uploadRequest = new Request(header[part], {
                        method: 'PUT',
                        body: chunk
                    });

                    // 添加进度跟踪
                    const progressRequest = this.addProgressTracking(uploadRequest, chunk.size, (progress) => {
                        if (progressCallback) {
                            const partProgress = totalUploaded + (progress * chunk.size);
                            const overallProgress = partProgress / file.size;
                            progressCallback(overallProgress);
                        }
                    });
                    
                    // 执行上传
                    const response = await this.fetchWithRetry(header[part], {
                        method: 'PUT',
                        body: progressRequest.body,
                        signal
                    });

                    if (!response.ok) {
                        throw new Error(`Failed to upload part ${part}: ${response.status}`);
                    }

                    const etag = response.headers.get('ETag');
                    if (!etag) {
                        throw new Error(`No ETag for part ${part}`);
                    }

                    totalUploaded += chunk.size;
                    return { partNumber: parseInt(part), etag };
                };

                const promise = uploadPart().then(result => {
                    completeParts.push(result);
                    return result;
                });
                
                results.push(promise);
                executing.push(promise);
                
                if (executing.length >= concurrency) {
                    await Promise.race(executing);
                    executing.filter(p => p !== promise);
                }
            }

            // 等待所有上传完成
            await Promise.all(results);

            // 完成分片上传
            console.log('Completing multipart upload...');
            const completeResponse = await this.fetchWithRetry(completionUrl, {
                method: 'POST',
                headers: {
                    'Accept': 'application/vnd.git-lfs+json',
                    'Content-Type': 'application/vnd.git-lfs+json'
                },
                body: JSON.stringify({
                    oid: oid,
                    parts: completeParts
                }),
                signal
            });

            if (!completeResponse.ok) {
                const error = await completeResponse.text();
                throw new Error(`Multipart complete failed: ${completeResponse.status} - ${error}`);
            }

            return true;
        } catch (error) {
            console.error('Multipart upload error:', error.message);
            throw new Error(`Multipart upload failed: ${error.message}`);
        }
    }

    /**
     * 步骤4: 提交 LFS 文件引用
     * @param {string} filePath - 文件路径
     * @param {string} oid - 文件的 SHA256 哈希
     * @param {number} fileSize - 文件大小
     * @param {string} commitMessage - 提交信息
     * @returns {Promise<Object>} 提交结果
     */
    async commitLfsFile(filePath, oid, fileSize, commitMessage) {
        try {
            const startTime = performance.now();
            
            const url = `${this.baseURL}/api/datasets/${this.repo}/commit/main`;
            
            // NDJSON 格式
            const body = [
                JSON.stringify({
                    key: 'header',
                    value: { summary: commitMessage }
                }),
                JSON.stringify({
                    key: 'lfsFile',
                    value: {
                        path: filePath,
                        algo: 'sha256',
                        size: fileSize,
                        oid: oid
                    }
                })
            ].join('\n');

            const response = await this.fetchWithRetry(url, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${this.token}`,
                    'Content-Type': 'application/x-ndjson'
                },
                body
            });

            const result = await response.json();
            this.performance.commitTimes.push(performance.now() - startTime);
            
            return result;
        } catch (error) {
            console.error('Commit error:', error.message);
            throw new Error(`Commit failed: ${error.message}`);
        }
    }

    /**
     * 获取 LFS 上传信息（用于前端直传大文件）
     * 返回上传 URL 和必要的信息，让前端直接上传到 S3
     * @param {number} fileSize - 文件大小
     * @param {string} filePath - 存储路径
     * @param {string} sha256 - 文件的 SHA256 哈希
     * @param {string} fileSample - 文件前512字节的 base64
     * @returns {Promise<Object>} 上传信息
     */
    async getLfsUploadInfo(fileSize, filePath, sha256, fileSample) {
        try {
            // 参数验证
            if (!fileSize || typeof fileSize !== 'number' || fileSize <= 0) {
                throw new Error('Invalid file size');
            }
            if (!filePath || typeof filePath !== 'string') {
                throw new Error('Invalid file path');
            }
            if (!sha256 || typeof sha256 !== 'string' || sha256.length !== 64) {
                throw new Error('Invalid SHA256 hash');
            }

            // 确保仓库存在
            if (!await this.createRepoIfNotExists()) {
                throw new Error('Failed to create or access repository');
            }

            // 1. Preupload 检查
            console.log('Preupload check for direct upload...');
            const preuploadResult = await this.preupload(filePath, fileSize, fileSample);
            console.log('Preupload result:', JSON.stringify(preuploadResult));

            const fileInfo = preuploadResult.files?.[0];
            const needsLfs = fileInfo?.uploadMode === 'lfs';

            if (!needsLfs) {
                // 小文件不需要 LFS，返回 null 让后端处理
                return { needsLfs: false };
            }

            // 2. LFS Batch - 获取上传 URL
            console.log('LFS batch request for direct upload...');
            const batchResult = await this.lfsBatch(sha256, fileSize);
            console.log('LFS batch result:', JSON.stringify(batchResult));

            const obj = batchResult.objects?.[0];
            if (obj?.error) {
                throw new Error(`LFS error: ${obj.error.message}`);
            }

            // 检查文件是否已存在
            if (!obj?.actions?.upload) {
                return {
                    needsLfs: true,
                    alreadyExists: true,
                    oid: sha256,
                    filePath
                };
            }

            // 返回上传信息
            return {
                needsLfs: true,
                alreadyExists: false,
                oid: sha256,
                filePath,
                uploadAction: obj.actions.upload,
                uploadUrl: obj.actions.upload.href,
                headers: obj.actions.upload.header || {}
            };
        } catch (error) {
            console.error('Get LFS upload info error:', error.message);
            throw new Error(`Failed to get LFS upload info: ${error.message}`);
        }
    }

    /**
     * 上传文件（完整流程）- 用于小文件或后端代理上传
     * @param {File|Blob} file - 要上传的文件
     * @param {string} filePath - 存储路径
     * @param {Object} [options={}] - 上传选项
     * @param {string} [options.commitMessage='Upload file'] - 提交信息
     * @param {string} [options.precomputedSha256=null] - 前端预计算的 SHA256
     * @param {Function} [options.progressCallback=null] - 进度回调函数
     * @param {AbortSignal} [options.signal=null] - 取消信号
     * @returns {Promise<Object>} 上传结果
     */
    async uploadFile(file, filePath, options = {}) {
        try {
            const {
                commitMessage = 'Upload file',
                precomputedSha256 = null,
                progressCallback = null,
                signal = null
            } = options;

            // 参数验证
            if (!file || !(file instanceof Blob)) {
                throw new Error('Invalid file');
            }
            if (!filePath || typeof filePath !== 'string') {
                throw new Error('Invalid file path');
            }

            // 确保仓库存在
            if (!await this.createRepoIfNotExists()) {
                throw new Error('Failed to create or access repository');
            }

            console.log('=== HuggingFace LFS Upload ===');
            console.log('Repo:', this.repo);
            console.log('Path:', filePath);
            console.log('Size:', file.size);

            const startTime = performance.now();

            // 1. 使用预计算的 SHA256 或在后端计算
            let oid;
            if (precomputedSha256) {
                console.log('Using precomputed SHA256:', precomputedSha256);
                oid = precomputedSha256;
            } else {
                console.log('Computing SHA256 on server (may timeout for large files)...');
                oid = await this.sha256(file);
                console.log('SHA256:', oid);
            }

            // 2. 获取文件样本（前512字节的base64）
            const sampleBytes = new Uint8Array(await file.slice(0, 512).arrayBuffer());
            const sample = btoa(String.fromCharCode(...sampleBytes));

            // 3. Preupload 检查
            console.log('Preupload check...');
            const preuploadResult = await this.preupload(filePath, file.size, sample);
            console.log('Preupload result:', JSON.stringify(preuploadResult));

            const fileInfo = preuploadResult.files?.[0];
            const needsLfs = fileInfo?.uploadMode === 'lfs';
            console.log('Needs LFS:', needsLfs);

            if (needsLfs) {
                // 4. LFS Batch - 获取上传 URL
                console.log('LFS batch request...');
                const batchResult = await this.lfsBatch(oid, file.size);
                console.log('LFS batch result:', JSON.stringify(batchResult));

                const obj = batchResult.objects?.[0];
                if (obj?.error) {
                    throw new Error(`LFS error: ${obj.error.message}`);
                }

                // 5. 上传到 LFS 存储（如果需要）
                if (obj?.actions?.upload) {
                    console.log('Uploading to LFS storage...');
                    await this.uploadToLFS(obj.actions.upload, file, oid, progressCallback, signal);
                    console.log('LFS upload complete');
                } else {
                    console.log('File already exists in LFS');
                }

                // 6. 提交 LFS 文件引用
                console.log('Committing LFS file...');
                const commitResult = await this.commitLfsFile(filePath, oid, file.size, commitMessage);
                console.log('Commit result:', JSON.stringify(commitResult));
            } else {
                // 非 LFS 文件：直接 base64 提交（小文本文件）
                console.log('Direct commit (non-LFS)...');
                await this.commitDirectFile(filePath, file, commitMessage, progressCallback, signal);
            }

            const totalTime = performance.now() - startTime;
            this.performance.uploadTimes.push(totalTime);
            
            const fileUrl = `${this.baseURL}/datasets/${this.repo}/resolve/main/${filePath}`;
            return {
                success: true,
                filePath,
                fileUrl,
                fileSize: file.size,
                oid,
                uploadTime: totalTime
            };

        } catch (error) {
            console.error('HuggingFace upload error:', error.message);
            throw error;
        }
    }

    /**
     * 直接提交文件（非 LFS，用于小文本文件）
     * @param {string} filePath - 文件路径
     * @param {File|Blob} file - 文件
     * @param {string} commitMessage - 提交信息
     * @param {Function} [progressCallback=null] - 进度回调函数
     * @param {AbortSignal} [signal=null] - 取消信号
     * @returns {Promise<Object>} 提交结果
     */
    async commitDirectFile(filePath, file, commitMessage, progressCallback = null, signal = null) {
        try {
            const startTime = performance.now();
            
            const url = `${this.baseURL}/api/datasets/${this.repo}/commit/main`;
            
            // 读取文件内容并转换为 base64
            const content = await this.readFileAsBase64(file, progressCallback);
            
            const body = [
                JSON.stringify({
                    key: 'header',
                    value: { summary: commitMessage }
                }),
                JSON.stringify({
                    key: 'file',
                    value: {
                        path: filePath,
                        content: content,
                        encoding: 'base64'
                    }
                })
            ].join('\n');

            const response = await this.fetchWithRetry(url, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${this.token}`,
                    'Content-Type': 'application/x-ndjson'
                },
                body,
                signal
            });

            const result = await response.json();
            this.performance.commitTimes.push(performance.now() - startTime);
            
            return result;
        } catch (error) {
            console.error('Direct commit error:', error.message);
            throw new Error(`Direct commit failed: ${error.message}`);
        }
    }

    /**
     * 删除文件
     * @param {string} filePath - 文件路径
     * @param {string} [commitMessage='Delete file'] - 提交信息
     * @returns {Promise<boolean>} 是否删除成功
     */
    async deleteFile(filePath, commitMessage = 'Delete file') {
        try {
            const url = `${this.baseURL}/api/datasets/${this.repo}/commit/main`;
            
            const body = [
                JSON.stringify({
                    key: 'header',
                    value: { summary: commitMessage }
                }),
                JSON.stringify({
                    key: 'deletedFile',
                    value: { path: filePath }
                })
            ].join('\n');

            const response = await this.fetchWithRetry(url, {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${this.token}`,
                    'Content-Type': 'application/x-ndjson'
                },
                body
            });

            return response.ok;
        } catch (error) {
            console.error('Delete file error:', error.message);
            return false;
        }
    }

    /**
     * 获取文件内容（用于私有仓库代理）
     * @param {string} filePath - 文件路径
     * @returns {Promise<Response>} 文件响应
     */
    async getFileContent(filePath) {
        try {
            const fileUrl = `${this.baseURL}/datasets/${this.repo}/resolve/main/${filePath}`;
            return await this.fetchWithRetry(fileUrl, {
                headers: this.options.isPrivate ? { 'Authorization': `Bearer ${this.token}` } : {}
            });
        } catch (error) {
            console.error('Get file content error:', error.message);
            throw error;
        }
    }

    /**
     * 获取文件 URL
     * @param {string} filePath - 文件路径
     * @returns {string} 文件 URL
     */
    getFileURL(filePath) {
        return `${this.baseURL}/datasets/${this.repo}/resolve/main/${filePath}`;
    }

    /**
     * 批量上传文件
     * @param {Array} files - 文件数组，每个元素包含 { file, filePath, options }
     * @param {number} [concurrency=3] - 并发上传数量
     * @returns {Promise<Array>} 上传结果数组
     */
    async batchUploadFiles(files, concurrency = 3) {
        try {
            if (!Array.isArray(files) || files.length === 0) {
                throw new Error('Invalid files array');
            }

            const results = [];
            const executing = [];

            for (const fileInfo of files) {
                const { file, filePath, options = {} } = fileInfo;
                
                const uploadFile = async () => {
                    try {
                        const result = await this.uploadFile(file, filePath, options);
                        return { ...result, success: true };
                    } catch (error) {
                        return {
                            success: false,
                            filePath,
                            error: error.message
                        };
                    }
                };

                const promise = uploadFile();
                results.push(promise);
                executing.push(promise);
                
                if (executing.length >= concurrency) {
                    await Promise.race(executing);
                    executing.filter(p => p !== promise);
                }
            }

            return await Promise.all(results);
        } catch (error) {
            console.error('Batch upload error:', error.message);
            throw error;
        }
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
            commit: calculateStats(this.performance.commitTimes),
            preupload: calculateStats(this.performance.preuploadTimes),
            lfsBatch: calculateStats(this.performance.lfsBatchTimes)
        };
    }

    /**
     * 清除缓存
     */
    clearCache() {
        this.cache.clear();
    }

    /**
     * 添加进度跟踪到请求
     * @param {Request} request - 请求对象
     * @param {number} totalSize - 总大小
     * @param {Function} progressCallback - 进度回调函数
     * @returns {Request} 带有进度跟踪的请求
     */
    addProgressTracking(request, totalSize, progressCallback) {
        if (!progressCallback || typeof progressCallback !== 'function') {
            return request;
        }

        // 创建一个新的 ReadableStream 来跟踪上传进度
        const { body } = request;
        if (!body) {
            return request;
        }

        let uploaded = 0;
        const reader = body.getReader();
        
        const stream = new ReadableStream({
            async start(controller) {
                while (true) {
                    const { done, value } = await reader.read();
                    
                    if (done) {
                        controller.close();
                        break;
                    }
                    
                    uploaded += value.byteLength;
                    const progress = Math.min(uploaded / totalSize, 1);
                    progressCallback(progress);
                    
                    controller.enqueue(value);
                }
            }
        });

        return new Request(request, { body: stream });
    }

    /**
     * 读取文件为 base64
     * @param {File|Blob} file - 文件
     * @param {Function} [progressCallback=null] - 进度回调函数
     * @returns {Promise<string>} base64 字符串
     */
    async readFileAsBase64(file, progressCallback = null) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            
            reader.onload = (event) => {
                const base64 = event.target.result.split(',')[1];
                resolve(base64);
            };
            
            reader.onerror = (error) => {
                reject(new Error(`Failed to read file: ${error.message}`));
            };
            
            if (progressCallback) {
                reader.onprogress = (event) => {
                    if (event.lengthComputable) {
                        const progress = event.loaded / event.total;
                        progressCallback(progress);
                    }
                };
            }
            
            reader.readAsDataURL(file);
        });
    }
}

/**
 * 创建 HuggingFace API 实例的工厂函数
 * @param {Object} config - 配置对象
 * @param {string} config.token - HuggingFace API 令牌
 * @param {string} config.repo - 仓库名称
 * @param {boolean} [config.isPrivate=false] - 是否为私有仓库
 * @returns {HuggingFaceAPI} HuggingFace API 实例
 */
export function createHuggingFaceAPI(config) {
    if (!config || !config.token || !config.repo) {
        throw new Error('Missing required configuration: token and repo are required');
    }
    
    return new HuggingFaceAPI(config.token, config.repo, {
        isPrivate: config.isPrivate || false,
        timeout: config.timeout || 30000,
        maxRetries: config.maxRetries || 3,
        retryDelay: config.retryDelay || 1000
    });
}

/**
 * 上传配置默认值
 */
export const DEFAULT_UPLOAD_OPTIONS = {
    commitMessage: 'Upload file',
    precomputedSha256: null,
    progressCallback: null,
    signal: null
};

/**
 * LFS 常量
 */
export const LFS_CONSTANTS = {
    LFS_THRESHOLD: 20 * 1024 * 1024, // 20MB LFS 阈值
    SAMPLE_SIZE: 512, // 文件样本大小（字节）
    MAX_CONCURRENCY: 5, // 最大并发上传数
    CHUNK_SIZE: 5 * 1024 * 1024 // 默认分片大小（5MB）
};
