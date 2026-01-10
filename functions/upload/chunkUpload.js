/* ======= 客户端分块上传处理 ======= */
import { createResponse, selectConsistentChannel, getUploadIp, getIPAddress, buildUniqueFileId, endUpload } from './uploadTools';
import { TelegramAPI } from '../utils/telegramAPI';
import { DiscordAPI } from '../utils/discordAPI';
import { S3Client, CreateMultipartUploadCommand, UploadPartCommand, AbortMultipartUploadCommand } from "@aws-sdk/client-s3";
import { getDatabase } from '../utils/databaseAdapter.js';

// 缓存配置
const CACHE_CONFIG = {
    UPLOAD_SESSION: { ttl: 60, maxSize: 1000 }, // 60秒缓存，最多1000个会话
    CHUNK_STATUS: { ttl: 30, maxSize: 1000 }    // 30秒缓存，最多1000个上传ID
};

// 内存缓存
const memoryCache = new Map();

// 统一错误类
export class UploadError extends Error {
    constructor(message, code = 'UPLOAD_ERROR', status = 500, details = {}) {
        super(message);
        this.name = 'UploadError';
        this.code = code;
        this.status = status;
        this.details = details;
        this.timestamp = Date.now();
    }
}

// 渠道处理函数映射
const ChannelUploaders = {
    cfr2: uploadSingleChunkToR2Multipart,
    s3: uploadSingleChunkToS3Multipart,
    telegram: uploadSingleChunkToTelegram,
    discord: uploadSingleChunkToDiscord
};

// 初始化分块上传
export async function initializeChunkedUpload(context) {
    const { request, env, url } = context;
    const db = getDatabase(env);

    try {
        // 解析表单数据
        const formdata = await parseFormDataWithValidation(request);

        const originalFileName = formdata.get('originalFileName');
        const originalFileType = formdata.get('originalFileType');
        const totalChunks = parseInt(formdata.get('totalChunks'));

        // 验证参数
        validateInitializationParameters(originalFileName, originalFileType, totalChunks);

        // 生成唯一的 uploadId
        const uploadId = generateUploadId();

        // 获取上传IP
        const uploadIp = getUploadIp(request);
        const ipAddress = await getIPAddressWithCache(uploadIp);

        // 获取上传渠道
        const uploadChannel = url.searchParams.get('uploadChannel') || 'telegram';
        // 获取指定的渠道名称
        const channelName = url.searchParams.get('channelName') || '';

        // 存储上传会话信息
        const sessionInfo = {
            uploadId,
            originalFileName,
            originalFileType,
            totalChunks,
            uploadChannel,
            channelName,
            uploadIp,
            ipAddress,
            status: 'initialized',
            createdAt: Date.now(),
            expiresAt: Date.now() + 3600000 // 1小时过期
        };

        // 保存会话信息
        const sessionKey = `upload_session_${uploadId}`;
        await db.put(sessionKey, JSON.stringify(sessionInfo), {
            expirationTtl: 3600 // 1小时过期
        });

        // 缓存会话信息
        cacheSet('UPLOAD_SESSION', uploadId, JSON.stringify(sessionInfo));

        return createResponse(JSON.stringify({
            success: true,
            uploadId,
            message: 'Chunked upload initialized successfully',
            sessionInfo: {
                uploadId,
                originalFileName,
                totalChunks,
                uploadChannel,
                channelName
            }
        }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' }
        });

    } catch (error) {
        const errorId = await logUploadError(env, error);
        return createResponse(`Error: Failed to initialize chunked upload - ${error.message} (Error ID: ${errorId})`, { 
            status: error.status || 500 
        });
    }
}

// 处理客户端分块上传
export async function handleChunkUpload(context) {
    const { env, request, url, waitUntil } = context;
    const db = getDatabase(env);

    try {
        // 解析表单数据
        const formdata = await parseFormDataWithValidation(request);
        context.formdata = formdata;

        // 验证分块上传参数
        const { chunk, chunkIndex, totalChunks, uploadId, originalFileName, originalFileType } = 
            await validateChunkUploadParameters(formdata);

        // 验证上传会话
        const sessionInfo = await validateUploadSession(env, uploadId, originalFileName, totalChunks);

        // 获取上传渠道
        const uploadChannel = url.searchParams.get('uploadChannel') || sessionInfo.uploadChannel || 'telegram';
        // 获取指定的渠道名称
        const channelName = url.searchParams.get('channelName') || sessionInfo.channelName || '';

        // 将渠道名称存入 context
        context.specifiedChannelName = channelName;

        // 立即创建分块记录，标记为"uploading"状态
        const chunkKey = `chunk_${uploadId}_${chunkIndex.toString().padStart(3, '0')}`;
        const chunkData = await chunk.arrayBuffer();
        const uploadStartTime = Date.now();
        
        // 创建分块元数据
        const initialChunkMetadata = createChunkMetadata(
            uploadId, chunkIndex, totalChunks, originalFileName, 
            originalFileType, chunkData.byteLength, uploadStartTime, uploadChannel
        );

        // 立即保存分块记录和数据，设置过期时间
        await db.put(chunkKey, chunkData, {
            metadata: initialChunkMetadata,
            expirationTtl: 3600 // 1小时过期
        });

        // 异步上传分块到存储端，添加超时保护
        waitUntil(uploadChunkToStorageWithTimeout(
            context, chunkIndex, totalChunks, uploadId, 
            originalFileName, originalFileType, uploadChannel
        ));

        return createResponse(JSON.stringify({
            success: true,
            message: `Chunk ${chunkIndex + 1}/${totalChunks} received and being uploaded`,
            uploadId,
            chunkIndex
        }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' }
        });

    } catch (error) {
        const errorId = await logUploadError(env, error, {
            uploadId: context.uploadId,
            chunkIndex: context.chunkIndex
        });
        return createResponse(`Error: Failed to upload chunk - ${error.message} (Error ID: ${errorId})`, { 
            status: error.status || 500 
        });
    }
}

// 处理清理请求
export async function handleCleanupRequest(context, uploadId, totalChunks) {
    try {
        if (!uploadId) {
            throw new UploadError('Missing uploadId parameter', 'MISSING_PARAMETER', 400);
        }

        // 强制清理所有相关数据
        await forceCleanupUpload(context, uploadId, totalChunks);

        return createResponse(JSON.stringify({
            success: true,
            message: `Cleanup completed for upload ${uploadId}`,
            uploadId: uploadId,
            cleanedChunks: totalChunks
        }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' }
        });

    } catch (error) {
        const errorId = await logUploadError(context.env, error, { uploadId });
        return createResponse(JSON.stringify({
            error: `Cleanup failed: ${error.message}`,
            errorId,
            uploadId: uploadId
        }), { status: error.status || 500, headers: { 'Content-Type': 'application/json' } });
    }
}

/* ======= 单个分块上传到不同渠道的存储端 ======= */

// 带超时保护的异步上传分块到存储端
async function uploadChunkToStorageWithTimeout(context, chunkIndex, totalChunks, uploadId, originalFileName, originalFileType, uploadChannel) {
    const { env } = context;
    const db = getDatabase(env);
    const chunkKey = `chunk_${uploadId}_${chunkIndex.toString().padStart(3, '0')}`;
    const UPLOAD_TIMEOUT = 180000; // 3分钟超时

    try {
        // 设置超时 Promise
        const timeoutPromise = new Promise((_, reject) => {
            setTimeout(() => reject(new UploadError('Upload timeout', 'UPLOAD_TIMEOUT', 408)), UPLOAD_TIMEOUT);
        });

        // 执行实际上传
        const uploadPromise = uploadChunkToStorage(
            context, chunkIndex, totalChunks, uploadId, 
            originalFileName, originalFileType, uploadChannel
        );

        // 竞速执行
        await Promise.race([uploadPromise, timeoutPromise]);

    } catch (error) {
        console.error(`Chunk ${chunkIndex} upload failed or timed out:`, error.message);

        // 超时或失败时，更新状态为超时/失败
        try {
            await updateChunkStatusOnError(db, chunkKey, error);
        } catch (metaError) {
            console.error('Failed to save timeout/error metadata:', metaError);
        }
    }
}

// 异步上传分块到存储端，失败自动重试
async function uploadChunkToStorage(context, chunkIndex, totalChunks, uploadId, originalFileName, originalFileType, uploadChannel) {
    const { env } = context;
    const db = getDatabase(env);

    const chunkKey = `chunk_${uploadId}_${chunkIndex.toString().padStart(3, '0')}`;
    const MAX_RETRIES = 3;

    try {
        // 从数据库分块数据和metadata
        const chunkRecord = await db.getWithMetadata(chunkKey, { type: 'arrayBuffer' });
        if (!chunkRecord || !chunkRecord.value) {
            throw new UploadError(`Chunk ${chunkIndex} data not found in database`, 'CHUNK_DATA_MISSING');
        }

        const chunkData = chunkRecord.value;
        const chunkMetadata = chunkRecord.metadata;

        // 使用策略模式获取上传处理器
        const uploadHandler = ChannelUploaders[uploadChannel];
        if (!uploadHandler) {
            throw new UploadError(`Unsupported upload channel: ${uploadChannel}`, 'UNSUPPORTED_CHANNEL', 400);
        }

        // 重试逻辑
        for (let retry = 0; retry < MAX_RETRIES; retry++) {
            try {
                // 执行上传
                const uploadResult = await uploadHandler(
                    context, chunkData, chunkIndex, totalChunks, 
                    uploadId, originalFileName, originalFileType
                );

                if (uploadResult && uploadResult.success) {
                    // 上传成功，更新状态并保存上传信息
                    const updatedMetadata = {
                        ...chunkMetadata,
                        status: 'completed',
                        uploadResult: uploadResult,
                        completedTime: Date.now(),
                        retryCount: retry
                    };

                    // 只保存metadata，不保存原始数据，设置过期时间
                    await db.put(chunkKey, '', {
                        metadata: updatedMetadata,
                        expirationTtl: 3600 // 1小时过期
                    });

                    console.log(`Chunk ${chunkIndex} uploaded successfully to ${uploadChannel} after ${retry + 1} attempts`);

                    // 清除缓存
                    clearCacheForUpload(uploadId);

                    return uploadResult;
                }
            } catch (uploadError) {
                console.warn(`Chunk ${chunkIndex} upload attempt ${retry + 1} failed:`, uploadError.message);
                
                if (retry === MAX_RETRIES - 1) {
                    // 最后一次上传失败，标记为失败状态并保留原始数据以便重试
                    throw uploadError;
                }
                
                // 指数退避延迟
                await new Promise(resolve => setTimeout(resolve, 1000 * (retry + 1)));
            }
        }

        throw new UploadError(`Chunk ${chunkIndex} upload failed after ${MAX_RETRIES} attempts`, 'UPLOAD_FAILED');

    } catch (error) {
        console.error(`Error uploading chunk ${chunkIndex}:`, error.message);

        // 发生异常时，确保保留原始数据并标记为失败
        try {
            await updateChunkStatusOnError(db, chunkKey, error);
        } catch (metaError) {
            console.error('Failed to save error metadata:', metaError);
        }

        throw error;
    }
}

// 上传单个分块到R2 (Multipart Upload)
async function uploadSingleChunkToR2Multipart(context, chunkData, chunkIndex, totalChunks, uploadId, originalFileName, originalFileType) {
    const { env, uploadConfig } = context;
    const db = getDatabase(env);

    try {
        const r2Settings = uploadConfig.cfr2;
        if (!r2Settings.channels || r2Settings.channels.length === 0) {
            throw new UploadError('No R2 channel provided', 'NO_CHANNEL_PROVIDED');
        }

        const R2DataBase = env.img_r2;
        const multipartKey = `multipart_${uploadId}`;

        let finalFileId;

        // 如果是第一个分块，生成并保存 finalFileId
        if (chunkIndex === 0) {
            finalFileId = await buildUniqueFileId(context, originalFileName, originalFileType);

            const multipartUpload = await R2DataBase.createMultipartUpload(finalFileId);
            const multipartInfo = {
                uploadId: multipartUpload.uploadId,
                key: finalFileId
            };

            // 保存multipart info
            await db.put(multipartKey, JSON.stringify(multipartInfo), {
                expirationTtl: 3600 // 1小时过期
            });
        } else {
            // 其他分块需要等待第一个分块完成multipart upload初始化
            finalFileId = await waitForMultipartInitialization(db, multipartKey, chunkIndex);
        }

        // 获取multipart info
        const multipartInfoData = await db.get(multipartKey);
        if (!multipartInfoData) {
            throw new UploadError('Multipart upload not initialized', 'MULTIPART_NOT_INITIALIZED');
        }

        const multipartInfo = JSON.parse(multipartInfoData);

        // 上传分块
        const multipartUpload = R2DataBase.resumeMultipartUpload(finalFileId, multipartInfo.uploadId);
        const uploadedPart = await multipartUpload.uploadPart(chunkIndex + 1, chunkData);

        if (!uploadedPart || !uploadedPart.etag) {
            throw new UploadError(`Failed to upload part ${chunkIndex + 1} to R2`, 'UPLOAD_FAILED');
        }

        return {
            success: true,
            partNumber: chunkIndex + 1,
            etag: uploadedPart.etag,
            size: chunkData.byteLength,
            uploadTime: Date.now(),
            multipartUploadId: multipartInfo.uploadId,
            key: finalFileId
        };

    } catch (error) {
        return {
            success: false,
            error: error.message,
            code: error.code || 'UNKNOWN_ERROR'
        };
    }
}

// 上传单个分块到S3 (Multipart Upload)
async function uploadSingleChunkToS3Multipart(context, chunkData, chunkIndex, totalChunks, uploadId, originalFileName, originalFileType) {
    const { env, uploadConfig, specifiedChannelName } = context;
    const db = getDatabase(env);

    try {
        const s3Settings = uploadConfig.s3;
        const s3Channels = s3Settings.channels;
        
        // 优先使用指定的渠道名称
        let s3Channel = getSpecifiedChannel(s3Channels, specifiedChannelName);
        
        // 如果没有指定渠道或指定渠道不存在，使用一致性哈希选择
        if (!s3Channel) {
            s3Channel = selectConsistentChannel(s3Channels, uploadId, s3Settings.loadBalance.enabled);
        }

        if (!s3Channel) {
            throw new UploadError('No S3 channel provided', 'NO_CHANNEL_PROVIDED');
        }

        console.log(`Uploading S3 chunk ${chunkIndex} for uploadId: ${uploadId}, selected channel: ${s3Channel.name || 'default'}`);

        const { endpoint, pathStyle, accessKeyId, secretAccessKey, bucketName, region } = s3Channel;

        const s3Client = new S3Client({
            region: region || "auto",
            endpoint,
            credentials: { accessKeyId, secretAccessKey },
            forcePathStyle: pathStyle
        });

        const multipartKey = `multipart_${uploadId}`;
        let finalFileId;

        // 如果是第一个分块，生成并保存 finalFileId
        if (chunkIndex === 0) {
            finalFileId = await buildUniqueFileId(context, originalFileName, originalFileType);

            const createResponse = await s3Client.send(new CreateMultipartUploadCommand({
                Bucket: bucketName,
                Key: finalFileId,
                ContentType: originalFileType || 'application/octet-stream'
            }));

            const multipartInfo = {
                uploadId: createResponse.UploadId,
                key: finalFileId
            };

            // 保存multipart info
            await db.put(multipartKey, JSON.stringify(multipartInfo), {
                expirationTtl: 3600 // 1小时过期
            });
        } else {
            // 其他分块需要等待第一个分块完成multipart upload初始化
            finalFileId = await waitForMultipartInitialization(db, multipartKey, chunkIndex);
        }

        // 获取multipart info
        const multipartInfoData = await db.get(multipartKey);
        if (!multipartInfoData) {
            throw new UploadError('Multipart upload not initialized', 'MULTIPART_NOT_INITIALIZED');
        }

        const multipartInfo = JSON.parse(multipartInfoData);

        // 上传分块
        const uploadResponse = await s3Client.send(new UploadPartCommand({
            Bucket: bucketName,
            Key: finalFileId,
            PartNumber: chunkIndex + 1,
            UploadId: multipartInfo.uploadId,
            Body: new Uint8Array(chunkData)
        }));

        if (!uploadResponse || !uploadResponse.ETag) {
            throw new UploadError(`Failed to upload part ${chunkIndex + 1} to S3`, 'UPLOAD_FAILED');
        }

        return {
            success: true,
            partNumber: chunkIndex + 1,
            etag: uploadResponse.ETag,
            size: chunkData.byteLength,
            uploadTime: Date.now(),
            s3Channel: s3Channel.name,
            multipartUploadId: multipartInfo.uploadId,
            key: finalFileId
        };

    } catch (error) {
        return {
            success: false,
            error: error.message,
            code: error.code || 'UNKNOWN_ERROR'
        };
    }
}

// 上传单个分块到Telegram
async function uploadSingleChunkToTelegram(context, chunkData, chunkIndex, totalChunks, uploadId, originalFileName, originalFileType) {
    const { uploadConfig, specifiedChannelName } = context;

    try {
        const tgSettings = uploadConfig.telegram;
        const tgChannels = tgSettings.channels;
        
        // 优先使用指定的渠道名称
        let tgChannel = getSpecifiedChannel(tgChannels, specifiedChannelName);
        
        // 如果没有指定渠道或指定渠道不存在，使用一致性哈希选择
        if (!tgChannel) {
            tgChannel = selectConsistentChannel(tgChannels, uploadId, tgSettings.loadBalance.enabled);
        }

        if (!tgChannel) {
            throw new UploadError('No Telegram channel provided', 'NO_CHANNEL_PROVIDED');
        }

        console.log(`Uploading Telegram chunk ${chunkIndex} for uploadId: ${uploadId}, selected channel: ${tgChannel.name || 'default'}`);

        const tgBotToken = tgChannel.botToken;
        const tgChatId = tgChannel.chatId;
        const tgProxyUrl = tgChannel.proxyUrl || '';

        // 创建分块文件名
        const chunkFileName = `${originalFileName}.part${chunkIndex.toString().padStart(3, '0')}`;
        const chunkBlob = new Blob([chunkData], { type: 'application/octet-stream' });

        // 上传分块到Telegram（支持代理域名）
        const chunkInfo = await uploadChunkToTelegramWithRetry(
            tgBotToken,
            tgChatId,
            tgProxyUrl,
            chunkBlob,
            chunkFileName,
            chunkIndex,
            totalChunks, // 传入正确的totalChunks
            2 // maxRetries
        );

        if (!chunkInfo) {
            throw new UploadError('Failed to upload chunk to Telegram', 'UPLOAD_FAILED');
        }

        return {
            success: true,
            fileId: chunkInfo.file_id,
            size: chunkInfo.file_size,
            fileName: chunkFileName,
            uploadTime: Date.now(),
            tgChannel: tgChannel.name
        };

    } catch (error) {
        return {
            success: false,
            error: error.message,
            code: error.code || 'UNKNOWN_ERROR'
        };
    }
}

// 上传单个分块到Discord
async function uploadSingleChunkToDiscord(context, chunkData, chunkIndex, totalChunks, uploadId, originalFileName, originalFileType) {
    const { uploadConfig, specifiedChannelName } = context;

    try {
        const discordSettings = uploadConfig.discord;
        const discordChannels = discordSettings.channels;
        
        // 优先使用指定的渠道名称
        let discordChannel = getSpecifiedChannel(discordChannels, specifiedChannelName);
        
        // 如果没有指定渠道或指定渠道不存在，使用一致性哈希选择
        if (!discordChannel) {
            discordChannel = selectConsistentChannel(discordChannels, uploadId, discordSettings.loadBalance?.enabled);
        }

        if (!discordChannel) {
            throw new UploadError('No Discord channel provided', 'NO_CHANNEL_PROVIDED');
        }

        console.log(`Uploading Discord chunk ${chunkIndex} for uploadId: ${uploadId}, selected channel: ${discordChannel.name || 'default'}`);

        const botToken = discordChannel.botToken;
        const channelId = discordChannel.channelId;

        // 创建分块文件名
        const chunkFileName = `${originalFileName}.part${chunkIndex.toString().padStart(3, '0')}`;
        const chunkBlob = new Blob([chunkData], { type: 'application/octet-stream' });

        // 上传分块到Discord（带重试）
        const chunkInfo = await uploadChunkToDiscordWithRetry(
            botToken,
            channelId,
            chunkBlob,
            chunkFileName,
            chunkIndex,
            totalChunks,
            2 // maxRetries
        );

        if (!chunkInfo) {
            throw new UploadError('Failed to upload chunk to Discord', 'UPLOAD_FAILED');
        }

        return {
            success: true,
            messageId: chunkInfo.message_id,
            // 注意：不存储 attachmentId 和 url，因为它们会在约24小时后过期
            // 读取时会通过 messageId 获取新的 URL
            size: chunkInfo.file_size,
            fileName: chunkFileName,
            uploadTime: Date.now(),
            discordChannel: discordChannel.name
        };

    } catch (error) {
        return {
            success: false,
            error: error.message,
            code: error.code || 'UNKNOWN_ERROR'
        };
    }
}

// 将每个分块上传至Discord，支持失败重试和 rate limit 处理
async function uploadChunkToDiscordWithRetry(botToken, channelId, chunkBlob, chunkFileName, chunkIndex, totalChunks, maxRetries = 2) {
    for (let attempt = 0; attempt < maxRetries; attempt++) {
        try {
            const discordAPI = new DiscordAPI(botToken);

            const response = await discordAPI.sendFile(chunkBlob, channelId, chunkFileName);

            if (!response || !response.id) {
                throw new Error('Invalid Discord response');
            }

            const fileInfo = discordAPI.getFileInfo(response);
            if (!fileInfo) {
                throw new Error('Failed to extract file info from response');
            }

            return fileInfo;

        } catch (error) {
            console.warn(`Discord chunk ${chunkIndex} upload attempt ${attempt + 1} failed:`, error.message);

            // 检查是否是 rate limit (429)
            if (error.message && error.message.includes('429')) {
                // 从错误消息中提取 retry_after，或使用默认值
                const retryAfter = 5000; // 默认等待 5 秒
                console.log(`Discord rate limited, waiting ${retryAfter}ms...`);
                await new Promise(resolve => setTimeout(resolve, retryAfter));
                continue; // 不计入重试次数
            }

            if (attempt === maxRetries - 1) {
                return null; // 最后一次尝试也失败了
            }

            // 指数退避延迟
            await new Promise(resolve => setTimeout(resolve, 1000 * (attempt + 1)));
        }
    }

    return null;
}

/* ======== 分块合并时与上传相关的工具函数 ======= */

// 重传失败的分块
// 并发重试失败的分块
export async function retryFailedChunks(context, failedChunks, uploadChannel, options = {}) {
    const {
        maxRetries = 5,
        retryTimeout = 60000, // 60秒重试超时
        maxConcurrency = 3, // 最大并发数
        batchSize = 6 // 每批处理的分块数
    } = options;

    if (!failedChunks || failedChunks.length === 0) {
        console.log('No failed chunks to retry');
        return { success: true, results: [] };
    }

    console.log(`Starting concurrent retry for ${failedChunks.length} failed chunks with max concurrency: ${maxConcurrency}`);

    const results = [];
    const chunksToRetry = failedChunks.filter(chunk =>
        chunk.hasData &&
        chunk.status !== 'uploading' &&
        chunk.status !== 'completed'
    );

    if (chunksToRetry.length === 0) {
        console.log('No chunks need retry (all are either uploading, completed, or have no data)');
        return { success: true, results: [] };
    }

    // 创建并发控制器
    const concurrencyController = new ConcurrencyController(maxConcurrency);

    // 分批处理以控制并发
    for (let i = 0; i < chunksToRetry.length; i += batchSize) {
        const batch = chunksToRetry.slice(i, i + batchSize);
        console.log(`Processing batch ${Math.floor(i / batchSize) + 1}: chunks ${batch.map(c => c.index).join(', ')}`);

        // 创建并发控制的重试任务
        const retryTasks = batch.map(async (chunk) => {
            return retrySingleChunk(context, chunk, uploadChannel, maxRetries, retryTimeout);
        });

        // 使用并发控制器处理任务
        const batchResults = await Promise.all(
            retryTasks.map(task => concurrencyController.addTask(task))
        );

        results.push(...batchResults);

        // 批次间稍作延迟
        if (i + batchSize < chunksToRetry.length) {
            await new Promise(resolve => setTimeout(resolve, 500));
        }
    }

    // 统计结果
    const successCount = results.filter(r => r.success).length;
    const failureCount = results.filter(r => !r.success).length;

    console.log(`Retry completed: ${successCount} successful, ${failureCount} failed out of ${results.length} chunks`);

    // 记录失败的分块信息
    const failedResults = results.filter(r => !r.success);
    if (failedResults.length > 0) {
        console.warn('Failed chunks:', failedResults.map(r => ({
            index: r.chunk?.index,
            reason: r.reason,
            error: r.error
        })));
    }

    return {
        success: failureCount === 0,
        results,
        summary: {
            total: results.length,
            successful: successCount,
            failed: failureCount,
            failedChunks: failedResults.map(r => r.chunk?.index).filter(Boolean)
        }
    };
}

// 重试单个失败的分块
async function retrySingleChunk(context, chunk, uploadChannel, maxRetries = 5, retryTimeout = 60000) {
    const { env } = context;
    const db = getDatabase(env);

    let retryCount = 0;
    let lastError = null;

    try {
        const chunkRecord = await db.getWithMetadata(chunk.key, { type: 'arrayBuffer' });
        if (!chunkRecord || !chunkRecord.value) {
            console.error(`Chunk ${chunk.index} data missing for retry`);
            return { success: false, chunk, reason: 'data_missing', error: 'Chunk data not found' };
        }

        const chunkData = chunkRecord.value;
        const originalFileName = chunkRecord.metadata?.originalFileName || 'unknown';
        const originalFileType = chunkRecord.metadata?.originalFileType || 'application/octet-stream';
        const uploadId = chunkRecord.metadata?.uploadId;
        const totalChunks = chunkRecord.metadata?.totalChunks || 1;

        // 更新重试状态
        const retryMetadata = {
            ...chunkRecord.metadata,
            status: 'retrying',
        };

        await db.put(chunk.key, chunkData, {
            metadata: retryMetadata,
            expirationTtl: 3600
        });

        // 使用策略模式获取上传处理器
        const uploadHandler = ChannelUploaders[uploadChannel];
        if (!uploadHandler) {
            throw new UploadError(`Unsupported upload channel: ${uploadChannel}`, 'UNSUPPORTED_CHANNEL', 400);
        }

        while (retryCount < maxRetries) {
            // 根据渠道重新上传，添加超时保护
            const retryPromise = uploadHandler(
                context, chunkData, chunk.index, totalChunks, 
                uploadId, originalFileName, originalFileType
            );

            const timeoutPromise = new Promise((resolve) => {
                setTimeout(() => resolve({
                    success: false,
                    error: 'Retry timeout',
                    code: 'RETRY_TIMEOUT'
                }), retryTimeout);
            });

            const uploadResult = await Promise.race([retryPromise, timeoutPromise]);

            if (uploadResult && uploadResult.success) {
                // 更新状态为成功
                const updatedMetadata = {
                    ...chunkRecord.metadata,
                    status: 'completed',
                    uploadResult: uploadResult,
                    retryCount: retryCount + 1,
                    completedTime: Date.now()
                };

                // 删除原始数据，只保留上传结果，设置过期时间
                await db.put(chunk.key, '', {
                    metadata: updatedMetadata,
                    expirationTtl: 3600 // 1小时过期
                });

                // 清除缓存
                clearCacheForUpload(uploadId);

                console.log(`Chunk ${chunk.index} retry successful after ${retryCount + 1} attempts`);
                return { success: true, chunk, retryCount: retryCount + 1 };
            } else if (retryCount === maxRetries - 1) {
                throw new UploadError(uploadResult?.error || 'Unknown retry error', uploadResult?.code || 'RETRY_FAILED');
            }

            retryCount++;
            lastError = uploadResult?.error || 'Unknown error';
            console.warn(`Chunk ${chunk.index} retry ${retryCount} failed: ${lastError}`);
            
            // 指数退避延迟
            const delay = Math.min(1000 * Math.pow(2, retryCount - 1), 10000);
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    } catch (error) {
        lastError = error;
        const isTimeout = error.code === 'RETRY_TIMEOUT';
        console.warn(`Chunk ${chunk.index} retry ${retryCount} ${isTimeout ? 'timed out' : 'failed'}: ${error.message}`);

        // 更新重试失败状态
        try {
            const chunkRecord = await db.getWithMetadata(chunk.key, { type: 'arrayBuffer' });
            if (chunkRecord) {
                const failedRetryMetadata = {
                    ...chunkRecord.metadata,
                    status: isTimeout ? 'retry_timeout' : 'retry_failed',
                    error: error.message,
                    failedTime: Date.now()
                };

                await db.put(chunk.key, chunkRecord.value, {
                    metadata: failedRetryMetadata,
                    expirationTtl: 3600
                });
            }
        } catch (metaError) {
            console.error(`Failed to update retry error metadata for chunk ${chunk.index}:`, metaError);
        }
    }

    console.error(`Chunk ${chunk.index} failed after ${maxRetries} retry attempts`);
    return { success: false, chunk, retryCount, error: lastError?.message || 'Max retries exceeded' };
}


// 清理失败的multipart upload
export async function cleanupFailedMultipartUploads(context, uploadId, uploadChannel) {
    const { env, uploadConfig } = context;
    const db = getDatabase(env);

    try {
        const multipartKey = `multipart_${uploadId}`;
        const multipartInfoData = await db.get(multipartKey);

        if (!multipartInfoData) {
            return; // 没有multipart upload需要清理
        }

        const multipartInfo = JSON.parse(multipartInfoData);

        if (uploadChannel === 'cfr2') {
            // 清理R2 multipart upload
            const R2DataBase = env.img_r2;
            const multipartUpload = R2DataBase.resumeMultipartUpload(multipartInfo.key, multipartInfo.uploadId);
            await multipartUpload.abort();

        } else if (uploadChannel === 's3') {
            // 清理S3 multipart upload
            const s3Settings = uploadConfig.s3;
            const s3Channels = s3Settings.channels;
            
            // 优先使用指定的渠道名称
            let s3Channel;
            const specifiedChannelName = context.specifiedChannelName;
            if (specifiedChannelName) {
                s3Channel = s3Channels.find(ch => ch.name === specifiedChannelName);
            }
            if (!s3Channel) {
                s3Channel = selectConsistentChannel(s3Channels, uploadId, s3Settings.loadBalance.enabled);
            }

            if (s3Channel) {
                const { endpoint, pathStyle, accessKeyId, secretAccessKey, bucketName, region } = s3Channel;

                const s3Client = new S3Client({
                    region: region || "auto",
                    endpoint,
                    credentials: { accessKeyId, secretAccessKey },
                    forcePathStyle: pathStyle
                });

                await s3Client.send(new AbortMultipartUploadCommand({
                    Bucket: bucketName,
                    Key: multipartInfo.key,
                    UploadId: multipartInfo.uploadId
                }));
            }
        }

        // 清理multipart info
        await db.delete(multipartKey);
        console.log(`Cleaned up failed multipart upload for ${uploadId}`);

    } catch (error) {
        console.error(`Failed to cleanup multipart upload for ${uploadId}:`, error);
    }
}


// 检查分块上传状态
export async function checkChunkUploadStatuses(env, uploadId, totalChunks) {
    // 尝试从缓存获取
    const cachedStatus = cacheGet('CHUNK_STATUS', uploadId);
    if (cachedStatus) {
        return cachedStatus;
    }

    const chunkStatuses = [];
    const currentTime = Date.now();

    const db = getDatabase(env);

    for (let i = 0; i < totalChunks; i++) {
        const chunkKey = `chunk_${uploadId}_${i.toString().padStart(3, '0')}`;
        try {
            const chunkRecord = await db.getWithMetadata(chunkKey, { type: 'arrayBuffer' });
            if (chunkRecord && chunkRecord.metadata) {
                let status = chunkRecord.metadata.status || 'unknown';

                // 检查上传超时：如果状态是 uploading 但超过了超时阈值，标记为超时
                if (status === 'uploading' && chunkRecord.metadata.timeoutThreshold && currentTime > chunkRecord.metadata.timeoutThreshold) {
                    status = 'timeout';

                    // 更新状态为超时
                    const timeoutMetadata = {
                        ...chunkRecord.metadata,
                        status: 'timeout',
                        error: 'Upload timeout detected',
                        timeoutDetectedTime: currentTime
                    };

                    await db.put(chunkKey, chunkRecord.value, {
                        metadata: timeoutMetadata,
                        expirationTtl: 3600
                    }).catch(err => console.warn(`Failed to update timeout status for chunk ${i}:`, err));
                }

                let hasData = false;
                if (status === 'completed') {
                    // 已完成的分块，不存储原始数据
                    hasData = false;
                } else if (status === 'uploading' || status === 'failed' || status === 'timeout') {
                    // 正在上传、失败或超时的分块通过原始数据判断
                    hasData = (chunkRecord.value && chunkRecord.value.byteLength > 0);
                } else {
                    // 其他状态也检查是否有数据
                    hasData = (chunkRecord.value && chunkRecord.value.byteLength > 0);
                }

                chunkStatuses.push({
                    index: i,
                    key: chunkKey,
                    status: status,
                    uploadResult: chunkRecord.metadata.uploadResult,
                    error: chunkRecord.metadata.error,
                    hasData: hasData,
                    chunkSize: chunkRecord.metadata.chunkSize,
                    uploadTime: chunkRecord.metadata.uploadTime,
                    uploadStartTime: chunkRecord.metadata.uploadStartTime,
                    timeoutThreshold: chunkRecord.metadata.timeoutThreshold,
                    uploadChannel: chunkRecord.metadata.uploadChannel,
                    isTimeout: status === 'timeout'
                });
            } else {
                chunkStatuses.push({
                    index: i,
                    key: chunkKey,
                    status: 'missing',
                    hasData: false
                });
            }
        } catch (error) {
            chunkStatuses.push({
                index: i,
                key: chunkKey,
                status: 'error',
                error: error.message,
                hasData: false
            });
        }
    }

    // 缓存结果
    cacheSet('CHUNK_STATUS', uploadId, chunkStatuses);

    return chunkStatuses;
}


// 清理临时分块数据
export async function cleanupChunkData(env, uploadId, totalChunks) {
    try {
        const db = getDatabase(env);

        for (let i = 0; i < totalChunks; i++) {
            const chunkKey = `chunk_${uploadId}_${i.toString().padStart(3, '0')}`;

            // 删除数据库中的分块记录
            await db.delete(chunkKey);
        }

        // 清理multipart info（如果存在）
        const multipartKey = `multipart_${uploadId}`;
        await db.delete(multipartKey);

        // 清除缓存
        clearCacheForUpload(uploadId);

    } catch (cleanupError) {
        console.warn('Failed to cleanup chunk data:', cleanupError);
    }
}

// 清理上传会话
export async function cleanupUploadSession(env, uploadId) {
    try {
        const db = getDatabase(env);

        const sessionKey = `upload_session_${uploadId}`;
        await db.delete(sessionKey);
        
        // 清除缓存
        clearCacheForUpload(uploadId);
        
        console.log(`Cleaned up upload session for ${uploadId}`);
    } catch (cleanupError) {
        console.warn('Failed to cleanup upload session:', cleanupError);
    }
}

// 强制清理所有相关数据（用于彻底清理失败的上传）
export async function forceCleanupUpload(context, uploadId, totalChunks) {
    const { env } = context;
    const db = getDatabase(env);

    try {
        // 读取 session 信息
        const sessionKey = `upload_session_${uploadId}`;
        const sessionRecord = await db.get(sessionKey);
        const uploadChannel = sessionRecord ? JSON.parse(sessionRecord).uploadChannel : 'cfr2'; // 默认使用 cfr2

        // 清理 multipart upload信息
        await cleanupFailedMultipartUploads(context, uploadId, uploadChannel);

        const cleanupPromises = [];

        // 清理所有分块
        for (let i = 0; i < totalChunks; i++) {
            const chunkKey = `chunk_${uploadId}_${i.toString().padStart(3, '0')}`;
            cleanupPromises.push(db.delete(chunkKey).catch(err =>
                console.warn(`Failed to delete chunk ${i}:`, err)
            ));
        }

        // 清理相关的键
        const keysToCleanup = [
            `upload_session_${uploadId}`,
            `multipart_${uploadId}`
        ];

        keysToCleanup.forEach(key => {
            cleanupPromises.push(db.delete(key).catch(err =>
                console.warn(`Failed to delete key ${key}:`, err)
            ));
        });

        await Promise.allSettled(cleanupPromises);
        
        // 清除缓存
        clearCacheForUpload(uploadId);
        
        console.log(`Force cleanup completed for ${uploadId}`);

    } catch (cleanupError) {
        console.warn('Failed to force cleanup upload:', cleanupError);
    }
}

/* ======= 单个大文件大文件分块上传到Telegram ======= */
export async function uploadLargeFileToTelegram(context, file, fullId, metadata, fileName, fileType, returnLink, tgBotToken, tgChatId, tgChannel) {
    const { env, waitUntil } = context;
    const db = getDatabase(env);

    const CHUNK_SIZE = 16 * 1024 * 1024; // 16MB (TG Bot getFile download limit: 20MB, leave 4MB safety margin)
    const fileSize = file.size;
    const totalChunks = Math.ceil(fileSize / CHUNK_SIZE);

    // 为了避免CPU超时，限制最大分片数（考虑Cloudflare Worker的CPU时间限制）
    if (totalChunks > 50) {
        return createResponse('Error: File too large (exceeds 1GB limit)', { status: 413 });
    }

    const chunks = [];
    const uploadedChunks = [];

    try {
        // 创建并发控制器
        const concurrencyController = new ConcurrencyController(3);

        // 准备所有分块上传任务
        const uploadTasks = [];
        for (let i = 0; i < totalChunks; i++) {
            uploadTasks.push(async () => {
                const start = i * CHUNK_SIZE;
                const end = Math.min(start + CHUNK_SIZE, fileSize);
                const chunkBlob = file.slice(start, end);

                // 生成分片文件名
                const chunkFileName = `${fileName}.part${i.toString().padStart(3, '0')}`;

                // 上传分片（带重试机制）
                const chunkInfo = await uploadChunkToTelegramWithRetry(
                    tgBotToken,
                    tgChatId,
                    chunkBlob,
                    chunkFileName,
                    i,
                    totalChunks
                );

                if (!chunkInfo) {
                    throw new Error(`Failed to upload chunk ${i + 1}/${totalChunks} after retries`);
                }

                // 验证分片信息完整性
                if (!chunkInfo.file_id || !chunkInfo.file_size) {
                    throw new Error(`Invalid chunk info for chunk ${i + 1}/${totalChunks}`);
                }

                return {
                    index: i,
                    fileId: chunkInfo.file_id,
                    size: chunkInfo.file_size,
                    fileName: chunkFileName
                };
            });
        }

        // 并发执行上传任务
        const results = await Promise.all(
            uploadTasks.map(task => concurrencyController.addTask(task))
        );

        // 整理结果
        results.forEach(result => {
            chunks.push(result);
            uploadedChunks.push(result.fileId);
        });

        // 所有分片上传成功，更新metadata
        metadata.Channel = "TelegramNew";
        metadata.ChannelName = tgChannel.name;
        metadata.TgChatId = tgChatId;
        metadata.TgBotToken = tgBotToken;
        metadata.IsChunked = true;
        metadata.TotalChunks = totalChunks;
        metadata.FileSize = (fileSize / 1024 / 1024).toFixed(2);


        // 将分片信息存储到value中
        const chunksData = JSON.stringify(chunks);

        // 验证分片完整性
        if (chunks.length !== totalChunks) {
            throw new Error(`Chunk count mismatch: expected ${totalChunks}, got ${chunks.length}`);
        }

        // 写入最终的数据库记录，分片信息作为value
        await db.put(fullId, chunksData, { metadata });

        // 异步结束上传
        waitUntil(endUpload(context, fullId, metadata));

        return createResponse(
            JSON.stringify([{ 'src': returnLink }]),
            {
                status: 200,
                headers: {
                    'Content-Type': 'application/json',
                }
            }
        );

    } catch (error) {
        return createResponse(`Telegram Channel Error: Large file upload failed - ${error.message}`, { status: 500 });
    }
}

// 将每个分块上传至Telegram，支持失败重试（支持代理域名）
async function uploadChunkToTelegramWithRetry(tgBotToken, tgChatId, tgProxyUrl, chunkBlob, chunkFileName, chunkIndex, totalChunks, maxRetries = 2) {
    for (let attempt = 0; attempt < maxRetries; attempt++) {
        try {
            const tgAPI = new TelegramAPI(tgBotToken, tgProxyUrl);

            const caption = `Part ${chunkIndex + 1}/${totalChunks}`;

            const response = await tgAPI.sendFile(chunkBlob, tgChatId, 'sendDocument', 'document', caption, chunkFileName);
            if (!response.ok) {
                throw new Error(response.description || 'Telegram API error');
            }

            const fileInfo = tgAPI.getFileInfo(response);
            if (!fileInfo) {
                throw new Error('Failed to extract file info from response');
            }

            return fileInfo;

        } catch (error) {
            console.warn(`Chunk ${chunkIndex} upload attempt ${attempt + 1} failed:`, error.message);

            if (attempt === maxRetries - 1) {
                return null; // 最后一次尝试也失败了
            }

            // 减少重试等待时间以节省CPU时间
            await new Promise(resolve => setTimeout(resolve, 500 * (attempt + 1)));
        }
    }

    return null;
}

/* ======= 辅助函数 ======= */

// 解析并验证表单数据
async function parseFormDataWithValidation(request) {
    try {
        return await request.formData();
    } catch (error) {
        throw new UploadError('Invalid form data', 'INVALID_FORM_DATA', 400);
    }
}

// 验证初始化参数
function validateInitializationParameters(originalFileName, originalFileType, totalChunks) {
    if (!originalFileName) {
        throw new UploadError('Missing originalFileName parameter', 'MISSING_PARAMETER', 400, { parameter: 'originalFileName' });
    }
    if (!originalFileType) {
        throw new UploadError('Missing originalFileType parameter', 'MISSING_PARAMETER', 400, { parameter: 'originalFileType' });
    }
    if (!totalChunks || isNaN(totalChunks) || totalChunks <= 0) {
        throw new UploadError('Invalid totalChunks parameter', 'INVALID_PARAMETER', 400, { parameter: 'totalChunks' });
    }
}

// 验证分块上传参数
async function validateChunkUploadParameters(formdata) {
    const chunk = formdata.get('file');
    const chunkIndex = parseInt(formdata.get('chunkIndex'));
    const totalChunks = parseInt(formdata.get('totalChunks'));
    const uploadId = formdata.get('uploadId');
    const originalFileName = formdata.get('originalFileName');
    const originalFileType = formdata.get('originalFileType');

    if (!chunk) {
        throw new UploadError('Missing file chunk', 'MISSING_PARAMETER', 400, { parameter: 'file' });
    }
    if (chunkIndex === null || isNaN(chunkIndex) || chunkIndex < 0) {
        throw new UploadError('Invalid chunkIndex parameter', 'INVALID_PARAMETER', 400, { parameter: 'chunkIndex' });
    }
    if (!totalChunks || isNaN(totalChunks) || totalChunks <= 0) {
        throw new UploadError('Invalid totalChunks parameter', 'INVALID_PARAMETER', 400, { parameter: 'totalChunks' });
    }
    if (!uploadId) {
        throw new UploadError('Missing uploadId parameter', 'MISSING_PARAMETER', 400, { parameter: 'uploadId' });
    }
    if (!originalFileName) {
        throw new UploadError('Missing originalFileName parameter', 'MISSING_PARAMETER', 400, { parameter: 'originalFileName' });
    }
    if (!originalFileType) {
        throw new UploadError('Missing originalFileType parameter', 'MISSING_PARAMETER', 400, { parameter: 'originalFileType' });
    }

    return { chunk, chunkIndex, totalChunks, uploadId, originalFileName, originalFileType };
}

// 验证上传会话
async function validateUploadSession(env, uploadId, originalFileName, totalChunks) {
    const db = getDatabase(env);
    const sessionKey = `upload_session_${uploadId}`;
    
    // 尝试从缓存获取会话
    let sessionData = cacheGet('UPLOAD_SESSION', uploadId);
    
    // 缓存未命中，从数据库获取
    if (!sessionData) {
        sessionData = await db.get(sessionKey);
        if (!sessionData) {
            throw new UploadError('Invalid or expired upload session', 'INVALID_SESSION', 400);
        }
        // 缓存会话数据
        cacheSet('UPLOAD_SESSION', uploadId, sessionData);
    }

    const sessionInfo = JSON.parse(sessionData);

    // 验证会话信息
    if (sessionInfo.originalFileName !== originalFileName ||
        sessionInfo.totalChunks !== totalChunks) {
        throw new UploadError('Session parameters mismatch', 'SESSION_MISMATCH', 400);
    }

    // 检查会话是否过期
    if (Date.now() > sessionInfo.expiresAt) {
        throw new UploadError('Upload session expired', 'SESSION_EXPIRED', 410);
    }

    return sessionInfo;
}

// 创建分块元数据
function createChunkMetadata(uploadId, chunkIndex, totalChunks, originalFileName, originalFileType, chunkSize, uploadStartTime, uploadChannel) {
    return {
        uploadId,
        chunkIndex,
        totalChunks,
        originalFileName,
        originalFileType,
        chunkSize,
        uploadTime: uploadStartTime,
        uploadStartTime: uploadStartTime,
        status: 'uploading',
        uploadChannel,
        timeoutThreshold: uploadStartTime + 60000 // 1分钟超时阈值
    };
}

// 更新分块错误状态
async function updateChunkStatusOnError(db, chunkKey, error) {
    const chunkRecord = await db.getWithMetadata(chunkKey, { type: 'arrayBuffer' });
    if (chunkRecord && chunkRecord.metadata) {
        const isTimeout = error.code === 'UPLOAD_TIMEOUT';
        const errorMetadata = {
            ...chunkRecord.metadata,
            status: isTimeout ? 'timeout' : 'failed',
            error: error.message,
            failedTime: Date.now(),
            isTimeout: isTimeout
        };

        // 保留原始数据以便重试
        await db.put(chunkKey, chunkRecord.value, {
            metadata: errorMetadata,
            expirationTtl: 3600
        });
    }
}

// 等待Multipart初始化
async function waitForMultipartInitialization(db, multipartKey, chunkIndex) {
    let multipartInfoData = null;
    let retryCount = 0;
    const maxRetries = 30; // 最多等待60秒

    while (!multipartInfoData && retryCount < maxRetries) {
        multipartInfoData = await db.get(multipartKey);
        if (!multipartInfoData) {
            // 等待2秒后重试
            await new Promise(resolve => setTimeout(resolve, 2000));
            retryCount++;
            console.log(`Chunk ${chunkIndex} waiting for multipart initialization... (${retryCount}/${maxRetries})`);
        }
    }

    if (!multipartInfoData) {
        throw new UploadError('Multipart upload not initialized after waiting', 'MULTIPART_INIT_TIMEOUT');
    }

    const multipartInfo = JSON.parse(multipartInfoData);
    return multipartInfo.key;
}

// 带缓存的IP地址获取
async function getIPAddressWithCache(ip) {
    if (!ip) return '未知';
    
    // 尝试从缓存获取
    const cachedAddress = cacheGet('IP_GEOLOCATION', ip);
    if (cachedAddress) {
        return cachedAddress;
    }

    const address = await getIPAddress(ip);
    
    // 缓存结果
    cacheSet('IP_GEOLOCATION', ip, address);
    
    return address;
}

// 生成上传ID
function generateUploadId() {
    const timestamp = Date.now();
    const random = Math.random().toString(36).slice(2, 11);
    return `upload_${timestamp}_${random}`;
}

// 获取指定的渠道
function getSpecifiedChannel(channels, channelName) {
    if (!channelName || !channels || !channels.length) {
        return null;
    }
    
    return channels.find(ch => ch.name === channelName);
}

// 清除上传相关的缓存
function clearCacheForUpload(uploadId) {
    memoryCache.delete(`CHUNK_STATUS_${uploadId}`);
    memoryCache.delete(`UPLOAD_SESSION_${uploadId}`);
}

// 并发控制工具
class ConcurrencyController {
    constructor(concurrencyLimit = 5) {
        this.concurrencyLimit = concurrencyLimit;
        this.running = 0;
        this.queue = [];
    }

    async addTask(task) {
        return new Promise((resolve, reject) => {
            this.queue.push({ task, resolve, reject });
            this.runNext();
        });
    }

    async runNext() {
        if (this.running >= this.concurrencyLimit || this.queue.length === 0) {
            return;
        }

        const { task, resolve, reject } = this.queue.shift();
        this.running++;

        try {
            const result = await task();
            resolve(result);
        } catch (error) {
            reject(error);
        } finally {
            this.running--;
            this.runNext();
        }
    }
}

// 缓存工具函数
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

// 错误日志记录
async function logUploadError(env, error, context = {}) {
    try {
        if (!env.KV) return null;
        
        const errorId = generateShortId(16);
        const errorLog = {
            errorId,
            message: error.message,
            code: error.code || 'UNKNOWN_ERROR',
            status: error.status || 500,
            stack: error.stack,
            timestamp: error.timestamp || Date.now(),
            context: {
                uploadId: context.uploadId,
                chunkIndex: context.chunkIndex,
                channel: context.channel,
                ...context
            }
        };
        
        await env.KV.put(`upload_error_${errorId}`, JSON.stringify(errorLog), {
            expirationTtl: 7 * 24 * 60 * 60 // 保存7天
        });
        
        return errorId;
    } catch (logError) {
        console.error('Failed to log upload error:', logError);
        return null;
    }
}

// 生成短链接
function generateShortId(length = 8) {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
}
