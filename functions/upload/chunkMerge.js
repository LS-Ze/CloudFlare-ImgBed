/* ========== 分块合并处理 ========== */
import { createResponse, getUploadIp, getIPAddress, selectConsistentChannel, buildUniqueFileId, endUpload } from './uploadTools';
import { retryFailedChunks, cleanupFailedMultipartUploads, checkChunkUploadStatuses, cleanupChunkData, cleanupUploadSession } from './chunkUpload';
import { S3Client, CompleteMultipartUploadCommand } from "@aws-sdk/client-s3";
import { getDatabase } from '../utils/databaseAdapter.js';

// 缓存配置
const CACHE_CONFIG = {
    CHUNK_STATUS: { ttl: 30, maxSize: 1000 }, // 30秒缓存，最多1000个上传ID
    UPLOAD_SESSION: { ttl: 60, maxSize: 1000 } // 60秒缓存，最多1000个会话
};

// 内存缓存
const memoryCache = new Map();

// 统一错误类
export class MergeError extends Error {
    constructor(message, code = 'MERGE_ERROR', status = 500, details = {}) {
        super(message);
        this.name = 'MergeError';
        this.code = code;
        this.status = status;
        this.details = details;
        this.timestamp = Date.now();
    }
}

// 渠道处理函数映射
const ChannelHandlers = {
    cfr2: mergeR2ChunksInfo,
    s3: mergeS3ChunksInfo,
    telegram: mergeTelegramChunksInfo,
    discord: mergeDiscordChunksInfo
};

// 处理分块合并
export async function handleChunkMerge(context) {
    const { request, env, url, waitUntil } = context;
    const db = getDatabase(env);

    try {
        // 解析表单数据
        const formdata = await parseFormDataWithValidation(request);
        context.formdata = formdata;

        // 验证并获取参数
        const { uploadId, totalChunks, originalFileName, originalFileType, uploadChannel } = 
            await validateMergeParameters(context, formdata, url);

        // 开始合并处理
        const result = await startMerge(context, uploadId, totalChunks, originalFileName, originalFileType, uploadChannel);

        return createResponse(JSON.stringify(result.result), {
            status: 200,
            headers: { 'Content-Type': 'application/json' }
        });

    } catch (error) {
        const errorId = await logMergeError(env, error, {
            uploadId: context.uploadId,
            requestId: context.requestId
        });

        // 清理失败的上传
        await cleanupFailedUpload(context);

        return createResponse(`Error: Failed to merge chunks - ${error.message} (Error ID: ${errorId})`, { 
            status: error.status || 500 
        });
    }
}

// 解析并验证表单数据
async function parseFormDataWithValidation(request) {
    try {
        return await request.formData();
    } catch (error) {
        throw new MergeError('Invalid form data', 'INVALID_FORM_DATA', 400);
    }
}

// 验证合并参数
async function validateMergeParameters(context, formdata, url) {
    const { env } = context;
    const db = getDatabase(env);

    // 从表单获取参数
    const uploadId = formdata.get('uploadId');
    const totalChunks = parseInt(formdata.get('totalChunks'));
    const originalFileName = formdata.get('originalFileName');
    const originalFileType = formdata.get('originalFileType');

    // 验证必填参数
    if (!uploadId) {
        throw new MergeError('Missing uploadId parameter', 'MISSING_PARAMETER', 400, { parameter: 'uploadId' });
    }
    if (!totalChunks || isNaN(totalChunks) || totalChunks <= 0) {
        throw new MergeError('Invalid totalChunks parameter', 'INVALID_PARAMETER', 400, { parameter: 'totalChunks' });
    }
    if (!originalFileName) {
        throw new MergeError('Missing originalFileName parameter', 'MISSING_PARAMETER', 400, { parameter: 'originalFileName' });
    }

    // 保存uploadId到上下文
    context.uploadId = uploadId;

    // 验证上传会话
    const sessionKey = `upload_session_${uploadId}`;
    
    // 尝试从缓存获取会话
    let sessionData = cacheGet('UPLOAD_SESSION', uploadId);
    
    // 缓存未命中，从数据库获取
    if (!sessionData) {
        sessionData = await db.get(sessionKey);
        if (!sessionData) {
            throw new MergeError('Invalid or expired upload session', 'INVALID_SESSION', 400);
        }
        // 缓存会话数据
        cacheSet('UPLOAD_SESSION', uploadId, sessionData);
    }

    const sessionInfo = JSON.parse(sessionData);

    // 验证会话信息
    if (sessionInfo.originalFileName !== originalFileName ||
        sessionInfo.totalChunks !== totalChunks) {
        throw new MergeError('Session parameters mismatch', 'SESSION_MISMATCH', 400);
    }

    // 检查会话是否过期
    if (Date.now() > sessionInfo.expiresAt) {
        throw new MergeError('Upload session expired', 'SESSION_EXPIRED', 410);
    }

    // 使用会话中的上传渠道，或者从URL参数获取
    const uploadChannel = url.searchParams.get('uploadChannel') || sessionInfo.uploadChannel || 'telegram';

    // 获取指定的渠道名称（优先URL参数，其次会话信息）
    const channelName = url.searchParams.get('channelName') || sessionInfo.channelName || '';
    context.specifiedChannelName = channelName;

    // 检查分块上传状态
    const chunkStatuses = await checkChunkUploadStatusesWithCache(env, uploadId, totalChunks);

    // 输出初始状态摘要
    const initialStatusSummary = chunkStatuses.reduce((acc, chunk) => {
        acc[chunk.status] = (acc[chunk.status] || 0) + 1;
        return acc;
    }, {});
    console.log(`Initial chunk status summary for ${uploadId}: ${JSON.stringify(initialStatusSummary)}`);

    return { uploadId, totalChunks, originalFileName, originalFileType, uploadChannel };
}

// 开始合并处理
async function startMerge(context, uploadId, totalChunks, originalFileName, originalFileType, uploadChannel) {
    const { env } = context;

    try {
        // 合并任务状态输出
        const mergeStatus = {
            uploadId,
            status: 'processing',
            progress: 0,
            totalChunks,
            originalFileName,
            originalFileType,
            uploadChannel,
            createdAt: Date.now(),
            message: 'Starting merge process...'
        };
        console.log(`Merge status for ${uploadId}: ${JSON.stringify(mergeStatus)}`);

        // 同步执行合并
        const result = await handleChannelBasedMerge(context, uploadId, totalChunks, originalFileName, originalFileType, uploadChannel);

        if (result.success) {
            // 清理临时分块数据
            await cleanupChunkData(env, uploadId, totalChunks);

            // 清理上传会话
            await cleanupUploadSession(env, uploadId);

            // 清除缓存
            clearCacheForUpload(uploadId);

            return result;
        } else {
            throw new MergeError(result.error || 'Merge failed', 'MERGE_FAILED');
        }

    } catch (error) {
        // 清除缓存
        clearCacheForUpload(uploadId);
        
        throw error;
    }
}

// 基于渠道的合并处理
async function handleChannelBasedMerge(context, uploadId, totalChunks, originalFileName, originalFileType, uploadChannel) {
    const { request, env, url } = context;

    try {
        // 获得上传IP
        const uploadIp = getUploadIp(request);

        const normalizedFolder = (url.searchParams.get('uploadFolder') || '').replace(/^\/+/, '').replace(/\/{2,}/g, '/').replace(/\/$/, '');

        // 构建基础metadata
        const metadata = {
            FileName: originalFileName,
            FileType: originalFileType,
            FileSize: '0', // 会在最终合并后更新
            UploadIP: uploadIp,
            UploadAddress: await getIPAddressWithCache(uploadIp),
            ListType: "None",
            TimeStamp: Date.now(),
            Label: "None",
            Directory: normalizedFolder === '' ? '' : normalizedFolder + '/',
            Tags: [],
            MergeId: generateShortId(12),
            MergeTimestamp: Date.now()
        };

        // 收集所有已上传的分块信息
        let chunkStatuses = await checkChunkUploadStatusesWithCache(env, uploadId, totalChunks);
        let completedChunks = chunkStatuses.filter(chunk => chunk.status === 'completed');
        let uploadingChunks = chunkStatuses.filter(chunk =>
            chunk.status === 'uploading' ||
            chunk.status === 'retrying'
        );
        let failedChunks = chunkStatuses.filter(chunk =>
            chunk.status === 'failed' ||
            chunk.status === 'timeout'
        );

        // 统计不同状态的分块
        const statusSummary = chunkStatuses.reduce((acc, chunk) => {
            acc[chunk.status] = (acc[chunk.status] || 0) + 1;
            return acc;
        }, {});

        console.log(`Chunk status summary for ${uploadId}: ${JSON.stringify(statusSummary)}`);

        // 如果有失败的分块，尝试重试
        if (failedChunks.length > 0) {
            console.log(`Retrying ${failedChunks.length} failed chunks for ${uploadId}...`);
            // 同步重试（await）
            await retryFailedChunks(context, failedChunks, uploadChannel);
            
            // 清除缓存，因为分块状态可能已更新
            clearCacheForUpload(uploadId);
            
            // 重新获取状态
            chunkStatuses = await checkChunkUploadStatusesWithCache(env, uploadId, totalChunks);
            completedChunks = chunkStatuses.filter(chunk => chunk.status === 'completed');
        }

        // 最终检查是否所有分块都完成
        if (completedChunks.length !== totalChunks) {
            // 获取最新的状态信息
            const finalStatuses = await checkChunkUploadStatusesWithCache(env, uploadId, totalChunks);
            const finalStatusSummary = finalStatuses.reduce((acc, chunk) => {
                acc[chunk.status] = (acc[chunk.status] || 0) + 1;
                return acc;
            }, {});

            throw new MergeError(
                `Only ${completedChunks.length}/${totalChunks} chunks completed successfully`,
                'INCOMPLETE_CHUNKS',
                400,
                { 
                    completed: completedChunks.length,
                    total: totalChunks,
                    status: finalStatusSummary
                }
            );
        }

        // 根据渠道合并分块信息
        const mergeHandler = ChannelHandlers[uploadChannel];
        if (!mergeHandler) {
            throw new MergeError(`Unsupported upload channel: ${uploadChannel}`, 'UNSUPPORTED_CHANNEL', 400);
        }

        return mergeHandler(context, uploadId, completedChunks, metadata);

    } catch (error) {
        return {
            success: false,
            error: error.message,
            code: error.code || 'UNKNOWN_ERROR'
        };
    }
}

// 合并R2分块信息
async function mergeR2ChunksInfo(context, uploadId, completedChunks, metadata) {
    const { env, waitUntil, url, specifiedChannelName } = context;
    const db = getDatabase(env);

    try {
        const R2DataBase = env.img_r2;
        const multipartKey = `multipart_${uploadId}`;

        // 获取multipart info
        const multipartInfoData = await db.get(multipartKey);
        if (!multipartInfoData) {
            throw new MergeError('Multipart upload info not found', 'MISSING_MULTIPART_INFO');
        }

        const multipartInfo = JSON.parse(multipartInfoData);

        // 组织所有分块
        const sortedChunks = completedChunks.sort((a, b) => a.index - b.index);
        const parts = sortedChunks.map(chunk => ({
            etag: chunk.uploadResult.etag,
            partNumber: chunk.uploadResult.partNumber,
        }));

        // 完成multipart upload
        const multipartUpload = R2DataBase.resumeMultipartUpload(multipartInfo.key, multipartInfo.uploadId);
        await multipartUpload.complete(parts);

        // 计算总大小
        const totalSize = completedChunks.reduce((sum, chunk) => sum + chunk.uploadResult.size, 0);

        // 使用multipart info中的finalFileId更新metadata
        const finalFileId = multipartInfo.key;
        metadata.Channel = "CloudflareR2";
        // 从 R2 设置中获取渠道名称（优先使用指定的渠道名称）
        const r2Settings = context.uploadConfig.cfr2;
        let r2ChannelName = "R2_env";
        if (specifiedChannelName) {
            const r2Channel = r2Settings.channels?.find(ch => ch.name === specifiedChannelName);
            if (r2Channel) {
                r2ChannelName = r2Channel.name;
            }
        } else if (r2Settings.channels?.[0]?.name) {
            r2ChannelName = r2Settings.channels[0].name;
        }
        metadata.ChannelName = r2ChannelName;
        metadata.FileSize = (totalSize / 1024 / 1024).toFixed(2);
        metadata.MergeMethod = "R2 Multipart";

        // 清理multipart info
        await db.delete(multipartKey);

        // 写入数据库
        await db.put(finalFileId, "", { metadata });

        // 结束上传
        waitUntil(endUpload(context, finalFileId, metadata));

        // 更新返回链接
        const returnFormat = url.searchParams.get('returnFormat') || 'default';
        const updatedReturnLink = returnFormat === 'full' 
            ? `${url.origin}/file/${finalFileId}` 
            : `/file/${finalFileId}`;

        return {
            success: true,
            result: [{ 'src': updatedReturnLink }]
        };

    } catch (error) {
        throw new MergeError(`R2 merge failed: ${error.message}`, 'R2_MERGE_FAILED');
    }
}

// 合并S3分块信息
async function mergeS3ChunksInfo(context, uploadId, completedChunks, metadata) {
    const { env, waitUntil, uploadConfig, url, specifiedChannelName } = context;
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
            throw new MergeError('No available S3 channels', 'NO_S3_CHANNELS');
        }

        console.log(`Merging S3 chunks for uploadId: ${uploadId}, selected channel: ${s3Channel.name || 'default'}`);

        const { endpoint, pathStyle, accessKeyId, secretAccessKey, bucketName, region } = s3Channel;

        const s3Client = new S3Client({
            region: region || "auto",
            endpoint,
            credentials: { accessKeyId, secretAccessKey },
            forcePathStyle: pathStyle
        });

        const multipartKey = `multipart_${uploadId}`;

        // 获取multipart info
        const multipartInfoData = await db.get(multipartKey);
        if (!multipartInfoData) {
            throw new MergeError('Multipart upload info not found', 'MISSING_MULTIPART_INFO');
        }

        const multipartInfo = JSON.parse(multipartInfoData);

        // 组织所有分块
        const sortedChunks = completedChunks.sort((a, b) => a.index - b.index);
        const parts = sortedChunks.map(chunk => ({
            ETag: chunk.uploadResult.etag,
            PartNumber: chunk.uploadResult.partNumber
        }));

        // 完成multipart upload
        await s3Client.send(new CompleteMultipartUploadCommand({
            Bucket: bucketName,
            Key: multipartInfo.key,
            UploadId: multipartInfo.uploadId,
            MultipartUpload: { Parts: parts }
        }));

        // 计算总大小
        const totalSize = completedChunks.reduce((sum, chunk) => sum + chunk.uploadResult.size, 0);

        // 使用multipart info中的finalFileId更新metadata
        const finalFileId = multipartInfo.key;
        metadata.Channel = "S3";
        metadata.ChannelName = s3Channel.name;
        metadata.FileSize = (totalSize / 1024 / 1024).toFixed(2);
        metadata.MergeMethod = "S3 Multipart";

        const s3ServerDomain = endpoint.replace(/https?:\/\//, "");
        if (pathStyle) {
            metadata.S3Location = `https://${s3ServerDomain}/${bucketName}/${finalFileId}`;
        } else {
            metadata.S3Location = `https://${bucketName}.${s3ServerDomain}/${finalFileId}`;
        }
        metadata.S3Endpoint = endpoint;
        metadata.S3PathStyle = pathStyle;
        metadata.S3BucketName = bucketName;
        metadata.S3FileKey = finalFileId;

        // 清理multipart info
        await db.delete(multipartKey);

        // 写入数据库
        await db.put(finalFileId, "", { metadata });

        // 异步结束上传
        waitUntil(endUpload(context, finalFileId, metadata));

        // 更新返回链接
        const returnFormat = url.searchParams.get('returnFormat') || 'default';
        const updatedReturnLink = returnFormat === 'full' 
            ? `${url.origin}/file/${finalFileId}` 
            : `/file/${finalFileId}`;

        return {
            success: true,
            result: [{ src: updatedReturnLink }]
        };

    } catch (error) {
        throw new MergeError(`S3 merge failed: ${error.message}`, 'S3_MERGE_FAILED');
    }
}

// 合并Telegram分块信息
async function mergeTelegramChunksInfo(context, uploadId, completedChunks, metadata) {
    const { env, waitUntil, uploadConfig, url, specifiedChannelName } = context;
    const db = getDatabase(env);

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
            throw new MergeError('No available Telegram channels', 'NO_TELEGRAM_CHANNELS');
        }

        console.log(`Merging Telegram chunks for uploadId: ${uploadId}, selected channel: ${tgChannel.name || 'default'}`);

        const tgBotToken = tgChannel.botToken;
        const tgChatId = tgChannel.chatId;

        // 按顺序排列分块
        const sortedChunks = completedChunks.sort((a, b) => a.index - b.index);

        // 计算总大小
        const totalSize = sortedChunks.reduce((sum, chunk) => sum + chunk.uploadResult.size, 0);

        // 构建分块信息数组
        const chunks = sortedChunks.map(chunk => ({
            index: chunk.index,
            fileId: chunk.uploadResult.fileId,
            size: chunk.uploadResult.size,
            fileName: chunk.uploadResult.fileName
        }));

        // 生成 finalFileId
        const finalFileId = await buildUniqueFileId(context, metadata.FileName, metadata.FileType);

        // 更新metadata
        metadata.Channel = "TelegramNew";
        metadata.ChannelName = tgChannel.name;
        metadata.TgChatId = tgChatId;
        metadata.IsChunked = true;
        metadata.TotalChunks = completedChunks.length;
        metadata.FileSize = (totalSize / 1024 / 1024).toFixed(2);
        metadata.MergeMethod = "Telegram Chunked";

        // 保存代理域名配置（如果有）
        if (tgChannel.proxyUrl) {
            metadata.TgProxyUrl = tgChannel.proxyUrl;
        }

        // 将分片信息存储到value中
        const chunksData = JSON.stringify(chunks);

        // 写入数据库
        await db.put(finalFileId, chunksData, { metadata });

        // 异步结束上传
        waitUntil(endUpload(context, finalFileId, metadata));

        // 生成返回链接
        const returnFormat = url.searchParams.get('returnFormat') || 'default';
        const updatedReturnLink = returnFormat === 'full' 
            ? `${url.origin}/file/${finalFileId}` 
            : `/file/${finalFileId}`;

        return {
            success: true,
            result: [{ 'src': updatedReturnLink }]
        };

    } catch (error) {
        throw new MergeError(`Telegram merge failed: ${error.message}`, 'TELEGRAM_MERGE_FAILED');
    }
}

// 合并Discord分块信息
async function mergeDiscordChunksInfo(context, uploadId, completedChunks, metadata) {
    const { env, waitUntil, uploadConfig, url, specifiedChannelName } = context;
    const db = getDatabase(env);

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
            throw new MergeError('No available Discord channels', 'NO_DISCORD_CHANNELS');
        }

        console.log(`Merging Discord chunks for uploadId: ${uploadId}, selected channel: ${discordChannel.name || 'default'}`);

        const botToken = discordChannel.botToken;
        const channelId = discordChannel.channelId;

        // 按顺序排列分块
        const sortedChunks = completedChunks.sort((a, b) => a.index - b.index);

        // 计算总大小
        const totalSize = sortedChunks.reduce((sum, chunk) => sum + chunk.uploadResult.size, 0);

        // 构建分块信息数组（不存储 url 因为会过期，读取时通过 API 获取）
        const chunks = sortedChunks.map(chunk => ({
            index: chunk.index,
            messageId: chunk.uploadResult.messageId,
            // 注意：不存储 attachmentId 和 url，它们会在约24小时后过期
            size: chunk.uploadResult.size,
            fileName: chunk.uploadResult.fileName
        }));

        // 生成 finalFileId
        const finalFileId = await buildUniqueFileId(context, metadata.FileName, metadata.FileType);

        // 更新metadata
        metadata.Channel = "Discord";
        metadata.ChannelName = discordChannel.name;
        metadata.DiscordChannelId = channelId;
        metadata.IsChunked = true;
        metadata.TotalChunks = completedChunks.length;
        metadata.FileSize = (totalSize / 1024 / 1024).toFixed(2);
        metadata.MergeMethod = "Discord Chunked";

        // 保存代理配置（如果有）
        if (discordChannel.proxyUrl) {
            metadata.DiscordProxyUrl = discordChannel.proxyUrl;
        }

        // 将分片信息存储到value中
        const chunksData = JSON.stringify(chunks);

        // 写入数据库
        await db.put(finalFileId, chunksData, { metadata });

        // 异步结束上传
        waitUntil(endUpload(context, finalFileId, metadata));

        // 生成返回链接
        const returnFormat = url.searchParams.get('returnFormat') || 'default';
        const updatedReturnLink = returnFormat === 'full' 
            ? `${url.origin}/file/${finalFileId}` 
            : `/file/${finalFileId}`;

        return {
            success: true,
            result: [{ 'src': updatedReturnLink }]
        };

    } catch (error) {
        throw new MergeError(`Discord merge failed: ${error.message}`, 'DISCORD_MERGE_FAILED');
    }
}

// 辅助函数：获取指定的渠道
function getSpecifiedChannel(channels, channelName) {
    if (!channelName || !channels || !channels.length) {
        return null;
    }
    
    return channels.find(ch => ch.name === channelName);
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

// 带缓存的分块状态检查
async function checkChunkUploadStatusesWithCache(env, uploadId, totalChunks) {
    // 尝试从缓存获取
    const cachedStatus = cacheGet('CHUNK_STATUS', uploadId);
    if (cachedStatus) {
        return cachedStatus;
    }
    
    // 实际检查分块状态
    const status = await checkChunkUploadStatuses(env, uploadId, totalChunks);
    
    // 缓存结果
    cacheSet('CHUNK_STATUS', uploadId, status);
    
    return status;
}

// 清除上传相关的缓存
function clearCacheForUpload(uploadId) {
    memoryCache.delete(`CHUNK_STATUS_${uploadId}`);
    memoryCache.delete(`UPLOAD_SESSION_${uploadId}`);
}

// 清理失败的上传
async function cleanupFailedUpload(context) {
    const { env, uploadId } = context;
    
    if (!uploadId) return;
    
    try {
        const promises = [];
        
        // 清理分块数据
        promises.push(cleanupChunkData(env, uploadId));
        
        // 清理上传会话
        promises.push(cleanupUploadSession(env, uploadId));
        
        // 清除缓存
        clearCacheForUpload(uploadId);
        
        await Promise.all(promises);
    } catch (error) {
        console.error('Failed to cleanup failed upload:', error);
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
async function logMergeError(env, error, context = {}) {
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
                requestId: context.requestId,
                ...context
            }
        };
        
        await env.KV.put(`merge_error_${errorId}`, JSON.stringify(errorLog), {
            expirationTtl: 7 * 24 * 60 * 60 // 保存7天
        });
        
        return errorId;
    } catch (logError) {
        console.error('Failed to log merge error:', logError);
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
