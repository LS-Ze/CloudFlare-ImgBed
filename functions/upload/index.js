import { userAuthCheck, UnauthorizedResponse } from "../utils/userAuth";
import { fetchUploadConfig, fetchSecurityConfig } from "../utils/sysConfig";
import {
    createResponse, getUploadIp, getIPAddress, isExtValid,
    moderateContent, purgeCDNCache, isBlockedUploadIp, buildUniqueFileId, endUpload
} from "./uploadTools";
import { initializeChunkedUpload, handleChunkUpload, uploadLargeFileToTelegram, handleCleanupRequest } from "./chunkUpload";
import { handleChunkMerge } from "./chunkMerge";
import { TelegramAPI } from "../utils/telegramAPI";
import { DiscordAPI } from "../utils/discordAPI";
import { HuggingFaceAPI } from "../utils/huggingfaceAPI";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { getDatabase } from '../utils/databaseAdapter.js';

// 缓存配置
const CACHE_CONFIG = {
    UPLOAD_CONFIG: { ttl: 300, maxSize: 100 }, // 5分钟缓存，最多100个配置
    SECURITY_CONFIG: { ttl: 300, maxSize: 100 }, // 5分钟缓存，最多100个配置
    IP_GEOLOCATION: { ttl: 86400, maxSize: 1000 } // 24小时缓存，最多1000个IP
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
    CloudflareR2: uploadFileToCloudflareR2,
    S3: uploadFileToS3,
    TelegramNew: uploadFileToTelegram,
    Discord: uploadFileToDiscord,
    HuggingFace: uploadFileToHuggingFace,
    External: uploadFileToExternal
};

export async function onRequest(context) {  // Contents of context object
    const { request, env, params, waitUntil, next, data } = context;

    try {
        // 解析请求的URL，存入 context
        const url = new URL(request.url);
        context.url = url;

        // 读取各项配置，存入 context（带缓存）
        const securityConfig = await fetchSecurityConfigWithCache(env);
        const uploadConfig = await fetchUploadConfigWithCache(env, context);

        context.securityConfig = securityConfig;
        context.uploadConfig = uploadConfig;

        // 鉴权
        const requiredPermission = 'upload';
        if (!await userAuthCheck(env, url, request, requiredPermission)) {
            return UnauthorizedResponse('Unauthorized');
        }

        // 获得上传IP - 使用改进的getUploadIp函数
        const uploadIp = getUploadIp(request);
        context.uploadIp = uploadIp;
        
        // 判断上传ip是否被封禁（增强错误处理）
        if (uploadIp !== 'unknown') {
            const isBlockedIp = await isBlockedUploadIp(env, uploadIp);
            if (isBlockedIp) {
                return createResponse('Error: Your IP is blocked', { status: 403 });
            }
        } else {
            console.warn('Failed to get client IP address, skipping IP block check');
        }

        // 检查是否为清理请求
        const cleanupRequest = url.searchParams.get('cleanup') === 'true';
        if (cleanupRequest) {
            const uploadId = url.searchParams.get('uploadId');
            const totalChunks = parseInt(url.searchParams.get('totalChunks')) || 0;
            return await handleCleanupRequest(context, uploadId, totalChunks);
        }

        // 检查是否为初始化分块上传请求
        const initChunked = url.searchParams.get('initChunked') === 'true';
        if (initChunked) {
            return await initializeChunkedUpload(context);
        }

        // 检查是否为分块上传
        const isChunked = url.searchParams.get('chunked') === 'true';
        const isMerge = url.searchParams.get('merge') === 'true';

        if (isChunked) {
            if (isMerge) {
                return await handleChunkMerge(context);
            } else {
                return await handleChunkUpload(context);
            }
        }

        // 处理非分块文件上传
        return await processFileUpload(context);

    } catch (error) {
        const errorId = await logUploadError(env, error, {
            requestId: context.requestId,
            uploadIp: context.uploadIp
        });
        
        return createResponse(`Error: ${error.message} (Error ID: ${errorId})`, { 
            status: error.status || 500 
        });
    }
}

// 通用文件上传处理函数
async function processFileUpload(context, formdata = null) {
    const { request, url, env } = context;

    try {
        // 解析表单数据
        formdata = formdata || await parseFormDataWithValidation(request);

        // 将 formdata 存储在 context 中
        context.formdata = formdata;

        // 获得上传渠道类型
        const urlParamUploadChannel = url.searchParams.get('uploadChannel');
        // 获得指定的渠道名称（可选）
        const urlParamChannelName = url.searchParams.get('channelName');

        // 获取IP地址 - 使用带缓存的方法
        const uploadIp = getUploadIp(request);
        const ipAddress = await getIPAddressWithCache(uploadIp);

        // 获取上传文件夹路径
        let uploadFolder = url.searchParams.get('uploadFolder') || '';

        // 确定上传渠道
        const uploadChannel = getUploadChannel(urlParamUploadChannel);

        // 将指定的渠道名称存入 context，供后续上传函数使用
        context.specifiedChannelName = urlParamChannelName || null;

        // 获取文件信息
        const time = Date.now();
        const file = formdata.get('file');
        
        // 验证文件对象
        if (!file) {
            throw new UploadError('No file provided', 'MISSING_FILE', 400);
        }

        const fileType = file.type;
        let fileName = file.name;
        const fileSize = (file.size / 1024 / 1024).toFixed(2); // 文件大小，单位MB

        // 检查fileType和fileName是否存在
        if (!fileType || !fileName) {
            throw new UploadError('fileType or fileName is wrong, check the integrity of this file!', 'INVALID_FILE_INFO', 400);
        }

        // 如果上传文件夹路径为空，尝试从文件名中获取
        if (!uploadFolder) {
            uploadFolder = fileName.split('/').slice(0, -1).join('/');
        }
        
        // 处理文件夹路径格式，确保没有开头的/
        const normalizedFolder = normalizeFolderPath(uploadFolder);

        const metadata = {
            FileName: fileName,
            FileType: fileType,
            FileSize: fileSize,
            UploadIP: uploadIp,
            UploadAddress: ipAddress,
            ListType: "None",
            TimeStamp: time,
            Label: "None",
            Directory: normalizedFolder === '' ? '' : normalizedFolder + '/',
            Tags: []
        };

        // 处理文件扩展名
        let fileExt = getFileExtension(fileName, fileType);
        if (!isExtValid(fileExt)) {
            fileExt = 'unknown'; // 默认扩展名
        }

        // 构建文件ID
        const fullId = await buildUniqueFileId(context, fileName, fileType);
        context.fileId = fullId;

        // 获得返回链接格式, default为返回/file/id, full为返回完整链接
        const returnFormat = url.searchParams.get('returnFormat') || 'default';
        const returnLink = getReturnLink(url, fullId, returnFormat);

        /* ====================================不同渠道上传======================================= */
        // 出错是否切换渠道自动重试，默认开启
        const autoRetry = url.searchParams.get('autoRetry') !== 'false';

        // 使用策略模式上传到指定渠道
        let response = await uploadToChannel(
            uploadChannel, context, fullId, metadata, 
            returnLink, fileExt, fileName, fileType
        );

        // 如果上传成功或不启用自动重试，直接返回
        if (response.status === 200 || !autoRetry) {
            return response;
        }

        // 上传失败，开始自动切换渠道重试
        const errorText = await response.text();
        return await tryRetry(
            errorText, context, uploadChannel, fullId, metadata, 
            fileExt, fileName, fileType, returnLink
        );

    } catch (error) {
        const errorId = await logUploadError(env, error, {
            fileId: context.fileId,
            uploadChannel: context.uploadChannel
        });
        
        return createResponse(`Error: ${error.message} (Error ID: ${errorId})`, { 
            status: error.status || 500 
        });
    }
}

// 上传到Cloudflare R2
async function uploadFileToCloudflareR2(context, fullId, metadata, returnLink) {
    const { env, waitUntil, uploadConfig, formdata, specifiedChannelName } = context;
    const db = getDatabase(env);

    try {
        // 检查R2数据库是否配置
        if (!env.img_r2) {
            throw new UploadError('Please configure R2 database', 'R2_NOT_CONFIGURED', 500);
        }

        // 检查 R2 渠道是否启用
        const r2Settings = uploadConfig.cfr2;
        if (!r2Settings.channels || r2Settings.channels.length === 0) {
            throw new UploadError('No R2 channel provided', 'NO_CHANNEL_PROVIDED', 400);
        }

        // 选择渠道：优先使用指定的渠道名称
        let r2Channel = getSpecifiedChannel(r2Settings.channels, specifiedChannelName);
        if (!r2Channel) {
            r2Channel = r2Settings.channels[0];
        }

        const R2DataBase = env.img_r2;

        // 写入R2数据库，获取实际存储大小
        const r2Object = await R2DataBase.put(fullId, formdata.get('file'));

        // 更新metadata
        metadata.Channel = "CloudflareR2";
        metadata.ChannelName = r2Channel.name || "R2_env";
        // 使用 R2 返回的实际文件大小
        if (r2Object && r2Object.size) {
            metadata.FileSize = (r2Object.size / 1024 / 1024).toFixed(2);
        }

        // 图像审查，采用R2的publicUrl
        const R2PublicUrl = r2Channel.publicUrl;
        const moderateUrl = `${R2PublicUrl}/${fullId}`;
        metadata.Label = await moderateContent(env, moderateUrl);

        // 写入数据库
        await db.put(fullId, "", { metadata });

        // 结束上传
        waitUntil(endUpload(context, fullId, metadata));

        // 成功上传，将文件ID返回给客户端
        return createResponse(
            JSON.stringify([{ 'src': `${returnLink}` }]),
            {
                status: 200,
                headers: {
                    'Content-Type': 'application/json',
                }
            }
        );

    } catch (error) {
        throw new UploadError(`Cloudflare R2 upload failed: ${error.message}`, 'R2_UPLOAD_FAILED', 500);
    }
}

// 上传到 S3（支持自定义端点）
async function uploadFileToS3(context, fullId, metadata, returnLink) {
    const { env, waitUntil, uploadConfig, securityConfig, url, formdata, specifiedChannelName } = context;
    const db = getDatabase(env);

    try {
        const uploadModerate = securityConfig.upload.moderate;

        const s3Settings = uploadConfig.s3;
        const s3Channels = s3Settings.channels;
        
        // 选择渠道：优先使用指定的渠道名称
        let s3Channel = getSpecifiedChannel(s3Channels, specifiedChannelName);
        if (!s3Channel) {
            s3Channel = s3Settings.loadBalance.enabled
                ? s3Channels[Math.floor(Math.random() * s3Channels.length)]
                : s3Channels[0];
        }

        if (!s3Channel) {
            throw new UploadError('No S3 channel provided', 'NO_CHANNEL_PROVIDED', 400);
        }

        const { endpoint, pathStyle, accessKeyId, secretAccessKey, bucketName, region } = s3Channel;

        // 创建 S3 客户端
        const s3Client = new S3Client({
            region: region || "auto", // R2 可用 "auto"
            endpoint, // 自定义 S3 端点
            credentials: {
                accessKeyId,
                secretAccessKey
            },
            forcePathStyle: pathStyle // 是否启用路径风格
        });

        // 获取文件
        const file = formdata.get("file");
        if (!file) throw new UploadError("No file provided", "MISSING_FILE", 400);

        // 转换 Blob 为 Uint8Array
        const arrayBuffer = await file.arrayBuffer();
        const uint8Array = new Uint8Array(arrayBuffer);

        const s3FileName = fullId;

        // S3 上传参数
        const putObjectParams = {
            Bucket: bucketName,
            Key: s3FileName,
            Body: uint8Array, // 直接使用 Blob
            ContentType: file.type
        };

        // 执行上传
        await s3Client.send(new PutObjectCommand(putObjectParams));

        // 更新 metadata
        metadata.Channel = "S3";
        metadata.ChannelName = s3Channel.name;

        const s3ServerDomain = endpoint.replace(/https?:\/\//, "");
        if (pathStyle) {
            metadata.S3Location = `https://${s3ServerDomain}/${bucketName}/${s3FileName}`; // 采用路径风格的 URL
        } else {
            metadata.S3Location = `https://${bucketName}.${s3ServerDomain}/${s3FileName}`; // 采用虚拟主机风格的 URL
        }
        metadata.S3Endpoint = endpoint;
        metadata.S3PathStyle = pathStyle;
        metadata.S3AccessKeyId = accessKeyId;
        metadata.S3SecretAccessKey = secretAccessKey;
        metadata.S3Region = region || "auto";
        metadata.S3BucketName = bucketName;
        metadata.S3FileKey = s3FileName;

        // 图像审查
        if (uploadModerate && uploadModerate.enabled) {
            try {
                await db.put(fullId, "", { metadata });
            } catch {
                throw new UploadError("Failed to write to KV database", "KV_WRITE_FAILED", 500);
            }

            const moderateUrl = `https://${url.hostname}/file/${fullId}`;
            await purgeCDNCache(env, moderateUrl, url);
            metadata.Label = await moderateContent(env, moderateUrl);
        }

        // 写入数据库
        await db.put(fullId, "", { metadata });

        // 结束上传
        waitUntil(endUpload(context, fullId, metadata));

        return createResponse(JSON.stringify([{ src: returnLink }]), {
            status: 200,
            headers: {
                "Content-Type": "application/json",
            },
        });

    } catch (error) {
        throw new UploadError(`S3 upload failed: ${error.message}`, 'S3_UPLOAD_FAILED', 500);
    }
}

// 上传到Telegram
async function uploadFileToTelegram(context, fullId, metadata, fileExt, fileName, fileType, returnLink) {
    const { env, waitUntil, uploadConfig, url, formdata, specifiedChannelName } = context;
    const db = getDatabase(env);

    try {
        // 选择一个 Telegram 渠道上传
        const tgSettings = uploadConfig.telegram;
        const tgChannels = tgSettings.channels;
        
        let tgChannel = getSpecifiedChannel(tgChannels, specifiedChannelName);
        // 未指定或未找到指定渠道，使用负载均衡或第一个
        if (!tgChannel) {
            tgChannel = tgSettings.loadBalance.enabled 
                ? tgChannels[Math.floor(Math.random() * tgChannels.length)] 
                : tgChannels[0];
        }
        
        if (!tgChannel) {
            throw new UploadError('No Telegram channel provided', 'NO_CHANNEL_PROVIDED', 400);
        }

        const tgBotToken = tgChannel.botToken;
        const tgChatId = tgChannel.chatId;
        const tgProxyUrl = tgChannel.proxyUrl || '';
        const file = formdata.get('file');
        const fileSize = file.size;

        const telegramAPI = new TelegramAPI(tgBotToken, tgProxyUrl);

        // 16MB 分片阈值 (TG Bot getFile download limit: 20MB, leave 4MB safety margin)
        const CHUNK_SIZE = 16 * 1024 * 1024; // 16MB

        if (fileSize > CHUNK_SIZE) {
            // 大文件分片上传
            return await uploadLargeFileToTelegram(context, file, fullId, metadata, fileName, fileType, returnLink, tgBotToken, tgChatId, tgChannel);
        }

        // 由于TG会把gif后缀的文件转为视频，所以需要修改后缀名绕过限制
        const processedFile = processTelegramFile(formdata.get('file'), fileName, fileExt, fileType);
        formdata.set('file', processedFile);

        // 选择对应的发送接口
        const sendFunction = getTelegramSendFunction(fileType, fileExt, url);

        // 上传文件到 Telegram
        const response = await telegramAPI.sendFile(
            formdata.get('file'), 
            tgChatId, 
            sendFunction.url, 
            sendFunction.type
        );
        
        const fileInfo = telegramAPI.getFileInfo(response);
        const filePath = await telegramAPI.getFilePath(fileInfo.file_id);
        const id = fileInfo.file_id;
        
        // 更新FileSize
        metadata.FileSize = (fileInfo.file_size / 1024 / 1024).toFixed(2);

        // 图像审查（使用代理域名或官方域名）
        const moderateDomain = tgProxyUrl ? `https://${tgProxyUrl}` : 'https://api.telegram.org';
        const moderateUrl = `${moderateDomain}/file/bot${tgBotToken}/${filePath}`;
        metadata.Label = await moderateContent(env, moderateUrl);

        // 更新metadata，写入KV数据库
        metadata.Channel = "TelegramNew";
        metadata.ChannelName = tgChannel.name;
        metadata.TgFileId = id;
        metadata.TgChatId = tgChatId;
        metadata.TgBotToken = tgBotToken;
        
        // 保存代理域名配置
        if (tgProxyUrl) {
            metadata.TgProxyUrl = tgProxyUrl;
        }
        
        await db.put(fullId, "", { metadata });

        // 结束上传
        waitUntil(endUpload(context, fullId, metadata));

        // 将响应返回给客户端
        return createResponse(
            JSON.stringify([{ 'src': `${returnLink}` }]),
            {
                status: 200,
                headers: {
                    'Content-Type': 'application/json',
                }
            }
        );

    } catch (error) {
        throw new UploadError(`Telegram upload failed: ${error.message}`, 'TELEGRAM_UPLOAD_FAILED', 500);
    }
}

// 外链渠道
async function uploadFileToExternal(context, fullId, metadata, returnLink) {
    const { env, waitUntil, formdata } = context;
    const db = getDatabase(env);

    try {
        // 直接将外链写入metadata
        metadata.Channel = "External";
        metadata.ChannelName = "External";
        
        // 从 formdata 中获取外链
        const extUrl = formdata.get('url');
        if (!extUrl) {
            throw new UploadError('No url provided', 'MISSING_URL', 400);
        }
        
        metadata.ExternalLink = extUrl;
        
        // 写入KV数据库
        await db.put(fullId, "", { metadata });

        // 结束上传
        waitUntil(endUpload(context, fullId, metadata));

        // 返回结果
        return createResponse(
            JSON.stringify([{ 'src': `${returnLink}` }]),
            {
                status: 200,
                headers: {
                    'Content-Type': 'application/json',
                }
            }
        );

    } catch (error) {
        throw new UploadError(`External upload failed: ${error.message}`, 'EXTERNAL_UPLOAD_FAILED', 500);
    }
}

// 上传到 Discord
async function uploadFileToDiscord(context, fullId, metadata, returnLink) {
    const { env, waitUntil, uploadConfig, formdata, specifiedChannelName } = context;
    const db = getDatabase(env);

    try {
        // 获取 Discord 渠道配置
        const discordSettings = uploadConfig.discord;
        if (!discordSettings || !discordSettings.channels || discordSettings.channels.length === 0) {
            throw new UploadError('No Discord channel configured', 'NO_CHANNEL_CONFIGURED', 400);
        }

        // 选择渠道：优先使用指定的渠道名称
        const discordChannels = discordSettings.channels;
        let discordChannel = getSpecifiedChannel(discordChannels, specifiedChannelName);
        
        if (!discordChannel) {
            discordChannel = discordSettings.loadBalance?.enabled
                ? discordChannels[Math.floor(Math.random() * discordChannels.length)]
                : discordChannels[0];
        }

        if (!discordChannel || !discordChannel.botToken || !discordChannel.channelId) {
            throw new UploadError('Discord channel not properly configured', 'CHANNEL_CONFIG_ERROR', 400);
        }

        const file = formdata.get('file');
        const fileSize = file.size;
        const fileName = metadata.FileName;

        // Discord 文件大小限制：Nitro 会员 25MB，免费用户 10MB
        const isNitro = discordChannel.isNitro || false;
        const DISCORD_MAX_SIZE = isNitro ? 25 * 1024 * 1024 : 10 * 1024 * 1024;
        
        if (fileSize > DISCORD_MAX_SIZE) {
            const limitMB = isNitro ? 25 : 10;
            throw new UploadError(
                `File size exceeds Discord limit (${limitMB}MB), please use another channel`, 
                'FILE_TOO_LARGE', 
                413
            );
        }

        const discordAPI = new DiscordAPI(discordChannel.botToken);

        // 上传文件到 Discord
        const response = await discordAPI.sendFile(file, discordChannel.channelId, fileName);
        const fileInfo = discordAPI.getFileInfo(response);

        if (!fileInfo) {
            throw new UploadError('Failed to get file info from Discord response', 'INVALID_RESPONSE', 500);
        }

        // 更新 metadata
        metadata.Channel = "Discord";
        metadata.ChannelName = discordChannel.name || "Discord_env";
        metadata.FileSize = (fileInfo.file_size / 1024 / 1024).toFixed(2);
        metadata.DiscordMessageId = fileInfo.message_id;
        metadata.DiscordChannelId = discordChannel.channelId;
        metadata.DiscordBotToken = discordChannel.botToken;
        
        // 如果配置了代理 URL，保存代理信息
        if (discordChannel.proxyUrl) {
            metadata.DiscordProxyUrl = discordChannel.proxyUrl;
        }

        // 图像审查（使用 Discord CDN URL 或代理 URL）
        let moderateUrl = fileInfo.url;
        if (discordChannel.proxyUrl) {
            moderateUrl = fileInfo.url.replace('https://cdn.discordapp.com', `https://${discordChannel.proxyUrl}`);
        }
        metadata.Label = await moderateContent(env, moderateUrl);

        // 写入 KV 数据库
        await db.put(fullId, "", { metadata });

        // 结束上传
        waitUntil(endUpload(context, fullId, metadata));

        // 返回成功响应
        return createResponse(
            JSON.stringify([{ 'src': returnLink }]),
            {
                status: 200,
                headers: { 'Content-Type': 'application/json' }
            }
        );

    } catch (error) {
        throw new UploadError(`Discord upload failed: ${error.message}`, 'DISCORD_UPLOAD_FAILED', 500);
    }
}

// 上传到 HuggingFace
async function uploadFileToHuggingFace(context, fullId, metadata, returnLink) {
    const { env, waitUntil, uploadConfig, formdata, specifiedChannelName } = context;
    const db = getDatabase(env);

    try {
        // 获取 HuggingFace 渠道配置
        const hfSettings = uploadConfig.huggingface;
        if (!hfSettings || !hfSettings.channels || hfSettings.channels.length === 0) {
            throw new UploadError('No HuggingFace channel configured', 'NO_CHANNEL_CONFIGURED', 400);
        }

        // 选择渠道：优先使用指定的渠道名称
        const hfChannels = hfSettings.channels;
        let hfChannel = getSpecifiedChannel(hfChannels, specifiedChannelName);
        
        if (!hfChannel) {
            hfChannel = hfSettings.loadBalance?.enabled
                ? hfChannels[Math.floor(Math.random() * hfChannels.length)]
                : hfChannels[0];
        }

        if (!hfChannel || !hfChannel.token || !hfChannel.repo) {
            throw new UploadError('HuggingFace channel not properly configured', 'CHANNEL_CONFIG_ERROR', 400);
        }

        const file = formdata.get('file');
        const fileName = metadata.FileName;
        // 获取前端预计算的 SHA256（如果有）
        const precomputedSha256 = formdata.get('sha256') || null;

        // 构建文件路径：直接使用 fullId（与其他渠道保持一致）
        const hfFilePath = fullId;

        const huggingfaceAPI = new HuggingFaceAPI(hfChannel.token, hfChannel.repo, hfChannel.isPrivate || false);

        // 上传文件到 HuggingFace（传入预计算的 SHA256）
        const result = await huggingfaceAPI.uploadFile(file, hfFilePath, `Upload ${fileName}`, precomputedSha256);

        if (!result.success) {
            throw new UploadError('Failed to upload file to HuggingFace', 'UPLOAD_FAILED', 500);
        }

        // 更新 metadata
        metadata.Channel = "HuggingFace";
        metadata.ChannelName = hfChannel.name || "HuggingFace_env";
        metadata.FileSize = (file.size / 1024 / 1024).toFixed(2);
        metadata.HfRepo = hfChannel.repo;
        metadata.HfFilePath = hfFilePath;
        metadata.HfToken = hfChannel.token;
        metadata.HfIsPrivate = hfChannel.isPrivate || false;
        metadata.HfFileUrl = result.fileUrl;

        // 图像审查（公开仓库直接访问，私有仓库需要代理）
        let moderateUrl = result.fileUrl;
        if (!hfChannel.isPrivate) {
            metadata.Label = await moderateContent(env, moderateUrl);
        } else {
            // 私有仓库暂不支持图像审查，标记为 None
            metadata.Label = "None";
        }

        // 写入 KV 数据库
        await db.put(fullId, "", { metadata });

        // 结束上传
        waitUntil(endUpload(context, fullId, metadata));

        // 返回成功响应
        return createResponse(
            JSON.stringify([{ 'src': returnLink }]),
            {
                status: 200,
                headers: { 'Content-Type': 'application/json' }
            }
        );

    } catch (error) {
        throw new UploadError(`HuggingFace upload failed: ${error.message}`, 'HUGGINGFACE_UPLOAD_FAILED', 500);
    }
}

// 自动切换渠道重试
async function tryRetry(err, context, uploadChannel, fullId, metadata, fileExt, fileName, fileType, returnLink) {
    const { env, url } = context;

    try {
        // 渠道列表（Discord 因为有 10MB 限制，放在最后尝试）
        const channelList = ['CloudflareR2', 'TelegramNew', 'S3', 'HuggingFace', 'Discord'];
        const errMessages = {};
        errMessages[uploadChannel] = `Error: ${uploadChannel} - ${err}`;

        // 创建并发控制器
        const concurrencyController = new ConcurrencyController(2);

        // 先用原渠道再试一次（关闭服务端压缩）
        url.searchParams.set('serverCompress', 'false');
        
        const retryTask = async () => {
            try {
                const retryRes = await uploadToChannel(
                    uploadChannel, context, fullId, metadata, 
                    returnLink, fileExt, fileName, fileType
                );
                
                if (retryRes.status === 200) {
                    return { success: true, response: retryRes };
                }
                
                const errorText = await retryRes.text();
                errMessages[`${uploadChannel}_retry`] = `Error: ${uploadChannel} retry - ${errorText}`;
                return { success: false };
            } catch (error) {
                errMessages[`${uploadChannel}_retry`] = `Error: ${uploadChannel} retry - ${error.message}`;
                return { success: false };
            }
        };

        const retryResult = await concurrencyController.addTask(retryTask);
        if (retryResult.success) {
            return retryResult.response;
        }

        // 准备其他渠道的重试任务
        const retryTasks = [];
        for (const channel of channelList) {
            if (channel !== uploadChannel) {
                retryTasks.push(async () => {
                    try {
                        const res = await uploadToChannel(
                            channel, context, fullId, metadata, 
                            returnLink, fileExt, fileName, fileType
                        );
                        
                        if (res.status === 200) {
                            return { success: true, channel, response: res };
                        }
                        
                        const errorText = await res.text();
                        errMessages[channel] = `Error: ${channel} - ${errorText}`;
                        return { success: false, channel };
                    } catch (error) {
                        errMessages[channel] = `Error: ${channel} - ${error.message}`;
                        return { success: false, channel };
                    }
                });
            }
        }

        // 并发执行重试任务
        const results = await Promise.all(
            retryTasks.map(task => concurrencyController.addTask(task))
        );

        // 检查是否有成功的重试
        for (const result of results) {
            if (result.success) {
                return result.response;
            }
        }

        // 所有渠道都失败
        return createResponse(JSON.stringify(errMessages), { status: 500 });

    } catch (error) {
        const errorId = await logUploadError(env, error, {
            fileId: fullId,
            uploadChannel
        });
        
        return createResponse(`Error: Retry process failed - ${error.message} (Error ID: ${errorId})`, { 
            status: 500 
        });
    }
}

/* ======= 辅助函数 ======= */

// 带缓存的配置获取
async function fetchSecurityConfigWithCache(env) {
    // 尝试从缓存获取
    const cachedConfig = cacheGet('SECURITY_CONFIG', 'default');
    if (cachedConfig) {
        return cachedConfig;
    }

    const config = await fetchSecurityConfig(env);
    
    // 缓存结果
    cacheSet('SECURITY_CONFIG', 'default', config);
    
    return config;
}

// 带缓存的上传配置获取
async function fetchUploadConfigWithCache(env, context) {
    // 尝试从缓存获取
    const cachedConfig = cacheGet('UPLOAD_CONFIG', 'default');
    if (cachedConfig) {
        return cachedConfig;
    }

    const config = await fetchUploadConfig(env, context);
    
    // 缓存结果
    cacheSet('UPLOAD_CONFIG', 'default', config);
    
    return config;
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

// 解析并验证表单数据
async function parseFormDataWithValidation(request) {
    try {
        return await request.formData();
    } catch (error) {
        throw new UploadError('Invalid form data', 'INVALID_FORM_DATA', 400);
    }
}

// 确定上传渠道
function getUploadChannel(urlParamUploadChannel) {
    switch (urlParamUploadChannel) {
        case 'telegram':
            return 'TelegramNew';
        case 'cfr2':
            return 'CloudflareR2';
        case 's3':
            return 'S3';
        case 'discord':
            return 'Discord';
        case 'huggingface':
            return 'HuggingFace';
        case 'external':
            return 'External';
        default:
            return 'TelegramNew';
    }
}

// 规范化文件夹路径
function normalizeFolderPath(folderPath) {
    if (!folderPath) return '';
    
    return folderPath
        .replace(/^\/+/, '') // 移除开头的/
        .replace(/\/{2,}/g, '/') // 替换多个连续的/为单个/
        .replace(/\/$/, ''); // 移除末尾的/
}

// 获取文件扩展名
function getFileExtension(fileName, fileType) {
    let fileExt = fileName.split('.').pop(); // 文件扩展名
    
    if (!fileExt || !isExtValid(fileExt)) {
        // 如果文件名中没有扩展名，尝试从文件类型中获取
        fileExt = fileType.split('/').pop();
        
        if (fileExt === fileType || !fileExt) {
            // Type中无法获取扩展名
            fileExt = 'unknown'; // 默认扩展名
        }
    }
    
    return fileExt;
}

// 获取返回链接
function getReturnLink(url, fullId, returnFormat = 'default') {
    if (returnFormat === 'full') {
        return `${url.origin}/file/${fullId}`;
    }
    return `/file/${fullId}`;
}

// 使用策略模式上传到指定渠道
async function uploadToChannel(channel, context, fullId, metadata, returnLink, fileExt, fileName, fileType) {
    const uploader = ChannelUploaders[channel];
    if (!uploader) {
        throw new UploadError(`Unsupported upload channel: ${channel}`, 'UNSUPPORTED_CHANNEL', 400);
    }
    
    if (channel === 'TelegramNew') {
        return uploader(context, fullId, metadata, fileExt, fileName, fileType, returnLink);
    }
    
    return uploader(context, fullId, metadata, returnLink);
}

// 处理Telegram文件（修改后缀名）
function processTelegramFile(file, fileName, fileExt, fileType) {
    // 由于TG会把gif后缀的文件转为视频，所以需要修改后缀名绕过限制
    if (fileExt === 'gif') {
        const newFileName = fileName.replace(/\.gif$/, '.jpeg');
        return new File([file], newFileName, { type: fileType });
    } else if (fileExt === 'webp') {
        const newFileName = fileName.replace(/\.webp$/, '.jpeg');
        return new File([file], newFileName, { type: fileType });
    }
    
    return file;
}

// 获取Telegram发送函数
function getTelegramSendFunction(fileType, fileExt, url) {
    // 选择对应的发送接口
    const fileTypeMap = {
        'image/': { 'url': 'sendPhoto', 'type': 'photo' },
        'video/': { 'url': 'sendVideo', 'type': 'video' },
        'audio/': { 'url': 'sendAudio', 'type': 'audio' },
        'application/pdf': { 'url': 'sendDocument', 'type': 'document' },
    };

    const defaultType = { 'url': 'sendDocument', 'type': 'document' };

    let sendFunction = Object.keys(fileTypeMap).find(key => fileType.startsWith(key))
        ? fileTypeMap[Object.keys(fileTypeMap).find(key => fileType.startsWith(key))]
        : defaultType;

    // GIF ICO 等发送接口特殊处理
    if (fileType === 'image/gif' || fileType === 'image/webp' || fileExt === 'gif' || fileExt === 'webp') {
        sendFunction = { 'url': 'sendAnimation', 'type': 'animation' };
    } else if (fileType === 'image/svg+xml' || fileType === 'image/x-icon') {
        sendFunction = { 'url': 'sendDocument', 'type': 'document' };
    }

    // 根据服务端压缩设置处理接口：从参数中获取serverCompress，如果为false，则使用sendDocument接口
    if (url.searchParams.get('serverCompress') === 'false') {
        sendFunction = { 'url': 'sendDocument', 'type': 'document' };
    }

    return sendFunction;
}

// 获取指定的渠道
function getSpecifiedChannel(channels, channelName) {
    if (!channelName || !channels || !channels.length) {
        return null;
    }
    
    return channels.find(ch => ch.name === channelName);
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
                channel: context.channel,
                fileId: context.fileId,
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
