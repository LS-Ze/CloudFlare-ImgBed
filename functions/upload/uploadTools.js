import { fetchSecurityConfig } from "../utils/sysConfig";
import { purgeCFCache } from "../utils/purgeCache";
import { addFileToIndex } from "../utils/indexManager.js";
import { getDatabase } from '../utils/databaseAdapter.js';

// ==========================================
// 常量定义
// ==========================================

const ALLOWED_FILE_EXTENSIONS = new Set([
    'jpeg', 'jpg', 'png', 'gif', 'webp',
    'mp4', 'mp3', 'ogg',
    'wav', 'flac', 'aac', 'opus',
    'doc', 'docx', 'ppt', 'pptx', 'xls', 'xlsx', 'pdf',
    'txt', 'md', 'json', 'xml', 'html', 'css', 'js', 'ts', 
    'go', 'java', 'php', 'py', 'rb', 'sh', 'bat', 'cmd', 'ps1', 'psm1',
    'psd', 'ai', 'sketch', 'fig', 'svg', 'eps',
    'zip', 'rar', '7z', 'tar', 'gz', 'bz2', 'xz',
    'apk', 'exe', 'msi', 'dmg', 'iso', 'torrent',
    'ico', 'ttf', 'otf', 'woff', 'woff2', 'eot',
    'crx', 'xpi', 'deb', 'rpm', 'jar', 'war', 'ear',
    'img', 'vdi', 'ova', 'ovf', 'qcow2', 'vmdk', 'vhd', 'vhdx', 'pvm', 'dsk', 'hdd',
    'bin', 'cue', 'mds', 'mdf', 'nrg', 'ccd', 'cif', 'c2d', 'daa', 'b6t', 'b5t', 'bwt', 'isz', 'cdi', 'flp', 'uif', 'xdi', 'sdi'
]);

const DEFAULT_SHORT_ID_LENGTH = 8;
const MAX_FILE_ID_RETRIES = 1000;
const FETCH_TIMEOUT = 8000;

// ==========================================
// 响应处理工具
// ==========================================

/**
 * 创建统一的响应对象
 */
export function createResponse(body, options = {}) {
    const defaultHeaders = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization, authCode',
        'Access-Control-Max-Age': '86400',
    };

    return new Response(body, {
        ...options,
        headers: {
            ...defaultHeaders,
            ...options.headers
        }
    });
}

/**
 * 带超时的fetch函数
 */
export async function fetchWithTimeout(resource, options = {}) {
    const { timeout = FETCH_TIMEOUT } = options;
    
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeout);
    
    try {
        const response = await fetch(resource, {
            ...options,
            signal: controller.signal
        });
        clearTimeout(id);
        return response;
    } catch (error) {
        if (error.name === 'AbortError') {
            throw new Error(`Request timed out after ${timeout}ms`);
        }
        throw error;
    }
}

// ==========================================
// ID生成工具
// ==========================================

/**
 * 生成短链接ID
 */
export function generateShortId(length = DEFAULT_SHORT_ID_LENGTH) {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
}

/**
 * 构建唯一文件ID
 */
export async function buildUniqueFileId(context, fileName, fileType = 'application/octet-stream') {
    const { env, url } = context;
    const db = getDatabase(env);

    // 获取文件扩展名
    let fileExt = getFileExtension(fileName, fileType);
    
    // 验证文件扩展名
    if (!isExtValid(fileExt)) {
        fileExt = getFileExtensionFromMimeType(fileType);
    }

    // 处理文件名，移除特殊字符
    fileName = sanitizeFileName(fileName);

    // 获取上传参数
    const nameType = url.searchParams.get('uploadNameType') || 'default';
    const uploadFolder = url.searchParams.get('uploadFolder') || '';
    const normalizedFolder = normalizeFolderPath(uploadFolder);

    // 对于短链接类型，直接生成唯一ID
    if (nameType === 'short') {
        return generateUniqueShortId(db, normalizedFolder, fileExt);
    }

    // 生成基础ID
    const baseId = generateBaseFileId(nameType, normalizedFolder, fileName, fileExt);

    // 检查基础ID是否已存在
    if (await isFileIdUnique(db, baseId)) {
        return baseId;
    }

    // 如果已存在，添加计数器
    return generateUniqueFileIdWithCounter(db, nameType, normalizedFolder, fileName, fileExt, baseId);
}

/**
 * 获取文件扩展名
 */
function getFileExtension(fileName, fileType) {
    let ext = fileName.split('.').pop();
    if (!ext || ext === fileName) {
        ext = getFileExtensionFromMimeType(fileType);
    }
    return ext.toLowerCase();
}

/**
 * 从MIME类型获取文件扩展名
 */
function getFileExtensionFromMimeType(mimeType) {
    const ext = mimeType.split('/').pop();
    return (ext && ext !== mimeType) ? ext.toLowerCase() : 'unknown';
}

/**
 * 生成基础文件ID
 */
function generateBaseFileId(nameType, folder, fileName, fileExt) {
    const uniqueIndex = Date.now() + Math.floor(Math.random() * 10000);
    
    switch (nameType) {
        case 'index':
            return folder ? `${folder}/${uniqueIndex}.${fileExt}` : `${uniqueIndex}.${fileExt}`;
        
        case 'origin':
            return folder ? `${folder}/${fileName}` : fileName;
        
        default:
            return folder ? `${folder}/${uniqueIndex}_${fileName}` : `${uniqueIndex}_${fileName}`;
    }
}

/**
 * 生成唯一的短ID
 */
async function generateUniqueShortId(db, folder, fileExt) {
    for (let i = 0; i < MAX_FILE_ID_RETRIES; i++) {
        const shortId = generateShortId();
        const fullId = folder ? `${folder}/${shortId}.${fileExt}` : `${shortId}.${fileExt}`;
        if (await isFileIdUnique(db, fullId)) {
            return fullId;
        }
    }
    throw new Error('无法生成唯一的短文件ID');
}

/**
 * 生成带计数器的唯一文件ID
 */
async function generateUniqueFileIdWithCounter(db, nameType, folder, fileName, fileExt, baseId) {
    for (let counter = 1; counter <= MAX_FILE_ID_RETRIES; counter++) {
        let duplicateId;
        
        if (nameType === 'index') {
            const baseName = baseId.split('.')[0];
            duplicateId = folder ? 
                `${folder}/${baseName}(${counter}).${fileExt}` : 
                `${baseName}(${counter}).${fileExt}`;
        
        } else if (nameType === 'origin') {
            const nameWithoutExt = fileName.substring(0, fileName.lastIndexOf('.')) || fileName;
            const ext = fileName.substring(fileName.lastIndexOf('.')) || `.${fileExt}`;
            duplicateId = folder ? 
                `${folder}/${nameWithoutExt}(${counter})${ext}` : 
                `${nameWithoutExt}(${counter})${ext}`;
        
        } else {
            const nameWithoutExt = baseId.substring(0, baseId.lastIndexOf('.')) || baseId;
            const ext = baseId.substring(baseId.lastIndexOf('.')) || `.${fileExt}`;
            duplicateId = folder ? 
                `${folder}/${nameWithoutExt}(${counter})${ext}` : 
                `${nameWithoutExt}(${counter})${ext}`;
        }

        if (await isFileIdUnique(db, duplicateId)) {
            return duplicateId;
        }
    }
    
    throw new Error('无法生成唯一的文件ID');
}

/**
 * 检查文件ID是否唯一
 */
async function isFileIdUnique(db, fileId) {
    try {
        return await db.get(fileId) === null;
    } catch (error) {
        console.error(`Error checking file ID uniqueness: ${fileId}`, error);
        return false;
    }
}

// ==========================================
// 文件处理工具
// ==========================================

/**
 * 处理文件名中的特殊字符
 */
export function sanitizeFileName(fileName) {
    if (!fileName) return '';
    
    fileName = decodeURIComponent(fileName);
    fileName = fileName.split('/').pop();
    
    // 移除不安全字符
    const unsafeCharsRe = /[\\\/:\*\?"'<>\| \(\)\[\]\{\}#%\^`~;@&=\+\$,]/g;
    return fileName.replace(unsafeCharsRe, '_');
}

/**
 * 检查文件扩展名是否有效
 */
export function isExtValid(fileExt) {
    return ALLOWED_FILE_EXTENSIONS.has(fileExt.toLowerCase());
}

/**
 * 规范化文件夹路径
 */
export function normalizeFolderPath(folderPath) {
    if (!folderPath) return '';
    
    return folderPath
        .replace(/^\/+/, '')       // 移除开头的斜杠
        .replace(/\/{2,}/g, '/')   // 将多个斜杠替换为单个斜杠
        .replace(/\/$/, '');       // 移除结尾的斜杠
}

// ==========================================
// IP地址工具
// ==========================================

/**
 * 从request中解析IP地址
 */
export function getUploadIp(request) {
    const ipHeaders = [
        "EO-Client-IP", "cf-connecting-ip", "x-real-ip", "x-forwarded-for",
        "x-client-ip", "x-host", "x-originating-ip", "x-cluster-client-ip",
        "forwarded-for", "forwarded", "via", "requester", "true-client-ip",
        "client-ip", "x-remote-ip", "fastly-client-ip", "akamai-origin-hop",
        "x-remote-addr", "x-remote-host", "x-client-ips"
    ];

    for (const header of ipHeaders) {
        const ip = request.headers.get(header);
        if (ip) {
            // 处理多个IP地址的情况
            const ips = ip.split(',').map(i => i.trim());
            return ips[0]; // 返回第一个IP地址
        }
    }

    return null;
}

/**
 * 检查上传IP是否被封禁
 */
export async function isBlockedUploadIp(env, uploadIp) {
    if (!uploadIp) return false;
    
    try {
        const db = getDatabase(env);
        const blockedList = await db.get("manage@blockipList");
        
        if (!blockedList) return false;
        
        const blockedIps = blockedList.split(',').map(ip => ip.trim());
        return blockedIps.includes(uploadIp);
    } catch (error) {
        console.error('Failed to check blocked IP:', error);
        // 如果数据库未配置，默认不阻止任何IP
        return false;
    }
}

/**
 * 获取IP地址的地理位置信息
 */
export async function getIPAddress(ip) {
    let address = '未知';
    try {
        const ipInfo = await fetchWithTimeout(`https://apimobile.meituan.com/locate/v2/ip/loc?rgeo=true&ip=${ip}`);
        const ipData = await ipInfo.json();

        if (ipInfo.ok && ipData.data) {
            const lng = ipData.data?.lng || 0;
            const lat = ipData.data?.lat || 0;

            // 读取具体地址
            const addressInfo = await fetchWithTimeout(`https://apimobile.meituan.com/group/v1/city/latlng/${lat},${lng}?tag=0`);
            const addressData = await addressInfo.json();

            if (addressInfo.ok && addressData.data) {
                // 根据各字段是否存在，拼接地址
                address = [
                    addressData.data.detail,
                    addressData.data.city,
                    addressData.data.province,
                    addressData.data.country
                ].filter(Boolean).join(', ');
            }
        }
    } catch (error) {
        console.error(`Error fetching IP address ${ip}:`, error);
    }
    return address;
}

// ==========================================
// 内容审查工具
// ==========================================

/**
 * 图像审查主函数
 */
export async function moderateContent(env, url) {
    try {
        const securityConfig = await fetchSecurityConfig(env);
        const uploadModerate = securityConfig?.upload?.moderate;

        // 如果未配置审查或未启用审查，直接返回"None"
        if (!uploadModerate?.enabled) {
            console.log('Content moderation is disabled');
            return "None";
        }

        const channel = uploadModerate.channel;
        console.log(`Using content moderation channel: ${channel}`);

        switch (channel) {
            case 'moderatecontent.com':
                return await moderateWithModerateContent(uploadModerate, url);
            
            case 'nsfwjs':
                return await moderateWithNsfwJs(uploadModerate, url);
            
            default:
                console.warn(`Unsupported moderation channel: ${channel}`);
                return "None";
        }
    } catch (error) {
        console.error('Content moderation failed:', error);
        return "None";
    }
}

/**
 * 使用 moderatecontent.com 进行内容审查
 */
async function moderateWithModerateContent(config, url) {
    try {
        const apiKey = config.moderateContentApiKey;
        
        if (!apiKey) {
            console.error('moderatecontent.com API key is missing');
            return "None";
        }

        const encodedUrl = encodeURIComponent(url);
        const apiUrl = `https://api.moderatecontent.com/moderate/?key=${apiKey}&url=${encodedUrl}`;
        
        console.log(`Calling moderatecontent.com API: ${apiUrl}`);
        
        const response = await fetchWithTimeout(apiUrl, { timeout: 10000 });
        
        if (!response.ok) {
            throw new Error(`API request failed with status: ${response.status}`);
        }

        const data = await response.json();
        console.log('moderatecontent.com response:', data);

        if (data.rating_label) {
            return data.rating_label;
        }
        
        console.warn('moderatecontent.com response does not contain rating_label');
        return "None";
    } catch (error) {
        console.error('moderatecontent.com moderation failed:', error);
        return "None";
    }
}

/**
 * 使用 nsfwjs 进行内容审查
 */
async function moderateWithNsfwJs(config, url) {
    try {
        const apiPath = config.nsfwApiPath;
        
        if (!apiPath) {
            console.error('nsfwjs API path is missing');
            return "None";
        }

        const encodedUrl = encodeURIComponent(url);
        const apiUrl = `${apiPath}?url=${encodedUrl}`;
        
        console.log(`Calling nsfwjs API: ${apiUrl}`);
        
        const response = await fetchWithTimeout(apiUrl, { timeout: 10000 });
        
        if (!response.ok) {
            throw new Error(`API request failed with status: ${response.status}`);
        }

        const data = await response.json();
        console.log('nsfwjs response:', data);

        // 检查API响应结构
        if (!data || !data.data) {
            throw new Error('Invalid API response structure');
        }

        const nsfwScore = data.data.nsfw || 0;
        const isNsfw = data.data.is_nsfw || false;

        console.log(`NSFW score: ${nsfwScore}, is_nsfw: ${isNsfw}`);

        // 根据NSFW分数设置标签
        if (nsfwScore >= 0.9) {
            return "adult";
        } else if (nsfwScore >= 0.7) {
            return "teen";
        } else {
            return "everyone";
        }
    } catch (error) {
        console.error('nsfwjs moderation failed:', error);
        return "None";
    }
}

// ==========================================
// 缓存管理工具
// ==========================================

/**
 * 清除CDN缓存
 */
export async function purgeCDNCache(env, cdnUrl, url, normalizedFolder) {
    if (env.dev_mode === 'true') {
        console.log('Development mode: skipping CDN cache purge');
        return;
    }

    // 清除CDN缓存
    try {
        await purgeCFCache(env, cdnUrl);
        console.log(`Successfully purged CDN cache for: ${cdnUrl}`);
    } catch (error) {
        console.error('Failed to clear CDN cache:', error);
    }

    // 清除api/randomFileList API缓存
    try {
        const cache = caches.default;
        const cacheUrl = `${url.origin}/api/randomFileList?dir=${normalizedFolder}`;
        
        // 通过写入一个max-age=0的response来清除缓存
        const nullResponse = new Response(null, {
            headers: { 'Cache-Control': 'max-age=0' },
        });

        await cache.put(cacheUrl, nullResponse);
        console.log(`Successfully cleared cache for: ${cacheUrl}`);
    } catch (error) {
        console.error('Failed to clear cache:', error);
    }
}

/**
 * 结束上传流程：清除缓存，维护索引
 */
export async function endUpload(context, fileId, metadata) {
    const { env, url } = context;

    try {
        // 清除CDN缓存
        const cdnUrl = `https://${url.hostname}/file/${fileId}`;
        const normalizedFolder = normalizeFolderPath(url.searchParams.get('uploadFolder') || '');
        await purgeCDNCache(env, cdnUrl, url, normalizedFolder);

        // 更新文件索引（索引更新时会自动计算容量统计）
        await addFileToIndex(context, fileId, metadata);
        
        console.log(`Upload completed successfully for file: ${fileId}`);
    } catch (error) {
        console.error(`Error completing upload for file ${fileId}:`, error);
        throw error;
    }
}

// ==========================================
// 渠道选择工具
// ==========================================

/**
 * 基于uploadId的一致性渠道选择
 */
export function selectConsistentChannel(channels, uploadId, loadBalanceEnabled) {
    if (!loadBalanceEnabled || !channels || channels.length === 0) {
        return channels ? channels[0] : null;
    }

    // 使用uploadId的哈希值来选择渠道，确保相同uploadId总是选择相同渠道
    let hash = 0;
    for (let i = 0; i < uploadId.length; i++) {
        const char = uploadId.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash; // 转换为32位整数
    }

    const index = Math.abs(hash) % channels.length;
    return channels[index];
}
