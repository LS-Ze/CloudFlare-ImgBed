import { fetchSecurityConfig } from "../utils/sysConfig";
import { purgeCFCache } from "../utils/purgeCache";
import { addFileToIndex } from "../utils/indexManager.js";
import { getDatabase } from '../utils/databaseAdapter.js';

// 缓存配置
const CACHE_CONFIG = {
    IP_GEOLOCATION: { ttl: 24 * 60 * 60, maxSize: 1000 }, // 24小时缓存，最多1000个IP
    SECURITY_CONFIG: { ttl: 5 * 60, maxSize: 1 }, // 5分钟缓存
    MODERATION_RESULTS: { ttl: 1 * 60, maxSize: 1000 } // 1分钟缓存，最多1000个URL
};

// 内存缓存
const memoryCache = new Map();

// 统一错误类
export class WorkerError extends Error {
    constructor(message, code = 'INTERNAL_ERROR', status = 500, details = {}) {
        super(message);
        this.name = 'WorkerError';
        this.code = code;
        this.status = status;
        this.details = details;
        this.timestamp = Date.now();
    }
}

// 统一的响应创建函数
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

// 生成短链接
export function generateShortId(length = 8) {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let result = '';
    for (let i = 0; i < length; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
}

// 使用更安全的ID生成算法
export function generateSecureId(length = 12) {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    const array = new Uint8Array(length);
    crypto.getRandomValues(array);
    
    let result = '';
    for (let i = 0; i < length; i++) {
        result += chars[array[i] % chars.length];
    }
    
    return result;
}

// 获取IP地址
export async function getIPAddress(ip) {
    if (!ip) return '未知';
    
    // 尝试从缓存获取
    const cachedAddress = cacheGet('IP_GEOLOCATION', ip);
    if (cachedAddress) {
        return cachedAddress;
    }

    let address = '未知';
    try {
        const ipInfo = await fetch(`https://apimobile.meituan.com/locate/v2/ip/loc?rgeo=true&ip=${ip}`);
        const ipData = await ipInfo.json();

        if (ipInfo.ok && ipData.data) {
            const lng = ipData.data?.lng || 0;
            const lat = ipData.data?.lat || 0;

            // 读取具体地址
            const addressInfo = await fetch(`https://apimobile.meituan.com/group/v1/city/latlng/${lat},${lng}?tag=0`);
            const addressData = await addressInfo.json();

            if (addressInfo.ok && addressData.data) {
                // 根据各字段是否存在，拼接地址
                address = [
                    addressData.data.detail,
                    addressData.data.city,
                    addressData.data.province,
                    addressData.data.country
                ].filter(Boolean).join(', ');
                
                // 缓存结果
                cacheSet('IP_GEOLOCATION', ip, address);
            }
        }
    } catch (error) {
        console.error('Error fetching IP address:', error);
    }
    return address;
}

// 处理文件名中的特殊字符
export function sanitizeFileName(fileName) {
    if (!fileName) return 'unknown_file';
    
    fileName = decodeURIComponent(fileName);
    fileName = fileName.split('/').pop();

    const unsafeCharsRe = /[\\\/:\*\?"'<>\| \(\)\[\]\{\}#%\^`~;@&=\+\$,]/g;
    return fileName.replace(unsafeCharsRe, '_');
}

// 检查文件扩展名是否有效
export function isExtValid(fileExt) {
    if (!fileExt) return false;
    
    const validExtensions = new Set([
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
        'bin', 'cue', 'mds', 'mdf', 'nrg', 'ccd', 'cif', 'c2d', 'daa', 'b6t', 'b5t', 'bwt', 
        'isz', 'cdi', 'flp', 'uif', 'xdi', 'sdi'
    ]);

    return validExtensions.has(fileExt.toLowerCase());
}

/**
 * 增强版内容审查函数
 * 支持多个审查渠道，处理不同的响应格式
 * 返回丰富的审查结果信息
 * 
 * @param {Object} env - 环境变量
 * @param {string} url - 要审查的URL
 * @returns {Object} - 审查结果对象
 */
export async function moderateContent(env, url) {
    try {
        // 尝试从缓存获取
        const cachedResult = cacheGet('MODERATION_RESULTS', url);
        if (cachedResult) {
            return { ...cachedResult, fromCache: true };
        }

        const securityConfig = await fetchSecurityConfigWithCache(env);
        const uploadModerate = securityConfig.upload.moderate;

        // 检查是否启用审查
        if (!uploadModerate || !uploadModerate.enabled) {
            const result = {
                success: true,
                enabled: false,
                label: "None",
                message: "内容审查未启用"
            };
            cacheSet('MODERATION_RESULTS', url, result);
            return result;
        }

        // 根据不同渠道处理
        let result;
        switch (uploadModerate.channel) {
            case 'moderatecontent.com':
                result = await moderateWithModerateContent(env, url, uploadModerate);
                break;
                
            case 'nsfwjs':
                result = await moderateWithNsfwJs(env, url, uploadModerate);
                break;
                
            case 'nsfw-api':
                result = await moderateWithNsfwApi(env, url, uploadModerate);
                break;
                
            case 'custom-api':
                result = await moderateWithCustomApi(env, url, uploadModerate);
                break;
                
            default:
                result = {
                    success: false,
                    enabled: true,
                    label: "None",
                    error: `不支持的审查渠道: ${uploadModerate.channel}`
                };
        }

        // 缓存成功的结果
        if (result.success) {
            cacheSet('MODERATION_RESULTS', url, result);
        }

        return result;
    } catch (error) {
        console.error('内容审查主函数错误:', error);
        return {
            success: false,
            enabled: true,
            label: "None",
            error: `审查处理失败: ${error.message}`
        };
    }
}

/**
 * 使用缓存的安全配置获取
 */
async function fetchSecurityConfigWithCache(env) {
    // 尝试从缓存获取
    const cachedConfig = cacheGet('SECURITY_CONFIG', 'default');
    if (cachedConfig) {
        return cachedConfig;
    }

    const config = await fetchSecurityConfig(env);
    cacheSet('SECURITY_CONFIG', 'default', config);
    return config;
}

/**
 * 使用 moderatecontent.com 进行内容审查
 * @param {Object} env - 环境变量
 * @param {string} url - 要审查的URL
 * @param {Object} config - 审查配置
 * @returns {Object} - 审查结果
 */
async function moderateWithModerateContent(env, url, config) {
    try {
        const apikey = config.moderateContentApiKey;
        
        // 检查API密钥
        if (!apikey || apikey.trim() === "") {
            throw new WorkerError("moderatecontent.com API密钥未配置", "MISSING_API_KEY", 400);
        }

        // 调用API
        const apiUrl = `https://api.moderatecontent.com/moderate/?key=${apikey}&url=${encodeURIComponent(url)}`;
        const response = await fetchWithTimeout(apiUrl, { timeout: 10000 });
        
        if (!response.ok) {
            const errorText = await response.text();
            throw new WorkerError(`API请求失败: ${response.status} - ${errorText}`, "API_REQUEST_FAILED", response.status);
        }

        const moderateData = await response.json();
        
        // 验证响应格式
        if (!moderateData || typeof moderateData.rating_label === 'undefined') {
            throw new WorkerError('无效的API响应格式', "INVALID_RESPONSE_FORMAT", 400);
        }

        // 处理结果
        return {
            success: true,
            enabled: true,
            label: moderateData.rating_label || "None",
            rawData: moderateData,
            channel: 'moderatecontent.com',
            timestamp: new Date().toISOString(),
            // 提取更多有用信息
            rating: moderateData.rating || 0,
            probability: moderateData.probability || 0,
            id: moderateData.id || null
        };
    } catch (error) {
        console.error('moderatecontent.com 审查错误:', error);
        return {
            success: false,
            enabled: true,
            label: "None",
            error: `moderatecontent.com 审查失败: ${error.message}`,
            channel: 'moderatecontent.com',
            timestamp: new Date().toISOString(),
            errorCode: error.code || 'UNKNOWN_ERROR'
        };
    }
}

/**
 * 使用 nsfwjs 进行内容审查
 * @param {Object} env - 环境变量
 * @param {string} url - 要审查的URL
 * @param {Object} config - 审查配置
 * @returns {Object} - 审查结果
 */
async function moderateWithNsfwJs(env, url, config) {
    try {
        const nsfwApiPath = config.nsfwApiPath;
        
        // 检查API路径配置
        if (!nsfwApiPath || nsfwApiPath.trim() === "") {
            throw new WorkerError("nsfwjs API路径未配置", "MISSING_API_PATH", 400);
        }

        // 调用API
        const apiUrl = `${nsfwApiPath}?url=${encodeURIComponent(url)}`;
        const response = await fetchWithTimeout(apiUrl, { timeout: 10000 });
        
        if (!response.ok) {
            const errorText = await response.text();
            throw new WorkerError(`API请求失败: ${response.status} - ${errorText}`, "API_REQUEST_FAILED", response.status);
        }

        const moderateData = await response.json();
        
        // 验证响应格式
        if (!moderateData || typeof moderateData.score === 'undefined') {
            throw new WorkerError('无效的API响应格式', "INVALID_RESPONSE_FORMAT", 400);
        }

        // 处理结果
        const score = moderateData.score || 0;
        let label;
        
        if (score >= 0.9) {
            label = "adult";
        } else if (score >= 0.7) {
            label = "teen";
        } else {
            label = "everyone";
        }

        return {
            success: true,
            enabled: true,
            label: label,
            rawData: moderateData,
            channel: 'nsfwjs',
            timestamp: new Date().toISOString(),
            // 提取更多有用信息
            score: score,
            threshold: {
                adult: 0.9,
                teen: 0.7
            }
        };
    } catch (error) {
        console.error('nsfwjs 审查错误:', error);
        return {
            success: false,
            enabled: true,
            label: "None",
            error: `nsfwjs 审查失败: ${error.message}`,
            channel: 'nsfwjs',
            timestamp: new Date().toISOString(),
            errorCode: error.code || 'UNKNOWN_ERROR'
        };
    }
}

/**
 * 使用自定义NSFW API进行内容审查
 * 适配新的响应格式: { code, msg, data: { sfw, nsfw, is_nsfw } }
 * 
 * @param {Object} env - 环境变量
 * @param {string} url - 要审查的URL
 * @param {Object} config - 审查配置
 * @returns {Object} - 审查结果
 */
async function moderateWithNsfwApi(env, url, config) {
    try {
        const apiKey = config.nsfwApiKey || env.NSFW_API_KEY;
        const apiEndpoint = config.nsfwApiEndpoint || 'https://nsfw-api.example.com/check';
        
        // 检查API配置
        if (!apiKey || apiKey.trim() === "") {
            throw new WorkerError("NSFW API密钥未配置", "MISSING_API_KEY", 400);
        }

        // 调用API
        const response = await fetchWithTimeout(`${apiEndpoint}?url=${encodeURIComponent(url)}`, {
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${apiKey}`,
                'Content-Type': 'application/json'
            },
            timeout: 10000
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new WorkerError(`API请求失败: ${response.status} - ${errorText}`, "API_REQUEST_FAILED", response.status);
        }

        const result = await response.json();
        
        // 验证响应格式
        if (!result || !result.data || typeof result.data.is_nsfw === 'undefined') {
            throw new WorkerError('无效的API响应格式', "INVALID_RESPONSE_FORMAT", 400);
        }

        const { data } = result;
        let label;
        
        // 智能标签生成
        if (typeof data.is_nsfw === 'boolean') {
            label = data.is_nsfw ? 'nsfw' : 'sfw';
        } else if (typeof data.nsfw === 'number') {
            const nsfwThreshold = config.nsfwThreshold || 0.7;
            label = data.nsfw >= nsfwThreshold ? 'nsfw' : 'sfw';
        } else {
            label = 'unknown';
        }

        // 处理结果
        return {
            success: true,
            enabled: true,
            label: label,
            rawData: result,
            channel: 'nsfw-api',
            timestamp: new Date().toISOString(),
            // 提取详细信息
            nsfwScore: data.nsfw || 0,
            sfwScore: data.sfw || 0,
            isNsfw: data.is_nsfw || false,
            code: result.code || null,
            message: result.msg || null
        };
    } catch (error) {
        console.error('NSFW API 审查错误:', error);
        return {
            success: false,
            enabled: true,
            label: "None",
            error: `NSFW API 审查失败: ${error.message}`,
            channel: 'nsfw-api',
            timestamp: new Date().toISOString(),
            errorCode: error.code || 'UNKNOWN_ERROR'
        };
    }
}

/**
 * 使用自定义API进行内容审查
 * 可配置响应映射规则
 * 
 * @param {Object} env - 环境变量
 * @param {string} url - 要审查的URL
 * @param {Object} config - 审查配置
 * @returns {Object} - 审查结果
 */
async function moderateWithCustomApi(env, url, config) {
    try {
        const apiConfig = config.customApi || {};
        
        // 检查API配置
        if (!apiConfig.endpoint || !apiConfig.method) {
            throw new WorkerError("自定义API配置不完整", "INVALID_API_CONFIG", 400);
        }

        // 准备请求参数
        const requestOptions = {
            method: apiConfig.method.toUpperCase(),
            headers: apiConfig.headers || {},
            timeout: 10000
        };

        // 处理请求体
        if (apiConfig.body) {
            requestOptions.body = JSON.stringify(apiConfig.body);
        }

        // 构建URL（支持占位符替换）
        let apiUrl = apiConfig.endpoint;
        if (apiUrl.includes('{{url}}')) {
            apiUrl = apiUrl.replace('{{url}}', encodeURIComponent(url));
        }

        // 调用API
        const response = await fetchWithTimeout(apiUrl, requestOptions);

        if (!response.ok) {
            const errorText = await response.text();
            throw new WorkerError(`API请求失败: ${response.status} - ${errorText}`, "API_REQUEST_FAILED", response.status);
        }

        const result = await response.json();
        
        // 验证响应格式
        if (!result) {
            throw new WorkerError('无效的API响应', "INVALID_RESPONSE", 400);
        }

        // 应用响应映射规则
        let label = "None";
        if (apiConfig.responseMapping && typeof apiConfig.responseMapping === 'object') {
            // 简单的JSON路径映射
            label = getValueFromJsonPath(result, apiConfig.responseMapping.labelPath);
            
            // 如果有自定义映射函数
            if (apiConfig.responseMapping.mapFunction && typeof apiConfig.responseMapping.mapFunction === 'function') {
                label = apiConfig.responseMapping.mapFunction(result);
            }
        }

        // 处理结果
        return {
            success: true,
            enabled: true,
            label: label || "None",
            rawData: result,
            channel: 'custom-api',
            timestamp: new Date().toISOString(),
            // 保存映射配置
            mappingConfig: apiConfig.responseMapping || null
        };
    } catch (error) {
        console.error('自定义API 审查错误:', error);
        return {
            success: false,
            enabled: true,
            label: "None",
            error: `自定义API 审查失败: ${error.message}`,
            channel: 'custom-api',
            timestamp: new Date().toISOString(),
            errorCode: error.code || 'UNKNOWN_ERROR'
        };
    }
}

/**
 * 辅助函数：从JSON对象中按路径获取值
 * 支持简单的点表示法，如 "data.rating.label"
 * 
 * @param {Object} obj - JSON对象
 * @param {string} path - 路径字符串
 * @returns {any} - 获取到的值或undefined
 */
function getValueFromJsonPath(obj, path) {
    if (!obj || !path || typeof path !== 'string') {
        return undefined;
    }

    const pathParts = path.split('.');
    let currentValue = obj;

    for (const part of pathParts) {
        if (currentValue && typeof currentValue === 'object' && part in currentValue) {
            currentValue = currentValue[part];
        } else {
            return undefined;
        }
    }

    return currentValue;
}

/**
 * 简化版内容审查函数（兼容原有代码）
 * 只返回标签，便于现有代码集成
 * 
 * @param {Object} env - 环境变量
 * @param {string} url - 要审查的URL
 * @returns {string} - 审查标签
 */
export async function moderateContentSimple(env, url) {
    const result = await moderateContent(env, url);
    return result.label || "None";
}

/**
 * 批量内容审查函数
 * 同时审查多个URL
 * 
 * @param {Object} env - 环境变量
 * @param {string[]} urls - 要审查的URL数组
 * @returns {Object[]} - 审查结果数组
 */
export async function moderateContentBatch(env, urls) {
    if (!Array.isArray(urls) || urls.length === 0) {
        return [];
    }

    // 并行处理所有URL
    const promises = urls.map(url => moderateContent(env, url));
    return Promise.all(promises);
}

/**
 * 内容审查中间件
 * 统一处理内容审查逻辑
 * 
 * @param {Object} context - 请求上下文
 * @param {string} fileId - 文件ID
 * @param {Object} metadata - 文件元数据
 * @param {string} fileUrl - 文件URL
 * @returns {Object} - 处理结果
 */
export async function moderationMiddleware(context, fileId, metadata, fileUrl) {
    const { env, db, securityConfig } = context;
    const uploadModerate = securityConfig.upload.moderate;

    if (!uploadModerate || !uploadModerate.enabled) {
        metadata.Label = 'moderation_disabled';
        metadata.Status = 'active';
        return { success: true };
    }

    try {
        // 调用增强版审查
        const moderationResult = await moderateContent(env, fileUrl);
        
        if (moderationResult.success) {
            // 更新元数据
            metadata.Label = moderationResult.label;
            
            // 保存详细信息
            if (moderationResult.nsfwScore !== undefined) {
                metadata.NSFWScore = moderationResult.nsfwScore;
            }
            if (moderationResult.sfwScore !== undefined) {
                metadata.SFWScore = moderationResult.sfwScore;
            }
            if (moderationResult.isNsfw !== undefined) {
                metadata.IsNSFW = moderationResult.isNsfw;
            }
            if (moderationResult.score !== undefined) {
                metadata.ModerationScore = moderationResult.score;
            }
            
            metadata.ModerationChannel = moderationResult.channel;
            metadata.ModerationTimestamp = moderationResult.timestamp;
            metadata.ModerationFromCache = moderationResult.fromCache || false;
            
            // 策略驱动的处理
            if (moderationResult.label === 'nsfw' || moderationResult.label === 'adult') {
                switch (uploadModerate.policy) {
                    case 'quarantine':
                        metadata.Status = 'quarantined';
                        break;
                    case 'delete':
                        await db.delete(fileId);
                        return { 
                            success: false, 
                            status: 403, 
                            message: "Content violates policy" 
                        };
                    case 'restrict':
                        metadata.Status = 'restricted';
                        break;
                    default:
                        metadata.Status = 'active';
                }
            } else {
                metadata.Status = 'active';
            }
            
            return { success: true };
        } else {
            // 审查失败处理
            metadata.Label = 'moderation_failed';
            metadata.ModerationError = moderationResult.error;
            metadata.ModerationChannel = moderationResult.channel;
            metadata.ModerationTimestamp = moderationResult.timestamp;
            metadata.Status = 'moderation_pending';
            
            return { success: false, error: moderationResult.error };
        }
    } catch (error) {
        // 异常处理
        metadata.Label = 'moderation_error';
        metadata.ModerationError = error.message;
        metadata.Status = 'moderation_failed';
        
        return { success: false, error: error.message };
    }
}

// 清除CDN缓存
export async function purgeCDNCache(env, cdnUrl, url, normalizedFolder) {
    if (env.dev_mode === 'true') {
        return;
    }

    // 并行清除缓存
    const cachePromises = [];

    // 清除CDN缓存
    cachePromises.push(
        purgeCFCache(env, cdnUrl)
            .catch(error => console.error('Failed to clear CDN cache:', error))
    );

    // 清除api/randomFileList API缓存
    cachePromises.push(
        (async () => {
            try {
                const cache = caches.default;
                const nullResponse = new Response(null, {
                    headers: { 'Cache-Control': 'max-age=0' },
                });

                await cache.put(`${url.origin}/api/randomFileList?dir=${normalizedFolder}`, nullResponse);
            } catch (error) {
                console.error('Failed to clear cache:', error);
            }
        })()
    );

    // 等待所有缓存清除操作完成
    await Promise.all(cachePromises);
}

// 结束上传：清除缓存，维护索引
export async function endUpload(context, fileId, metadata) {
    const { env, url } = context;

    // 清除CDN缓存
    const cdnUrl = `https://${url.hostname}/file/${fileId}`;
    const normalizedFolder = (url.searchParams.get('uploadFolder') || '').replace(/^\/+/, '').replace(/\/{2,}/g, '/').replace(/\/$/, '');
    
    // 并行处理缓存清除和索引更新
    const promises = [
        purgeCDNCache(env, cdnUrl, url, normalizedFolder),
        addFileToIndex(context, fileId, metadata)
    ];

    await Promise.all(promises);
}

// 从 request 中解析 ip 地址
export function getUploadIp(request) {
    const ipHeaders = [
        "cf-connecting-ip", "x-real-ip", "x-forwarded-for", 
        "x-client-ip", "x-host", "x-originating-ip", 
        "x-cluster-client-ip", "forwarded-for", "forwarded", 
        "via", "requester", "true-client-ip", "client-ip",
        "x-remote-ip", "fastly-client-ip", "akamai-origin-hop",
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

// 检查上传IP是否被封禁
export async function isBlockedUploadIp(env, uploadIp) {
    if (!uploadIp) return false;

    try {
        const db = getDatabase(env);

        let list = await db.get("manage@blockipList");
        if (list == null) {
            return false;
        }

        const blockedIps = list.split(",").map(ip => ip.trim()).filter(ip => ip);
        return blockedIps.includes(uploadIp);
    } catch (error) {
        console.error('Failed to check blocked IP:', error);
        // 如果数据库未配置，默认不阻止任何IP
        return false;
    }
}

// 构建唯一文件ID
export async function buildUniqueFileId(context, fileName, fileType = 'application/octet-stream') {
    const { env, url } = context;
    const db = getDatabase(env);

    if (!fileName) {
        fileName = 'unknown_file';
    }

    // 提取文件扩展名
    let fileExt = getFileExtension(fileName, fileType);
    
    // 验证文件扩展名
    if (!isExtValid(fileExt)) {
        fileExt = getFileExtensionFromMimeType(fileType);
    }

    // 处理文件名
    fileName = sanitizeFileName(fileName);
    
    // 处理上传文件夹
    const uploadFolder = url.searchParams.get('uploadFolder') || '';
    const normalizedFolder = normalizeFolderPath(uploadFolder);
    
    // 获取命名方式
    const nameType = url.searchParams.get('uploadNameType') || 'default';

    // 根据命名方式生成ID
    switch (nameType) {
        case 'index':
            return buildIndexBasedId(normalizedFolder, fileExt);
            
        case 'origin':
            return buildOriginBasedId(db, normalizedFolder, fileName);
            
        case 'short':
            return buildShortId(db, normalizedFolder, fileExt);
            
        default:
            return buildDefaultId(db, normalizedFolder, fileName, fileExt);
    }
}

// 辅助函数：提取文件扩展名
function getFileExtension(fileName, fileType) {
    let ext = fileName.split('.').pop();
    if (!ext || ext === fileName) {
        ext = fileType.split('/').pop();
    }
    return ext || 'unknown';
}

// 辅助函数：从MIME类型获取扩展名
function getFileExtensionFromMimeType(fileType) {
    const ext = fileType.split('/').pop();
    return ext === fileType || !ext ? 'unknown' : ext;
}

// 辅助函数：标准化文件夹路径
function normalizeFolderPath(folder) {
    if (!folder) return '';
    
    return folder
        .replace(/^\/+/, '')      // 移除开头的斜杠
        .replace(/\/{2,}/g, '/')  // 替换多个斜杠为单个斜杠
        .replace(/\/$/, '');      // 移除结尾的斜杠
}

// 基于索引的ID生成
function buildIndexBasedId(folder, ext) {
    const uniqueIndex = Date.now() + Math.floor(Math.random() * 10000);
    return folder ? `${folder}/${uniqueIndex}.${ext}` : `${uniqueIndex}.${ext}`;
}

// 基于原始文件名的ID生成
async function buildOriginBasedId(db, folder, fileName) {
    const baseId = folder ? `${folder}/${fileName}` : fileName;
    
    if (await db.get(baseId) === null) {
        return baseId;
    }
    
    // 如果已存在，添加计数器
    return findUniqueIdWithCounter(db, baseId, fileName);
}

// 短ID生成
async function buildShortId(db, folder, ext) {
    const maxRetries = 10;
    let retries = 0;
    
    while (retries < maxRetries) {
        const shortId = generateSecureId(8);
        const testFullId = folder ? `${folder}/${shortId}.${ext}` : `${shortId}.${ext}`;
        
        if (await db.get(testFullId) === null) {
            return testFullId;
        }
        
        retries++;
    }
    
    throw new WorkerError('无法生成唯一的短文件ID', 'ID_GENERATION_FAILED', 500);
}

// 默认ID生成
async function buildDefaultId(db, folder, fileName, ext) {
    const uniqueIndex = Date.now() + Math.floor(Math.random() * 10000);
    const baseName = `${uniqueIndex}_${fileName}`;
    const baseId = folder ? `${folder}/${baseName}` : baseName;
    
    if (await db.get(baseId) === null) {
        return baseId;
    }
    
    // 如果已存在，添加计数器
    return findUniqueIdWithCounter(db, baseId, baseName);
}

// 查找带计数器的唯一ID
async function findUniqueIdWithCounter(db, baseId, baseName) {
    const maxRetries = 1000;
    let counter = 1;
    
    while (counter <= maxRetries) {
        const nameWithoutExt = baseName.substring(0, baseName.lastIndexOf('.')) || baseName;
        const ext = baseName.substring(baseName.lastIndexOf('.')) || '';
        
        const duplicateId = baseId.replace(baseName, `${nameWithoutExt}(${counter})${ext}`);
        
        if (await db.get(duplicateId) === null) {
            return duplicateId;
        }
        
        counter++;
    }
    
    throw new WorkerError('无法生成唯一的文件ID', 'ID_GENERATION_FAILED', 500);
}

// 基于uploadId的一致性渠道选择
export function selectConsistentChannel(channels, uploadId, loadBalanceEnabled) {
    if (!loadBalanceEnabled || !channels || !Array.isArray(channels) || channels.length === 0) {
        return channels[0];
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

// 带超时的fetch
async function fetchWithTimeout(url, options = {}) {
    const { timeout = 10000, ...fetchOptions } = options;
    
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeout);
    
    try {
        const response = await fetch(url, {
            ...fetchOptions,
            signal: controller.signal
        });
        return response;
    } catch (error) {
        if (error.name === 'AbortError') {
            throw new WorkerError(`请求超时（${timeout}ms）`, 'REQUEST_TIMEOUT', 408);
        }
        throw error;
    } finally {
        clearTimeout(id);
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
export async function logError(env, error, context = {}) {
    try {
        if (!env.KV) return;
        
        const errorId = generateSecureId(16);
        const errorLog = {
            errorId,
            message: error.message,
            code: error.code || 'UNKNOWN_ERROR',
            status: error.status || 500,
            stack: error.stack,
            timestamp: error.timestamp || Date.now(),
            context: {
                requestId: context.requestId,
                ip: context.ip,
                url: context.url,
                userAgent: context.userAgent,
                ...context
            }
        };
        
        await env.KV.put(`error_${errorId}`, JSON.stringify(errorLog), {
            expirationTtl: 7 * 24 * 60 * 60 // 保存7天
        });
        
        return errorId;
    } catch (logError) {
        console.error('Failed to log error:', logError);
        return null;
    }
}

// 模块导出
export const ContentModeration = {
    moderate: moderateContent,
    moderateSimple: moderateContentSimple,
    moderateBatch: moderateContentBatch,
    middleware: moderationMiddleware
};

export const FileManagement = {
    buildUniqueId: buildUniqueFileId,
    endUpload: endUpload,
    purgeCache: purgeCDNCache
};

export const Security = {
    getIP: getUploadIp,
    isBlockedIP: isBlockedUploadIp,
    selectChannel: selectConsistentChannel
};

export const Utils = {
    createResponse,
    generateShortId,
    generateSecureId,
    getIPAddress,
    sanitizeFileName,
    isExtValid,
    logError
};
