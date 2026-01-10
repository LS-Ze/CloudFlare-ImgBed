import { fetchSecurityConfig } from "../utils/sysConfig";
import { purgeCFCache } from "../utils/purgeCache";
import { addFileToIndex } from "../utils/indexManager.js";
import { getDatabase } from '../utils/databaseAdapter.js';

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

// 获取IP地址
export async function getIPAddress(ip) {
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
            }
        }
    } catch (error) {
        console.error('Error fetching IP address:', error);
    }
    return address;
}

// 处理文件名中的特殊字符
export function sanitizeFileName(fileName) {
    fileName = decodeURIComponent(fileName);
    fileName = fileName.split('/').pop();

    const unsafeCharsRe = /[\\\/:\*\?"'<>\| \(\)\[\]\{\}#%\^`~;@&=\+\$,]/g;
    return fileName.replace(unsafeCharsRe, '_');
}

// 检查文件扩展名是否有效
export function isExtValid(fileExt) {
    return ['jpeg', 'jpg', 'png', 'gif', 'webp',
        'mp4', 'mp3', 'ogg',
        'mp3', 'wav', 'flac', 'aac', 'opus',
        'doc', 'docx', 'ppt', 'pptx', 'xls', 'xlsx', 'pdf',
        'txt', 'md', 'json', 'xml', 'html', 'css', 'js', 'ts', 'go', 'java', 'php', 'py', 'rb', 'sh', 'bat', 'cmd', 'ps1', 'psm1', 'psd', 'ai', 'sketch', 'fig', 'svg', 'eps', 'zip', 'rar', '7z', 'tar', 'gz', 'bz2', 'xz', 'apk', 'exe', 'msi', 'dmg', 'iso', 'torrent', 'webp', 'ico', 'svg', 'ttf', 'otf', 'woff', 'woff2', 'eot', 'apk', 'crx', 'xpi', 'deb', 'rpm', 'jar', 'war', 'ear', 'img', 'iso', 'vdi', 'ova', 'ovf', 'qcow2', 'vmdk', 'vhd', 'vhdx', 'pvm', 'dsk', 'hdd', 'bin', 'cue', 'mds', 'mdf', 'nrg', 'ccd', 'cif', 'c2d', 'daa', 'b6t', 'b5t', 'bwt', 'isz', 'isz', 'cdi', 'flp', 'uif', 'xdi', 'sdi'
    ].includes(fileExt);
}

// 图像审查
export async function moderateContent(env, url) {
    try {
        const securityConfig = await fetchSecurityConfig(env);
        const uploadModerate = securityConfig.upload.moderate;

        // 检查是否启用审查
        if (!uploadModerate || !uploadModerate.enabled) {
            return {
                success: true,
                enabled: false,
                label: "None",
                message: "内容审查未启用"
            };
        }

        // 根据不同渠道处理
        switch (uploadModerate.channel) {
            case 'moderatecontent.com':
                return await moderateWithModerateContent(env, url, uploadModerate);
                
            case 'nsfwjs':
                return await moderateWithNsfwJs(env, url, uploadModerate);
                
            case 'nsfw-api':
                return await moderateWithNsfwApi(env, url, uploadModerate);
                
            case 'custom-api':
                return await moderateWithCustomApi(env, url, uploadModerate);
                
            default:
                return {
                    success: false,
                    enabled: true,
                    label: "None",
                    error: `不支持的审查渠道: ${uploadModerate.channel}`
                };
        }
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
            return {
                success: false,
                enabled: true,
                label: "None",
                error: "moderatecontent.com API密钥未配置"
            };
        }

        // 调用API
        const apiUrl = `https://api.moderatecontent.com/moderate/?key=${apikey}&url=${encodeURIComponent(url)}`;
        const response = await fetch(apiUrl);
        
        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`API请求失败: ${response.status} - ${errorText}`);
        }

        const moderateData = await response.json();
        
        // 验证响应格式
        if (!moderateData || typeof moderateData.rating_label === 'undefined') {
            throw new Error('无效的API响应格式');
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
            timestamp: new Date().toISOString()
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
            return {
                success: false,
                enabled: true,
                label: "None",
                error: "nsfwjs API路径未配置"
            };
        }

        // 调用API
        const apiUrl = `${nsfwApiPath}?url=${encodeURIComponent(url)}`;
        const response = await fetch(apiUrl);
        
        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`API请求失败: ${response.status} - ${errorText}`);
        }

        const moderateData = await response.json();
        
        // 验证响应格式
        if (!moderateData || typeof moderateData.score === 'undefined') {
            throw new Error('无效的API响应格式');
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
            timestamp: new Date().toISOString()
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
            return {
                success: false,
                enabled: true,
                label: "None",
                error: "NSFW API密钥未配置"
            };
        }

        // 调用API
        const response = await fetch(`${apiEndpoint}?url=${encodeURIComponent(url)}`, {
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${apiKey}`,
                'Content-Type': 'application/json'
            }
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`API请求失败: ${response.status} - ${errorText}`);
        }

        const result = await response.json();
        
        // 验证响应格式
        if (!result || !result.data || typeof result.data.is_nsfw === 'undefined') {
            throw new Error('无效的API响应格式');
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
            timestamp: new Date().toISOString()
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
            return {
                success: false,
                enabled: true,
                label: "None",
                error: "自定义API配置不完整"
            };
        }

        // 准备请求参数
        const requestOptions = {
            method: apiConfig.method.toUpperCase(),
            headers: apiConfig.headers || {}
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
        const response = await fetch(apiUrl, requestOptions);

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`API请求失败: ${response.status} - ${errorText}`);
        }

        const result = await response.json();
        
        // 验证响应格式
        if (!result) {
            throw new Error('无效的API响应');
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
            timestamp: new Date().toISOString()
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

    // nsfw 渠道 和 默认渠道
    if (uploadModerate.channel === 'nsfwjs') {
        const nsfwApiPath = securityConfig.upload.moderate.nsfwApiPath;

        try {
            const fetchResponse = await fetch(`${nsfwApiPath}?url=${encodeURIComponent(url)}`);
            if (!fetchResponse.ok) {
                throw new Error(`HTTP error! status: ${fetchResponse.status}`);
            }
            const moderate_data = await fetchResponse.json();

            const score = moderate_data.score || 0;
            if (score >= 0.9) {
                label = "adult";
            } else if (score >= 0.7) {
                label = "teen";
            } else {
                label = "everyone";
            }
        } catch (error) {
            console.error('Moderate Error:', error);
            // 将不带审查的图片写入数据库
            label = "None";
        }

        return label;
    }

    return label;
}

// 清除CDN缓存
export async function purgeCDNCache(env, cdnUrl, url, normalizedFolder) {
    if (env.dev_mode === 'true') {
        return;
    }

    // 清除CDN缓存
    try {
        await purgeCFCache(env, cdnUrl);
    } catch (error) {
        console.error('Failed to clear CDN cache:', error);
    }

    // 清除api/randomFileList API缓存
    try {
        const cache = caches.default;
        // await cache.delete(`${url.origin}/api/randomFileList`); delete有bug，通过写入一个max-age=0的response来清除缓存
        const nullResponse = new Response(null, {
            headers: { 'Cache-Control': 'max-age=0' },
        });

        await cache.put(`${url.origin}/api/randomFileList?dir=${normalizedFolder}`, nullResponse);
    } catch (error) {
        console.error('Failed to clear cache:', error);
    }
}

// 结束上传：清除缓存，维护索引
export async function endUpload(context, fileId, metadata) {
    const { env, url } = context;

    // 清除CDN缓存
    const cdnUrl = `https://${url.hostname}/file/${fileId}`;
    const normalizedFolder = (url.searchParams.get('uploadFolder') || '').replace(/^\/+/, '').replace(/\/{2,}/g, '/').replace(/\/$/, '');
    await purgeCDNCache(env, cdnUrl, url, normalizedFolder);

    // 更新文件索引（索引更新时会自动计算容量统计）
    await addFileToIndex(context, fileId, metadata);
}

// 从 request 中解析 ip 地址
export function getUploadIp(request) {
    const ip = request.headers.get("cf-connecting-ip") || request.headers.get("x-real-ip") || request.headers.get("x-forwarded-for") || request.headers.get("x-client-ip") || request.headers.get("x-host") || request.headers.get("x-originating-ip") || request.headers.get("x-cluster-client-ip") || request.headers.get("forwarded-for") || request.headers.get("forwarded") || request.headers.get("via") || request.headers.get("requester") || request.headers.get("true-client-ip") || request.headers.get("client-ip") || request.headers.get("x-remote-ip") || request.headers.get("x-originating-ip") || request.headers.get("fastly-client-ip") || request.headers.get("akamai-origin-hop") || request.headers.get("x-remote-addr") || request.headers.get("x-remote-host") || request.headers.get("x-client-ips")

    if (!ip) {
        return null;
    }

    // 处理多个IP地址的情况
    const ips = ip.split(',').map(i => i.trim());

    return ips[0]; // 返回第一个IP地址
}

// 检查上传IP是否被封禁
export async function isBlockedUploadIp(env, uploadIp) {
    try {
        const db = getDatabase(env);

        let list = await db.get("manage@blockipList");
        if (list == null) {
            list = [];
        } else {
            list = list.split(",");
        }

        return list.includes(uploadIp);
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

    let fileExt = fileName.split('.').pop();
    if (!fileExt || fileExt === fileName) {
        fileExt = fileType.split('/').pop();
        if (fileExt === fileType || fileExt === '' || fileExt === null || fileExt === undefined) {
            fileExt = 'unknown';
        }
    }

    const nameType = url.searchParams.get('uploadNameType') || 'default';
    const uploadFolder = url.searchParams.get('uploadFolder') || '';
    const normalizedFolder = uploadFolder
        ? uploadFolder.replace(/^\/+/, '').replace(/\/{2,}/g, '/').replace(/\/$/, '')
        : '';

    if (!isExtValid(fileExt)) {
        fileExt = fileType.split('/').pop();
        if (fileExt === fileType || fileExt === '' || fileExt === null || fileExt === undefined) {
            fileExt = 'unknown';
        }
    }

    // 处理文件名，移除特殊字符
    fileName = sanitizeFileName(fileName);

    const unique_index = Date.now() + Math.floor(Math.random() * 10000);
    let baseId = '';

    // 根据命名方式构建基础ID
    if (nameType === 'index') {
        baseId = normalizedFolder ? `${normalizedFolder}/${unique_index}.${fileExt}` : `${unique_index}.${fileExt}`;
    } else if (nameType === 'origin') {
        baseId = normalizedFolder ? `${normalizedFolder}/${fileName}` : fileName;
    } else if (nameType === 'short') {
        // 对于短链接，直接在循环中生成不重复的ID
        while (true) {
            const shortId = generateShortId(8);
            const testFullId = normalizedFolder ? `${normalizedFolder}/${shortId}.${fileExt}` : `${shortId}.${fileExt}`;
            if (await db.get(testFullId) === null) {
                return testFullId;
            }
        }
    } else {
        baseId = normalizedFolder ? `${normalizedFolder}/${unique_index}_${fileName}` : `${unique_index}_${fileName}`;
    }

    // 检查基础ID是否已存在
    if (await db.get(baseId) === null) {
        return baseId;
    }

    // 如果已存在，在文件名后面加上递增编号
    let counter = 1;
    while (true) {
        let duplicateId;

        if (nameType === 'index') {
            const baseName = unique_index;
            duplicateId = normalizedFolder ?
                `${normalizedFolder}/${baseName}(${counter}).${fileExt}` :
                `${baseName}(${counter}).${fileExt}`;
        } else if (nameType === 'origin') {
            const nameWithoutExt = fileName.substring(0, fileName.lastIndexOf('.'));
            const ext = fileName.substring(fileName.lastIndexOf('.'));
            duplicateId = normalizedFolder ?
                `${normalizedFolder}/${nameWithoutExt}(${counter})${ext}` :
                `${nameWithoutExt}(${counter})${ext}`;
        } else {
            const baseName = `${unique_index}_${fileName}`;
            const nameWithoutExt = baseName.substring(0, baseName.lastIndexOf('.'));
            const ext = baseName.substring(baseName.lastIndexOf('.'));
            duplicateId = normalizedFolder ?
                `${normalizedFolder}/${nameWithoutExt}(${counter})${ext}` :
                `${nameWithoutExt}(${counter})${ext}`;
        }

        // 检查新ID是否已存在
        if (await db.get(duplicateId) === null) {
            return duplicateId;
        }

        counter++;

        // 防止无限循环，最多尝试1000次
        if (counter > 1000) {
            throw new Error('无法生成唯一的文件ID');
        }
    }
}

// 基于uploadId的一致性渠道选择
export function selectConsistentChannel(channels, uploadId, loadBalanceEnabled) {
    if (!loadBalanceEnabled || !channels || channels.length === 0) {
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
