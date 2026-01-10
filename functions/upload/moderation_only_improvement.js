// 仅审查功能改进 - 可以直接集成到现有代码中

import { fetchSecurityConfig } from "../utils/sysConfig";

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
