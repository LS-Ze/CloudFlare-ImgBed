// 审查功能改进 - 增强兼容性版本

import { fetchSecurityConfig } from "../utils/sysConfig";

/**
 * 增强版内容审查函数 - 完全兼容旧配置
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
        const uploadModerate = securityConfig.upload?.moderate || {};

        // 检查是否启用审查（兼容旧配置）
        const enableModerate = uploadModerate.enabled !== false;
        
        // 如果未启用审查，直接返回label
        if (!enableModerate) {
            return {
                success: true,
                enabled: false,
                label: "None",
                message: "内容审查未启用"
            };
        }

        // 获取渠道配置（兼容旧配置）
        const channel = uploadModerate.channel || 'default';
        
        // 根据不同渠道处理
        switch (channel.toLowerCase()) {
            case 'moderatecontent.com':
                return await moderateWithModerateContent(env, url, uploadModerate);
                
            case 'nsfwjs':
                return await moderateWithNsfwJs(env, url, uploadModerate);
                
            case 'nsfw-api':
                return await moderateWithNsfwApi(env, url, uploadModerate);
                
            case 'custom-api':
                return await moderateWithCustomApi(env, url, uploadModerate);
                
            // 兼容旧的"default"渠道
            case 'default':
                // 尝试使用moderatecontent.com作为默认（如果配置了API密钥）
                if (uploadModerate.moderateContentApiKey) {
                    return await moderateWithModerateContent(env, url, uploadModerate);
                }
                // 否则尝试使用nsfwjs（如果配置了API路径）
                else if (uploadModerate.nsfwApiPath) {
                    return await moderateWithNsfwJs(env, url, uploadModerate);
                }
                // 没有配置任何渠道，返回默认
                else {
                    return {
                        success: true,
                        enabled: true,
                        label: "None",
                        message: "默认渠道未配置API密钥或路径"
                    };
                }
                
            default:
                return {
                    success: false,
                    enabled: true,
                    label: "None",
                    error: `不支持的审查渠道: ${channel}`
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
        
        // 检查API密钥（兼容旧配置，允许空字符串）
        if (apikey == undefined || apikey == null || apikey == "") {
            console.warn('moderatecontent.com API密钥未配置或为空');
            return {
                success: true,
                enabled: true,
                label: "None",
                message: "API密钥未配置"
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
        
        // 验证响应格式（兼容不同版本的API响应）
        let label = "None";
        if (moderateData.rating_label) {
            label = moderateData.rating_label;
        } else if (moderateData.label) {
            label = moderateData.label;
        } else if (moderateData.classification) {
            label = moderateData.classification;
        }

        // 处理结果
        return {
            success: true,
            enabled: true,
            label: label,
            rawData: moderateData,
            channel: 'moderatecontent.com',
            timestamp: new Date().toISOString(),
            // 提取更多有用信息（兼容不同字段名）
            rating: moderateData.rating || moderateData.score || 0,
            probability: moderateData.probability || moderateData.confidence || 0,
            id: moderateData.id || moderateData.request_id || null
        };
    } catch (error) {
        console.error('moderatecontent.com 审查错误:', error);
        // 兼容旧行为：即使失败也返回"None"标签
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
        
        // 检查API路径配置（兼容旧配置）
        if (!nsfwApiPath || nsfwApiPath.trim() === "") {
            console.warn('nsfwjs API路径未配置或为空');
            return {
                success: true,
                enabled: true,
                label: "None",
                message: "API路径未配置"
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
        
        // 验证响应格式（兼容不同版本的API响应）
        let score = 0;
        if (typeof moderateData.score === 'number') {
            score = moderateData.score;
        } else if (typeof moderateData.nsfw_score === 'number') {
            score = moderateData.nsfw_score;
        } else if (typeof moderateData.probability === 'number') {
            score = moderateData.probability;
        }

        // 处理结果（兼容旧的阈值设置）
        let label = "everyone";
        const thresholds = {
            adult: config.adultThreshold || 0.9,
            teen: config.teenThreshold || 0.7
        };
        
        if (score >= thresholds.adult) {
            label = "adult";
        } else if (score >= thresholds.teen) {
            label = "teen";
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
            threshold: thresholds
        };
    } catch (error) {
        console.error('nsfwjs 审查错误:', error);
        // 兼容旧行为：即使失败也返回"None"标签
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
 * 同时兼容旧格式
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
            console.warn('NSFW API密钥未配置或为空');
            return {
                success: true,
                enabled: true,
                label: "None",
                message: "API密钥未配置"
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
        
        // 验证响应格式（兼容新旧格式）
        let data = result;
        let isNsfw = false;
        let nsfwScore = 0;
        let sfwScore = 0;
        
        // 新格式: { code, msg, data: { sfw, nsfw, is_nsfw } }
        if (result.data && typeof result.data === 'object') {
            data = result.data;
        }
        
        // 提取数据（兼容不同字段名）
        if (typeof data.is_nsfw === 'boolean') {
            isNsfw = data.is_nsfw;
        } else if (typeof data.nsfw === 'boolean') {
            isNsfw = data.nsfw;
        }
        
        if (typeof data.nsfw === 'number') {
            nsfwScore = data.nsfw;
        } else if (typeof data.nsfw_score === 'number') {
            nsfwScore = data.nsfw_score;
        }
        
        if (typeof data.sfw === 'number') {
            sfwScore = data.sfw;
        } else if (typeof data.sfw_score === 'number') {
            sfwScore = data.sfw_score;
        }

        // 智能标签生成
        let label;
        const nsfwThreshold = config.nsfwThreshold || 0.7;
        
        if (typeof isNsfw === 'boolean') {
            label = isNsfw ? 'nsfw' : 'sfw';
        } else if (typeof nsfwScore === 'number') {
            label = nsfwScore >= nsfwThreshold ? 'nsfw' : 'sfw';
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
            nsfwScore: nsfwScore,
            sfwScore: sfwScore,
            isNsfw: isNsfw,
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
            console.warn('自定义API配置不完整');
            return {
                success: true,
                enabled: true,
                label: "None",
                message: "API配置不完整"
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
 * 简化版内容审查函数（完全兼容原有代码）
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
 * 内容审查中间件 - 增强兼容性
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
    const uploadModerate = securityConfig.upload?.moderate || {};

    // 检查是否启用审查（兼容旧配置）
    const enableModerate = uploadModerate.enabled !== false;
    
    if (!enableModerate) {
        metadata.Label = 'moderation_disabled';
        metadata.Status = metadata.Status || 'active';
        return { success: true };
    }

    try {
        // 调用增强版审查
        const moderationResult = await moderateContent(env, fileUrl);
        
        if (moderationResult.success) {
            // 更新元数据（兼容旧的元数据结构）
            metadata.Label = moderationResult.label;
            
            // 保存详细信息（只在有值时保存，避免覆盖旧数据）
            if (moderationResult.nsfwScore !== undefined && moderationResult.nsfwScore !== 0) {
                metadata.NSFWScore = moderationResult.nsfwScore;
            }
            if (moderationResult.sfwScore !== undefined && moderationResult.sfwScore !== 0) {
                metadata.SFWScore = moderationResult.sfwScore;
            }
            if (moderationResult.isNsfw !== undefined) {
                metadata.IsNSFW = moderationResult.isNsfw;
            }
            if (moderationResult.score !== undefined && moderationResult.score !== 0) {
                metadata.ModerationScore = moderationResult.score;
            }
            
            // 新增元数据字段（不影响旧字段）
            if (moderationResult.channel) {
                metadata.ModerationChannel = moderationResult.channel;
            }
            if (moderationResult.timestamp) {
                metadata.ModerationTimestamp = moderationResult.timestamp;
            }
            
            // 策略驱动的处理（兼容旧的处理方式）
            const policy = uploadModerate.policy || 'default';
            const isNsfw = moderationResult.label === 'nsfw' || moderationResult.label === 'adult';
            
            if (isNsfw) {
                switch (policy) {
                    case 'quarantine':
                        metadata.Status = 'quarantined';
                        break;
                    case 'delete':
                        if (db && db.delete) {
                            await db.delete(fileId);
                            return { 
                                success: false, 
                                status: 403, 
                                message: "Content violates policy" 
                            };
                        }
                        metadata.Status = 'deleted';
                        break;
                    case 'restrict':
                        metadata.Status = 'restricted';
                        break;
                    default:
                        // 兼容旧行为：只设置标签，不改变状态
                        metadata.Status = metadata.Status || 'active';
                }
            } else {
                metadata.Status = metadata.Status || 'active';
            }
            
            return { success: true };
        } else {
            // 审查失败处理（兼容旧行为）
            metadata.Label = 'moderation_failed';
            if (moderationResult.error) {
                metadata.ModerationError = moderationResult.error;
            }
            if (moderationResult.channel) {
                metadata.ModerationChannel = moderationResult.channel;
            }
            if (moderationResult.timestamp) {
                metadata.ModerationTimestamp = moderationResult.timestamp;
            }
            metadata.Status = metadata.Status || 'moderation_pending';
            
            return { success: false, error: moderationResult.error };
        }
    } catch (error) {
        // 异常处理（兼容旧行为）
        metadata.Label = 'moderation_error';
        metadata.ModerationError = error.message;
        metadata.Status = metadata.Status || 'moderation_failed';
        
        return { success: false, error: error.message };
    }
}

/**
 * 配置迁移辅助函数
 * 帮助将旧配置迁移到新格式
 * 
 * @param {Object} oldConfig - 旧配置
 * @returns {Object} - 迁移后的新配置
 */
export function migrateModerationConfig(oldConfig) {
    if (!oldConfig) return {};
    
    const newConfig = { ...oldConfig };
    
    // 迁移默认渠道配置
    if (oldConfig.channel === 'default') {
        // 如果有moderateContentApiKey，迁移到moderatecontent.com渠道
        if (oldConfig.moderateContentApiKey) {
            newConfig.channel = 'moderatecontent.com';
        }
        // 如果有nsfwApiPath，迁移到nsfwjs渠道
        else if (oldConfig.nsfwApiPath) {
            newConfig.channel = 'nsfwjs';
        }
    }
    
    // 为旧配置添加默认策略
    if (!newConfig.policy) {
        newConfig.policy = 'default';
    }
    
    // 为nsfwjs添加默认阈值
    if (newConfig.channel === 'nsfwjs' && !newConfig.nsfwThreshold) {
        newConfig.nsfwThreshold = 0.7;
    }
    
    return newConfig;
}
