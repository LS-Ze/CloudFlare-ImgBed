/**
 * Cloudflare Worker 重定向处理函数
 * 增强版重定向功能，支持配置、日志、错误处理和安全措施
 */

/**
 * @typedef {Object} RedirectConfig
 * @property {string} targetPath - 目标路径，默认为 "/dashboard"
 * @property {number} statusCode - 重定向状态码，默认为 302
 * @property {boolean} preserveQuery - 是否保留查询参数，默认为 true
 * @property {boolean} logRequests - 是否记录请求日志，默认为 true
 * @property {Array<string>} skipPaths - 跳过重定向的路径列表
 * @property {boolean} enableSecurityChecks - 是否启用安全检查，默认为 true
 * @property {boolean} allowSameOrigin - 是否允许同源重定向，默认为 false
 */

/**
 * 默认配置
 */
const DEFAULT_CONFIG = {
    targetPath: "/dashboard",
    statusCode: 302,
    preserveQuery: true,
    logRequests: true,
    skipPaths: [],
    enableSecurityChecks: true,
    allowSameOrigin: false
};

/**
 * 环境变量配置键
 */
const CONFIG_ENV_KEYS = {
    TARGET_PATH: "REDIRECT_TARGET_PATH",
    STATUS_CODE: "REDIRECT_STATUS_CODE",
    PRESERVE_QUERY: "REDIRECT_PRESERVE_QUERY",
    LOG_REQUESTS: "REDIRECT_LOG_REQUESTS",
    SKIP_PATHS: "REDIRECT_SKIP_PATHS",
    ENABLE_SECURITY_CHECKS: "REDIRECT_ENABLE_SECURITY_CHECKS",
    ALLOW_SAME_ORIGIN: "REDIRECT_ALLOW_SAME_ORIGIN"
};

/**
 * 从环境变量加载配置
 * @param {Object} env - 环境变量
 * @returns {RedirectConfig} 配置对象
 */
function loadConfigFromEnv(env) {
    const config = { ...DEFAULT_CONFIG };
    
    if (env[CONFIG_ENV_KEYS.TARGET_PATH]) {
        config.targetPath = env[CONFIG_ENV_KEYS.TARGET_PATH];
    }
    
    if (env[CONFIG_ENV_KEYS.STATUS_CODE]) {
        const statusCode = parseInt(env[CONFIG_ENV_KEYS.STATUS_CODE]);
        if (!isNaN(statusCode) && statusCode >= 300 && statusCode < 400) {
            config.statusCode = statusCode;
        }
    }
    
    if (env[CONFIG_ENV_KEYS.PRESERVE_QUERY] !== undefined) {
        config.preserveQuery = env[CONFIG_ENV_KEYS.PRESERVE_QUERY] === "true";
    }
    
    if (env[CONFIG_ENV_KEYS.LOG_REQUESTS] !== undefined) {
        config.logRequests = env[CONFIG_ENV_KEYS.LOG_REQUESTS] === "true";
    }
    
    if (env[CONFIG_ENV_KEYS.SKIP_PATHS]) {
        config.skipPaths = env[CONFIG_ENV_KEYS.SKIP_PATHS].split(",").map(path => path.trim());
    }
    
    if (env[CONFIG_ENV_KEYS.ENABLE_SECURITY_CHECKS] !== undefined) {
        config.enableSecurityChecks = env[CONFIG_ENV_KEYS.ENABLE_SECURITY_CHECKS] === "true";
    }
    
    if (env[CONFIG_ENV_KEYS.ALLOW_SAME_ORIGIN] !== undefined) {
        config.allowSameOrigin = env[CONFIG_ENV_KEYS.ALLOW_SAME_ORIGIN] === "true";
    }
    
    return config;
}

/**
 * 记录请求信息
 * @param {Request} request - 请求对象
 * @param {URL} url - URL对象
 * @param {string} targetUrl - 目标URL
 * @param {RedirectConfig} config - 配置对象
 */
function logRequest(request, url, targetUrl, config) {
    if (!config.logRequests) return;
    
    const logData = {
        timestamp: new Date().toISOString(),
        method: request.method,
        originalUrl: url.href,
        targetUrl,
        statusCode: config.statusCode,
        userAgent: request.headers.get("User-Agent") || "unknown",
        referrer: request.headers.get("Referer") || "unknown",
        ip: request.headers.get("CF-Connecting-IP") || "unknown"
    };
    
    console.log("[Redirect] " + JSON.stringify(logData));
}

/**
 * 检查是否应该跳过重定向
 * @param {URL} url - URL对象
 * @param {RedirectConfig} config - 配置对象
 * @returns {boolean} 是否跳过
 */
function shouldSkipRedirect(url, config) {
    if (!config.skipPaths || config.skipPaths.length === 0) {
        return false;
    }
    
    const path = url.pathname;
    
    return config.skipPaths.some(skipPath => {
        if (skipPath.endsWith("*")) {
            const prefix = skipPath.slice(0, -1);
            return path.startsWith(prefix);
        }
        return path === skipPath;
    });
}

/**
 * 检查是否存在重定向循环
 * @param {URL} url - URL对象
 * @param {string} targetPath - 目标路径
 * @returns {boolean} 是否存在循环
 */
function hasRedirectLoop(url, targetPath) {
    return url.pathname === targetPath;
}

/**
 * 验证重定向目标
 * @param {string} targetPath - 目标路径
 * @returns {boolean} 是否有效
 */
function isValidTarget(targetPath) {
    return targetPath && targetPath.startsWith("/");
}

/**
 * 处理预飞行请求
 * @param {Request} request - 请求对象
 * @returns {Response|null} 预飞行响应或null
 */
function handlePreflightRequest(request) {
    if (request.method === "OPTIONS" && request.headers.has("Access-Control-Request-Method")) {
        return new Response(null, {
            status: 204,
            headers: {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
                "Access-Control-Max-Age": "86400",
                "Cache-Control": "no-store"
            }
        });
    }
    return null;
}

/**
 * 获取客户端IP地址
 * @param {Request} request - 请求对象
 * @returns {string} IP地址
 */
function getClientIp(request) {
    return request.headers.get("CF-Connecting-IP") || 
           request.headers.get("X-Forwarded-For")?.split(",")[0].trim() || 
           request.headers.get("Remote-Addr") || 
           "unknown";
}

/**
 * 主处理函数
 * @param {Object} context - 上下文对象
 * @returns {Promise<Response>} 响应对象
 */
export async function onRequest(context) {
    const startTime = performance.now();
    
    try {
        // 解构上下文对象
        const {
            request,
            env,
            params,
            waitUntil,
            next,
            data
        } = context;
        
        // 处理预飞行请求
        const preflightResponse = handlePreflightRequest(request);
        if (preflightResponse) {
            return preflightResponse;
        }
        
        // 解析URL
        const url = new URL(request.url);
        
        // 加载配置
        const config = loadConfigFromEnv(env);
        
        // 安全检查
        if (config.enableSecurityChecks) {
            // 检查重定向循环
            if (hasRedirectLoop(url, config.targetPath)) {
                console.warn(`[Redirect] Loop detected for ${url.href} -> ${config.targetPath}`);
                return new Response("Redirect loop detected", {
                    status: 400,
                    headers: {
                        "Content-Type": "text/plain",
                        "Cache-Control": "no-store"
                    }
                });
            }
            
            // 验证目标路径
            if (!isValidTarget(config.targetPath)) {
                console.error(`[Redirect] Invalid target path: ${config.targetPath}`);
                return new Response("Invalid redirect configuration", {
                    status: 500,
                    headers: {
                        "Content-Type": "text/plain",
                        "Cache-Control": "no-store"
                    }
                });
            }
        }
        
        // 检查是否应该跳过重定向
        if (shouldSkipRedirect(url, config)) {
            if (config.logRequests) {
                console.log(`[Redirect] Skipping redirect for ${url.href}`);
            }
            return next();
        }
        
        // 构建目标URL
        let targetUrl = new URL(config.targetPath, url.origin);
        
        // 保留查询参数
        if (config.preserveQuery && url.search) {
            targetUrl.search = url.search;
        }
        
        // 记录请求
        logRequest(request, url, targetUrl.href, config);
        
        // 记录性能指标
        const duration = performance.now() - startTime;
        if (config.logRequests) {
            console.log(`[Redirect] Processed in ${duration.toFixed(2)}ms`);
        }
        
        // 创建重定向响应
        return Response.redirect(targetUrl.href, config.statusCode);
        
    } catch (error) {
        console.error(`[Redirect] Error: ${error.message}`, error.stack);
        
        // 错误响应
        return new Response("Redirect service error", {
            status: 500,
            headers: {
                "Content-Type": "text/plain",
                "Cache-Control": "no-store"
            }
        });
    }
}

/**
 * 辅助函数：获取配置
 * @param {Object} env - 环境变量
 * @returns {RedirectConfig} 配置对象
 */
export function getRedirectConfig(env) {
    return loadConfigFromEnv(env);
}

/**
 * 辅助函数：测试重定向逻辑
 * @param {string} url - 测试URL
 * @param {RedirectConfig} config - 配置对象
 * @returns {string|null} 目标URL或null（如果跳过）
 */
export function testRedirect(url, config) {
    try {
        const parsedUrl = new URL(url);
        
        if (shouldSkipRedirect(parsedUrl, config)) {
            return null;
        }
        
        if (config.enableSecurityChecks && hasRedirectLoop(parsedUrl, config.targetPath)) {
            return null;
        }
        
        let targetUrl = new URL(config.targetPath, parsedUrl.origin);
        
        if (config.preserveQuery && parsedUrl.search) {
            targetUrl.search = parsedUrl.search;
        }
        
        return targetUrl.href;
    } catch (error) {
        console.error(`[Redirect] Test error: ${error.message}`);
        return null;
    }
}
