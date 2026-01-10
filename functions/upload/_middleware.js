import { errorHandling, telemetryData, checkDatabaseConfig } from '../utils/middleware';

// 环境配置
const ENV = {
    isProduction: process.env.NODE_ENV === 'production',
    allowedOrigins: process.env.ALLOWED_ORIGINS ? process.env.ALLOWED_ORIGINS.split(',') : ['*']
};

// 增强的CORS配置
const corsHeaders = {
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With, Accept, Origin, X-CSRF-Token',
    'Access-Control-Expose-Headers': 'Content-Length, X-Custom-Header, X-Request-ID',
    'Access-Control-Max-Age': '86400',
    'Vary': 'Origin'
};

// 安全响应头
const securityHeaders = {
    'X-Frame-Options': 'DENY',
    'X-XSS-Protection': '1; mode=block',
    'X-Content-Type-Options': 'nosniff',
    'Referrer-Policy': 'strict-origin-when-cross-origin',
    'X-DNS-Prefetch-Control': 'on',
    'Strict-Transport-Security': 'max-age=63072000; includeSubDomains; preload'
};

// 生产环境额外的安全头
if (ENV.isProduction) {
    securityHeaders['Content-Security-Policy'] = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self'; frame-src 'none'; object-src 'none';";
}

/**
 * 处理CORS和预检请求
 */
async function handleCORS(context) {
    const request = context.request;
    const response = await context.next();
    
    // 复制响应头
    const newHeaders = new Headers(response.headers);
    
    // 处理预检请求
    if (request.method === 'OPTIONS') {
        // 处理允许的来源
        const origin = request.headers.get('Origin');
        if (origin && (ENV.allowedOrigins.includes('*') || ENV.allowedOrigins.includes(origin))) {
            newHeaders.set('Access-Control-Allow-Origin', origin);
        } else {
            newHeaders.set('Access-Control-Allow-Origin', ENV.allowedOrigins[0] || '*');
        }
        
        // 添加CORS头
        Object.entries(corsHeaders).forEach(([key, value]) => {
            newHeaders.set(key, value);
        });
        
        return new Response(null, {
            status: 204,
            headers: newHeaders
        });
    }
    
    // 处理普通请求的CORS
    const origin = request.headers.get('Origin');
    if (origin) {
        if (ENV.allowedOrigins.includes('*') || ENV.allowedOrigins.includes(origin)) {
            newHeaders.set('Access-Control-Allow-Origin', origin);
        }
    }
    
    // 添加安全头
    Object.entries(securityHeaders).forEach(([key, value]) => {
        newHeaders.set(key, value);
    });
    
    // 添加请求ID
    newHeaders.set('X-Request-ID', crypto.randomUUID());
    
    return new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers: newHeaders
    });
}

/**
 * 请求日志中间件
 */
async function requestLogging(context) {
    const request = context.request;
    const start = Date.now();
    
    // 记录请求信息
    const requestInfo = {
        method: request.method,
        url: new URL(request.url).pathname,
        ip: request.headers.get('CF-Connecting-IP') || 'unknown',
        userAgent: request.headers.get('User-Agent') || 'unknown',
        referrer: request.headers.get('Referer') || 'unknown',
        requestId: crypto.randomUUID(),
        timestamp: new Date().toISOString()
    };
    
    // 将请求ID添加到上下文
    context.requestId = requestInfo.requestId;
    
    try {
        const response = await context.next();
        
        // 记录响应信息
        const duration = Date.now() - start;
        const responseInfo = {
            ...requestInfo,
            status: response.status,
            duration,
            responseSize: response.headers.get('Content-Length') || 'unknown'
        };
        
        // 记录日志（可以发送到日志服务）
        console.log(JSON.stringify(responseInfo));
        
        return response;
    } catch (error) {
        // 记录错误日志
        const duration = Date.now() - start;
        const errorInfo = {
            ...requestInfo,
            status: 500,
            duration,
            error: error.message,
            stack: error.stack
        };
        
        console.error(JSON.stringify(errorInfo));
        throw error;
    }
}

/**
 * 速率限制中间件
 */
async function rateLimiting(context) {
    const { env, request } = context;
    
    // 开发环境跳过速率限制
    if (!ENV.isProduction) {
        return context.next();
    }
    
    try {
        const clientIp = request.headers.get('CF-Connecting-IP');
        if (!clientIp) {
            return context.next();
        }
        
        // 速率限制配置
        const RATE_LIMIT = 60; // 每分钟请求数
        const RATE_LIMIT_WINDOW = 60; // 时间窗口（秒）
        
        // 使用KV存储速率限制信息
        if (env.KV) {
            const rateLimitKey = `rate_limit_${clientIp}`;
            const currentCount = await env.KV.get(rateLimitKey);
            const count = currentCount ? parseInt(currentCount, 10) : 0;
            
            if (count >= RATE_LIMIT) {
                return new Response(JSON.stringify({
                    error: 'Too Many Requests',
                    message: 'Rate limit exceeded',
                    retryAfter: RATE_LIMIT_WINDOW
                }), {
                    status: 429,
                    headers: {
                        'Content-Type': 'application/json',
                        'Retry-After': RATE_LIMIT_WINDOW.toString()
                    }
                });
            }
            
            // 更新计数
            await env.KV.put(rateLimitKey, (count + 1).toString(), {
                expirationTtl: RATE_LIMIT_WINDOW
            });
        }
        
        return context.next();
    } catch (error) {
        console.error('Rate limiting error:', error);
        // 速率限制失败时继续处理请求
        return context.next();
    }
}

/**
 * API密钥认证中间件
 */
async function apiKeyAuth(context) {
    const { env, request } = context;
    
    // 不需要认证的路径
    const publicPaths = ['/api/public', '/health', '/'];
    const url = new URL(request.url);
    
    if (publicPaths.includes(url.pathname)) {
        return context.next();
    }
    
    // 检查API密钥
    if (env.API_KEY) {
        const authHeader = request.headers.get('Authorization');
        if (!authHeader || !authHeader.startsWith('Bearer ')) {
            return new Response(JSON.stringify({
                error: 'Unauthorized',
                message: 'API key is required'
            }), {
                status: 401,
                headers: { 'Content-Type': 'application/json' }
            });
        }
        
        const apiKey = authHeader.split(' ')[1];
        if (apiKey !== env.API_KEY) {
            return new Response(JSON.stringify({
                error: 'Unauthorized',
                message: 'Invalid API key'
            }), {
                status: 401,
                headers: { 'Content-Type': 'application/json' }
            });
        }
    }
    
    return context.next();
}

/**
 * 缓存控制中间件
 */
async function cacheControl(context) {
    const response = await context.next();
    const request = context.request;
    
    // 只缓存GET请求
    if (request.method !== 'GET') {
        return response;
    }
    
    const newHeaders = new Headers(response.headers);
    
    // 静态资源缓存
    const url = new URL(request.url);
    const path = url.pathname;
    
    if (path.match(/\.(js|css|png|jpg|jpeg|gif|ico|svg|woff|woff2|ttf|eot)$/)) {
        newHeaders.set('Cache-Control', 'public, max-age=31536000, immutable');
    } else if (path.startsWith('/api/')) {
        // API响应缓存
        newHeaders.set('Cache-Control', 'public, max-age=60');
    } else {
        // 动态内容不缓存
        newHeaders.set('Cache-Control', 'no-cache, no-store, must-revalidate');
        newHeaders.set('Pragma', 'no-cache');
        newHeaders.set('Expires', '0');
    }
    
    return new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers: newHeaders
    });
}

// 优化后的中间件链
export const onRequest = [
    handleCORS,           // CORS处理应该在最前面
    requestLogging,       // 请求日志
    rateLimiting,         // 速率限制
    apiKeyAuth,           // API密钥认证
    checkDatabaseConfig,  // 数据库配置检查
    cacheControl,         // 缓存控制
    errorHandling,        // 错误处理
    telemetryData         // 遥测数据
];
