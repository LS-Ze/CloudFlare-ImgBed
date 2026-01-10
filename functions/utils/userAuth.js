// 在Cloudflare Worker中使用
import { userAuthCheck, UnauthorizedResponse, isPreflightRequest, createPreflightResponse, shouldSkipAuth } from './auth.js';

export default {
    async fetch(request, env) {
        try {
            // 处理预飞行请求
            if (isPreflightRequest(request)) {
                return createPreflightResponse();
            }

            const url = new URL(request.url);
            
            // 检查是否应该跳过认证
            const skipPaths = ['/public', '/health', '/api/v1/status*'];
            if (shouldSkipAuth(request, skipPaths)) {
                return handlePublicRequest(request);
            }

            // 进行用户认证
            const authResult = await userAuthCheck(
                env,
                url,
                request,
                'upload:files', // 需要的权限
                {
                    requireToken: env.NODE_ENV === 'production',
                    maxAttempts: 5,
                    lockoutDuration: 300000, // 5分钟
                    allowedIps: env.ALLOWED_IPS ? env.ALLOWED_IPS.split(',') : null
                }
            );

            // 认证失败
            if (!authResult.valid) {
                console.warn(`Authentication failed for ${authResult.ipAddress}: ${authResult.reason}`);
                return UnauthorizedResponse(authResult.reason);
            }

            // 认证成功，处理请求
            console.log(`Authentication successful for ${authResult.ipAddress} using ${authResult.authMethod}`);
            return handleAuthenticatedRequest(request, authResult);
            
        } catch (error) {
            console.error('Authentication error:', error);
            return new Response(JSON.stringify({
                error: 'Server error',
                message: 'An unexpected error occurred'
            }), {
                status: 500,
                headers: {
                    'Content-Type': 'application/json'
                }
            });
        }
    }
};

async function handlePublicRequest(request) {
    // 处理公开请求
    return new Response('Public content', {
        headers: {
            'Content-Type': 'text/plain'
        }
    });
}

async function handleAuthenticatedRequest(request, authResult) {
    // 处理认证后的请求
    return new Response(JSON.stringify({
        message: 'Authenticated',
        user: authResult.userId,
        ip: authResult.ipAddress,
        method: authResult.authMethod
    }), {
        headers: {
            'Content-Type': 'application/json'
        }
    });
}
