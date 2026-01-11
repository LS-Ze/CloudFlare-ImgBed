/**
 * Batch Tag Management API
 *
 * POST /api/manage/tags/batch - Update tags for multiple files
 *
 * Request body format:
 * {
 *   fileIds: ["file1", "file2", ...],
 *   action: "set" | "add" | "remove",
 *   tags: ["tag1", "tag2", ...]
 * }
 */
import { purgeCFCache } from "../../../utils/purgeCache.js";
import { batchAddFilesToIndex } from "../../../utils/indexManager.js";
import { getDatabase } from "../../../utils/databaseAdapter.js";
import { mergeTags, validateTag, normalizeTags } from "../../../utils/tagHelpers.js";

/**
 * 配置常量
 */
const CONFIG = {
    MAX_FILES_PER_REQUEST: 100,
    DEFAULT_CONCURRENCY: 5,
    SUPPORTED_ACTIONS: ['set', 'add', 'remove']
};

/**
 * 验证请求参数
 * @param {Object} requestBody - 请求体
 * @returns {Object} 验证结果
 */
function validateRequest(requestBody) {
    const { fileIds = [], action = 'set', tags = [] } = requestBody;

    // Validate fileIds
    if (!Array.isArray(fileIds) || fileIds.length === 0) {
        return {
            valid: false,
            error: 'Invalid fileIds',
            message: 'fileIds must be a non-empty array of file identifiers'
        };
    }

    if (fileIds.length > CONFIG.MAX_FILES_PER_REQUEST) {
        return {
            valid: false,
            error: 'Too many files',
            message: `Maximum ${CONFIG.MAX_FILES_PER_REQUEST} files per request`
        };
    }

    // Validate action
    if (!CONFIG.SUPPORTED_ACTIONS.includes(action)) {
        return {
            valid: false,
            error: 'Invalid action',
            message: `Action must be one of: ${CONFIG.SUPPORTED_ACTIONS.join(', ')}`
        };
    }

    // Validate tags array
    if (!Array.isArray(tags)) {
        return {
            valid: false,
            error: 'Invalid tags format',
            message: 'Tags must be an array of strings'
        };
    }

    // Validate each tag
    const invalidTags = tags.filter(tag => !validateTag(tag));
    if (invalidTags.length > 0) {
        return {
            valid: false,
            error: 'Invalid tag format',
            message: 'Tags must contain only alphanumeric characters, underscores, hyphens, and CJK characters',
            invalidTags: invalidTags
        };
    }

    return {
        valid: true,
        params: {
            fileIds: [...new Set(fileIds)], // Remove duplicates
            action,
            tags: normalizeTags(tags)
        }
    };
}

/**
 * 主API处理函数
 */
export async function onRequest(context) {
    const { request, env, waitUntil } = context;

    try {
        // Get URL safely
        let url;
        try {
            url = new URL(request.url);
        } catch (error) {
            // Fallback to default URL if request.url is not available
            url = new URL('https://example.com');
        }

        // Only allow POST requests
        if (request.method !== 'POST') {
            return new Response(JSON.stringify({
                error: 'Method not allowed',
                allowedMethods: ['POST']
            }), {
                status: 405,
                headers: {
                    'Content-Type': 'application/json',
                    'Allow': 'POST'
                }
            });
        }

        // Parse request body
        const body = await request.json();

        // Validate request parameters
        const validation = validateRequest(body);
        if (!validation.valid) {
            return new Response(JSON.stringify({
                error: validation.error,
                message: validation.message,
                ...(validation.invalidTags && { invalidTags: validation.invalidTags }),
                status: 400
            }), {
                status: 400,
                headers: { 'Content-Type': 'application/json' }
            });
        }

        const { fileIds, action, tags } = validation.params;
        const db = getDatabase(env);

        // Process files in batch
        const results = {
            success: true,
            total: fileIds.length,
            updated: 0,
            errors: []
        };

        const updatedFiles = [];

        for (const fileId of fileIds) {
            try {
                // Get file metadata
                const fileData = await db.getWithMetadata(fileId);

                if (!fileData || !fileData.metadata) {
                    results.errors.push({
                        fileId: fileId,
                        error: 'File not found'
                    });
                    continue;
                }

                // Get existing tags
                const existingTags = fileData.metadata.Tags || [];

                // Merge tags based on action
                const updatedTags = mergeTags(existingTags, tags, action);

                // Update metadata
                fileData.metadata.Tags = updatedTags;
                fileData.metadata.updatedAt = new Date().toISOString();

                // Save to database
                await db.put(fileId, fileData.value, {
                    metadata: fileData.metadata
                });

                // Clear CDN cache (async)
                const cdnUrl = `https://${url.hostname}/file/${fileId}`;
                waitUntil(purgeCFCache(env, cdnUrl));

                // Track updated file for batch index update
                updatedFiles.push({
                    fileId: fileId,
                    metadata: fileData.metadata
                });

                results.updated++;

            } catch (error) {
                results.errors.push({
                    fileId: fileId,
                    error: error.message
                });
            }
        }

        // Batch update file index asynchronously
        if (updatedFiles.length > 0) {
            waitUntil(batchAddFilesToIndex(context, updatedFiles, { skipExisting: false }));
        }

        // Set success to false if there were any errors
        if (results.errors.length > 0) {
            results.success = false;
        }

        return new Response(JSON.stringify(results), {
            status: results.success ? 200 : 207, // 207 = Multi-Status (partial success)
            headers: { 'Content-Type': 'application/json' }
        });

    } catch (error) {
        console.error('Error in batch tag update:', error);
        return new Response(JSON.stringify({
            error: 'Internal server error',
            message: env.DEBUG_MODE === 'true' ? error.message : 'An unexpected error occurred',
            status: 500
        }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
        });
    }
}
