/**
 * Batch Tag Management API
 *
 * POST /api/manage/tags/batch - Update tags for multiple files
 *
 * Request body format:
 * {
 *   fileIds: ["file1", "file2", ...],
 *   action: "set" | "add" | "remove" | "replace" | "toggle",
 *   tags: ["tag1", "tag2", ...],
 *   options: {
 *     caseSensitive: false,
 *     unique: true,
 *     normalize: true
 *   }
 * }
 */

import { purgeCFCache } from "../../../utils/purgeCache.js";
import { batchAddFilesToIndex } from "../../../utils/indexManager.js";
import { getDatabase } from "../../../utils/databaseAdapter.js";
import { mergeTags, validateTag, normalizeTag } from "../../../utils/tagHelpers.js";

/**
 * @typedef {Object} BatchTagRequest
 * @property {Array<string>} fileIds - Array of file IDs
 * @property {string} action - Tag operation action
 * @property {Array<string>} tags - Array of tags to apply
 * @property {BatchTagOptions} [options] - Operation options
 */

/**
 * @typedef {Object} BatchTagOptions
 * @property {boolean} [caseSensitive=false] - Case sensitive tag matching
 * @property {boolean} [unique=true] - Ensure tags are unique
 * @property {boolean} [normalize=true] - Normalize tag names
 * @property {boolean} [validate=true] - Validate tag format
 * @property {boolean} [dryRun=false] - Preview changes without applying
 * @property {number} [concurrency=5] - Maximum concurrent operations
 */

/**
 * @typedef {Object} BatchTagResult
 * @property {boolean} success - Overall success status
 * @property {number} total - Total files processed
 * @property {number} updated - Number of files successfully updated
 * @property {number} skipped - Number of files skipped
 * @property {Array<Object>} errors - Array of errors
 * @property {Array<Object>} details - Detailed results for each file
 * @property {boolean} dryRun - Whether this was a dry run
 */

/**
 * 配置常量
 */
const CONFIG = {
    MAX_FILES_PER_REQUEST: 100,
    DEFAULT_CONCURRENCY: 5,
    SUPPORTED_ACTIONS: ['set', 'add', 'remove', 'replace', 'toggle'],
    MAX_TAG_LENGTH: 100,
    MIN_TAG_LENGTH: 1
};

/**
 * 验证请求参数
 * @param {BatchTagRequest} requestBody - Request body
 * @returns {Object} Validation result
 */
function validateRequest(requestBody) {
    const { fileIds = [], action = 'set', tags = [], options = {} } = requestBody;
    
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
    
    // Validate options
    const validOptions = {
        caseSensitive: typeof options.caseSensitive === 'boolean' ? options.caseSensitive : false,
        unique: typeof options.unique === 'boolean' ? options.unique : true,
        normalize: typeof options.normalize === 'boolean' ? options.normalize : true,
        validate: typeof options.validate === 'boolean' ? options.validate : true,
        dryRun: typeof options.dryRun === 'boolean' ? options.dryRun : false,
        concurrency: Math.min(
            Math.max(parseInt(options.concurrency) || CONFIG.DEFAULT_CONCURRENCY, 1),
            20
        )
    };
    
    // Validate tags if needed
    if (validOptions.validate && tags.length > 0) {
        const validationResults = validateTags(tags, validOptions);
        if (!validationResults.valid) {
            return validationResults;
        }
    }
    
    return {
        valid: true,
        params: {
            fileIds: [...new Set(fileIds)], // Remove duplicates
            action,
            tags: processTags(tags, validOptions),
            options: validOptions
        }
    };
}

/**
 * 验证标签数组
 * @param {Array<string>} tags - Tags to validate
 * @param {BatchTagOptions} options - Validation options
 * @returns {Object} Validation result
 */
function validateTags(tags, options) {
    const invalidTags = [];
    const duplicateTags = [];
    const seenTags = new Set();
    
    for (const tag of tags) {
        // Check tag format
        if (!validateTag(tag)) {
            invalidTags.push(tag);
            continue;
        }
        
        // Check tag length
        if (tag.length < CONFIG.MIN_TAG_LENGTH || tag.length > CONFIG.MAX_TAG_LENGTH) {
            invalidTags.push(tag);
            continue;
        }
        
        // Check for duplicates
        const tagKey = options.caseSensitive ? tag : tag.toLowerCase();
        if (seenTags.has(tagKey)) {
            duplicateTags.push(tag);
        }
        seenTags.add(tagKey);
    }
    
    if (invalidTags.length > 0) {
        return {
            valid: false,
            error: 'Invalid tag format',
            message: `Tags must be ${CONFIG.MIN_TAG_LENGTH}-${CONFIG.MAX_TAG_LENGTH} characters and contain only valid characters`,
            invalidTags
        };
    }
    
    if (duplicateTags.length > 0 && options.unique) {
        return {
            valid: false,
            error: 'Duplicate tags',
            message: 'Tags array contains duplicate entries',
            duplicateTags
        };
    }
    
    return { valid: true };
}

/**
 * 处理标签（规范化、去重等）
 * @param {Array<string>} tags - Tags to process
 * @param {BatchTagOptions} options - Processing options
 * @returns {Array<string>} Processed tags
 */
function processTags(tags, options) {
    if (!tags || tags.length === 0) return [];
    
    let processed = [...tags];
    
    // Normalize tags
    if (options.normalize) {
        processed = processed.map(tag => normalizeTag(tag));
    }
    
    // Remove duplicates
    if (options.unique) {
        const seen = new Set();
        processed = processed.filter(tag => {
            const tagKey = options.caseSensitive ? tag : tag.toLowerCase();
            if (seen.has(tagKey)) return false;
            seen.add(tagKey);
            return true;
        });
    }
    
    return processed;
}

/**
 * 并发处理函数
 * @param {Array<any>} items - Items to process
 * @param {Function} processor - Processing function
 * @param {number} concurrency - Maximum concurrency
 * @returns {Promise<Array<any>>} Processing results
 */
async function processConcurrent(items, processor, concurrency) {
    const results = [];
    const executing = [];
    
    for (const item of items) {
        const p = Promise.resolve()
            .then(() => processor(item))
            .then(result => {
                results.push(result);
                executing.splice(executing.indexOf(p), 1);
            })
            .catch(error => {
                results.push({ error: error.message });
                executing.splice(executing.indexOf(p), 1);
            });
        
        executing.push(p);
        
        if (executing.length >= concurrency) {
            await Promise.race(executing);
        }
    }
    
    await Promise.all(executing);
    return results;
}

/**
 * 处理单个文件的标签更新
 * @param {Object} params - Processing parameters
 * @returns {Promise<Object>} Processing result
 */
async function processFileTags({ fileId, action, tags, options, db, url, dryRun }) {
    try {
        // Get file metadata
        const fileData = await db.getWithMetadata(fileId);
        
        if (!fileData || !fileData.metadata) {
            return {
                fileId,
                status: 'error',
                error: 'File not found'
            };
        }
        
        // Get existing tags
        const existingTags = fileData.metadata.Tags || [];
        
        // Merge tags based on action
        let updatedTags = [...existingTags];
        
        switch (action) {
            case 'set':
                updatedTags = [...tags];
                break;
                
            case 'add':
                updatedTags = mergeTags(existingTags, tags, 'add', options);
                break;
                
            case 'remove':
                updatedTags = mergeTags(existingTags, tags, 'remove', options);
                break;
                
            case 'replace':
                // Replace existing tags that match any in the tags array
                updatedTags = existingTags.filter(tag => 
                    !tags.some(t => 
                        options.caseSensitive ? t === tag : t.toLowerCase() === tag.toLowerCase()
                    )
                ).concat(tags);
                break;
                
            case 'toggle':
                // Toggle each tag in the tags array
                updatedTags = existingTags.filter(tag => 
                    !tags.some(t => 
                        options.caseSensitive ? t === tag : t.toLowerCase() === tag.toLowerCase()
                    )
                );
                
                // Add tags that were not present
                const tagsToAdd = tags.filter(tag => 
                    !existingTags.some(t => 
                        options.caseSensitive ? t === tag : t.toLowerCase() === tag.toLowerCase()
                    )
                );
                
                updatedTags = [...updatedTags, ...tagsToAdd];
                break;
        }
        
        // Remove duplicates and empty tags
        updatedTags = [...new Set(updatedTags.filter(tag => tag && tag.trim() !== ''))];
        
        // Check if tags actually changed
        const tagsChanged = JSON.stringify(existingTags.sort()) !== JSON.stringify(updatedTags.sort());
        
        if (!tagsChanged) {
            return {
                fileId,
                status: 'skipped',
                reason: 'No tag changes'
            };
        }
        
        // If dry run, don't make actual changes
        if (dryRun) {
            return {
                fileId,
                status: 'dry-run',
                existingTags,
                updatedTags
            };
        }
        
        // Update metadata
        fileData.metadata.Tags = updatedTags;
        fileData.metadata.updatedAt = new Date().toISOString();
        
        // Save to database with retry
        await db.put(fileId, fileData.value, {
            metadata: fileData.metadata
        });
        
        return {
            fileId,
            status: 'updated',
            existingTags,
            updatedTags,
            metadata: {
                ...fileData.metadata,
                value: undefined // Don't include file content in response
            }
        };
        
    } catch (error) {
        console.error(`Error processing file ${fileId}:`, error);
        return {
            fileId,
            status: 'error',
            error: error.message,
            stack: error.stack
        };
    }
}

/**
 * 批量清除CDN缓存
 * @param {Object} env - Environment variables
 * @param {Array<string>} fileIds - File IDs to purge
 * @param {URL} url - Request URL
 * @param {Function} waitUntil - Cloudflare waitUntil function
 */
function batchPurgeCache(env, fileIds, url, waitUntil) {
    if (!fileIds || fileIds.length === 0) return;
    
    const urlsToPurge = fileIds.map(fileId => 
        `https://${url.hostname}/file/${fileId}`
    );
    
    // Split into chunks to avoid hitting API limits
    const chunks = [];
    for (let i = 0; i < urlsToPurge.length; i += 30) {
        chunks.push(urlsToPurge.slice(i, i + 30));
    }
    
    chunks.forEach(chunk => {
        waitUntil(purgeCFCache(env, chunk));
    });
}

/**
 * 主API处理函数
 */
export async function onRequest(context) {
    const startTime = performance.now();
    const { request, env, waitUntil } = context;
    const url = new URL(request.url);
    
    try {
        // Only allow POST requests
        if (request.method !== 'POST') {
            return new Response(JSON.stringify({
                error: 'Method not allowed',
                allowedMethods: ['POST'],
                status: 405
            }), {
                status: 405,
                headers: { 
                    'Content-Type': 'application/json',
                    'Allow': 'GET, POST'
                }
            });
        }
        
        // Check content type
        const contentType = request.headers.get('Content-Type');
        if (!contentType || !contentType.includes('application/json')) {
            return new Response(JSON.stringify({
                error: 'Unsupported media type',
                message: 'Request must be JSON',
                status: 415
            }), {
                status: 415,
                headers: { 'Content-Type': 'application/json' }
            });
        }
        
        // Parse request body with size limit
        const body = await new Promise((resolve, reject) => {
            const chunks = [];
            let size = 0;
            const MAX_SIZE = 1024 * 1024; // 1MB
            
            request.body.on('data', (chunk) => {
                size += chunk.length;
                if (size > MAX_SIZE) {
                    reject(new Error('Request body too large'));
                }
                chunks.push(chunk);
            });
            
            request.body.on('end', () => {
                try {
                    resolve(JSON.parse(Buffer.concat(chunks).toString()));
                } catch (error) {
                    reject(new Error('Invalid JSON'));
                }
            });
            
            request.body.on('error', reject);
        });
        
        // Validate request parameters
        const validation = validateRequest(body);
        if (!validation.valid) {
            return new Response(JSON.stringify({
                error: validation.error,
                message: validation.message,
                ...(validation.invalidTags && { invalidTags: validation.invalidTags }),
                ...(validation.duplicateTags && { duplicateTags: validation.duplicateTags }),
                status: 400
            }), {
                status: 400,
                headers: { 'Content-Type': 'application/json' }
            });
        }
        
        const { fileIds, action, tags, options } = validation.params;
        const { dryRun, concurrency } = options;
        
        console.log(`[Batch Tag API] Processing ${fileIds.length} files with action: ${action}, tags: ${tags.length}, dryRun: ${dryRun}, concurrency: ${concurrency}`);
        
        const db = getDatabase(env);
        
        // Process files concurrently
        const processingResults = await processConcurrent(
            fileIds.map(fileId => ({
                fileId,
                action,
                tags,
                options,
                db,
                url,
                dryRun
            })),
            processFileTags,
            concurrency
        );
        
        // Aggregate results
        const result = {
            success: true,
            total: fileIds.length,
            updated: 0,
            skipped: 0,
            errors: [],
            details: [],
            dryRun
        };
        
        const updatedFiles = [];
        const filesToPurge = [];
        
        for (const res of processingResults) {
            result.details.push(res);
            
            switch (res.status) {
                case 'updated':
                    result.updated++;
                    updatedFiles.push({
                        fileId: res.fileId,
                        metadata: res.metadata
                    });
                    filesToPurge.push(res.fileId);
                    break;
                    
                case 'skipped':
                    result.skipped++;
                    break;
                    
                case 'error':
                    result.success = false;
                    result.errors.push({
                        fileId: res.fileId,
                        error: res.error
                    });
                    break;
            }
        }
        
        // If not dry run, update index and purge cache
        if (!dryRun) {
            // Batch update file index
            if (updatedFiles.length > 0) {
                waitUntil(batchAddFilesToIndex(context, updatedFiles, { skipExisting: false }));
            }
            
            // Batch purge CDN cache
            if (filesToPurge.length > 0) {
                batchPurgeCache(env, filesToPurge, url, waitUntil);
            }
        }
        
        const duration = performance.now() - startTime;
        console.log(`[Batch Tag API] Completed in ${duration.toFixed(2)}ms - ${result.updated} updated, ${result.skipped} skipped, ${result.errors.length} errors`);
        
        return new Response(JSON.stringify({
            ...result,
            processingTime: `${duration.toFixed(2)}ms`
        }), {
            status: result.success ? 200 : 207, // 207 = Multi-Status
            headers: { 
                'Content-Type': 'application/json',
                'X-Processing-Time': `${duration.toFixed(2)}ms`
            }
        });
        
    } catch (error) {
        const duration = performance.now() - startTime;
        console.error(`[Batch Tag API] Error (${duration.toFixed(2)}ms):`, error);
        
        return new Response(JSON.stringify({
            error: 'Internal server error',
            message: env.DEBUG_MODE === 'true' ? error.message : 'An unexpected error occurred',
            status: 500,
            processingTime: `${duration.toFixed(2)}ms`
        }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
        });
    }
}

/**
 * 辅助函数：获取批量标签操作状态
 * @param {string} batchId - Batch operation ID
 * @returns {Promise<Object>} Batch status
 */
export async function getBatchStatus(batchId, env) {
    const db = getDatabase(env);
    const status = await db.get(`batch:tag:${batchId}`);
    return status ? JSON.parse(status) : null;
}
