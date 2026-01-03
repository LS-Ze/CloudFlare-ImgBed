import { createResponse, moderateContent, purgeCDNCache } from "./uploadTools";
import { getDatabase } from '../utils/databaseAdapter.js';
import qiniu from 'qiniu';

/**
 * 上传文件到七牛云存储
 * @param {Object} context - 请求上下文
 * @param {string} fullId - 文件ID
 * @param {Object} metadata - 文件元数据
 * @param {string} returnLink - 返回链接
 * @returns {Response} - HTTP响应
 */
export async function uploadFileToQiniu(context, fullId, metadata, returnLink) {
    const { env, waitUntil, uploadConfig, url, formdata } = context;
    const db = getDatabase(env);

    console.log('=== Qiniu Upload Start ===');

    // 获取七牛云渠道配置
    const qiniuSettings = uploadConfig.qiniu;
    console.log('Qiniu settings:', qiniuSettings ? 'found' : 'not found');

    if (!qiniuSettings || !qiniuSettings.channels || qiniuSettings.channels.length === 0) {
        console.log('Error: No Qiniu channel configured');
        return createResponse('Error: No Qiniu channel configured', { status: 400 });
    }

    // 选择渠道（支持负载均衡）
    const qiniuChannels = qiniuSettings.channels;
    console.log('Qiniu channels count:', qiniuChannels.length);

    const qiniuChannel = qiniuSettings.loadBalance?.enabled
        ? qiniuChannels[Math.floor(Math.random() * qiniuChannels.length)]
        : qiniuChannels[0];

    console.log('Selected channel:', qiniuChannel?.name, 'bucket:', qiniuChannel?.bucket);

    if (!qiniuChannel || !qiniuChannel.accessKey || !qiniuChannel.secretKey || !qiniuChannel.bucket) {
        console.log('Error: Qiniu channel not properly configured', {
            hasChannel: !!qiniuChannel,
            hasAccessKey: !!qiniuChannel?.accessKey,
            hasSecretKey: !!qiniuChannel?.secretKey,
            hasBucket: !!qiniuChannel?.bucket
        });
        return createResponse('Error: Qiniu channel not properly configured', { status: 400 });
    }

    const file = formdata.get('file');
    const fileName = metadata.FileName;
    const fileSize = file.size;

    console.log('File to upload:', fileName, 'size:', fileSize);

    // 七牛云存储区域映射
    const QINIU_REGIONS = {
        'z0': qiniu.zone.Zone_z0, // 华东
        'z1': qiniu.zone.Zone_z1, // 华北
        'z2': qiniu.zone.Zone_z2, // 华南
        'na0': qiniu.zone.Zone_na0, // 北美
        'as0': qiniu.zone.Zone_as0 // 东南亚
    };

    // 获取存储区域
    const region = qiniuChannel.region || 'z0';
    const zone = QINIU_REGIONS[region] || qiniu.zone.Zone_z0;

    console.log('Qiniu region:', region, 'zone:', zone);

    try {
        // 创建七牛云认证对象
        const mac = new qiniu.auth.digest.Mac(qiniuChannel.accessKey, qiniuChannel.secretKey);
        
        // 配置上传选项
        const options = {
            scope: `${qiniuChannel.bucket}:${fullId}`,
            expires: 3600, // 1小时有效
            returnBody: JSON.stringify({
                key: '$(key)',
                hash: '$(etag)',
                fsize: '$(fsize)',
                bucket: '$(bucket)'
            })
        };

        const putPolicy = new qiniu.rs.PutPolicy(options);
        const uploadToken = putPolicy.uploadToken(mac);

        console.log('Generated upload token successfully');

        // 配置上传客户端
        const config = new qiniu.conf.Config();
        config.zone = zone;

        const formUploader = new qiniu.form_up.FormUploader(config);
        const putExtra = new qiniu.form_up.PutExtra();

        // 转换文件为Buffer
        const arrayBuffer = await file.arrayBuffer();
        const buffer = Buffer.from(arrayBuffer);

        console.log('Starting Qiniu upload...');

        // 执行上传
        return new Promise((resolve, reject) => {
            formUploader.put(
                uploadToken,
                fullId,
                buffer,
                putExtra,
                async (err, respBody, respInfo) => {
                    if (err) {
                        console.error('Qiniu upload error:', err);
                        return resolve(createResponse(`Error: Qiniu upload failed - ${err.message}`, { status: 500 }));
                    }

                    if (respInfo.statusCode === 200) {
                        console.log('Qiniu upload successful:', respBody);

                        // 更新metadata
                        metadata.Channel = "Qiniu";
                        metadata.ChannelName = qiniuChannel.name || "Qiniu_env";
                        metadata.FileSize = (respBody.fsize / 1024 / 1024).toFixed(2);
                        metadata.QiniuBucket = qiniuChannel.bucket;
                        metadata.QiniuRegion = region;
                        metadata.QiniuDomain = qiniuChannel.domain;
                        metadata.QiniuFileKey = respBody.key;
                        metadata.QiniuFileHash = respBody.hash;
                        
                        // 构建文件访问URL
                        const domain = qiniuChannel.domain || `${qiniuChannel.bucket}.qiniudn.com`;
                        metadata.QiniuFileUrl = `https://${domain}/${respBody.key}`;

                        // 图像审查
                        let moderateUrl = metadata.QiniuFileUrl;
                        metadata.Label = await moderateContent(env, moderateUrl);

                        // 写入KV数据库
                        try {
                            await db.put(fullId, "", { metadata });
                        } catch (error) {
                            console.error('Error writing to KV database:', error);
                            return resolve(createResponse('Error: Failed to write to KV database', { status: 500 }));
                        }

                        // 结束上传
                        waitUntil(endUpload(context, fullId, metadata));

                        // 返回成功响应
                        return resolve(createResponse(
                            JSON.stringify([{ 'src': returnLink }]),
                            {
                                status: 200,
                                headers: { 'Content-Type': 'application/json' }
                            }
                        ));
                    } else {
                        console.error('Qiniu upload failed:', respInfo.statusCode, respBody);
                        return resolve(createResponse(`Error: Qiniu upload failed - ${respInfo.statusCode} ${JSON.stringify(respBody)}`, { status: respInfo.statusCode }));
                    }
                }
            );
        });

    } catch (error) {
        console.error('Qiniu upload error:', error.message);
        return createResponse(`Error: Qiniu upload failed - ${error.message}`, { status: 500 });
    }
}

/**
 * 上传大文件到七牛云存储（分片上传）
 * @param {Object} context - 请求上下文
 * @param {File} file - 文件对象
 * @param {string} fullId - 文件ID
 * @param {Object} metadata - 文件元数据
 * @param {string} fileName - 文件名
 * @param {string} fileType - 文件类型
 * @param {string} returnLink - 返回链接
 * @param {Object} qiniuChannel - 七牛云渠道配置
 * @returns {Response} - HTTP响应
 */
export async function uploadLargeFileToQiniu(context, file, fullId, metadata, fileName, fileType, returnLink, qiniuChannel) {
    const { env, waitUntil, url } = context;
    const db = getDatabase(env);

    console.log('=== Qiniu Large File Upload Start ===');
    console.log('File size:', file.size, '超过4MB，启用分片上传');

    try {
        // 创建七牛云认证对象
        const mac = new qiniu.auth.digest.Mac(qiniuChannel.accessKey, qiniuChannel.secretKey);
        
        // 配置上传选项
        const options = {
            scope: `${qiniuChannel.bucket}:${fullId}`,
            expires: 3600, // 1小时有效
            returnBody: JSON.stringify({
                key: '$(key)',
                hash: '$(etag)',
                fsize: '$(fsize)',
                bucket: '$(bucket)'
            })
        };

        const putPolicy = new qiniu.rs.PutPolicy(options);
        const uploadToken = putPolicy.uploadToken(mac);

        // 配置上传客户端
        const config = new qiniu.conf.Config();
        const region = qiniuChannel.region || 'z0';
        const QINIU_REGIONS = {
            'z0': qiniu.zone.Zone_z0,
            'z1': qiniu.zone.Zone_z1,
            'z2': qiniu.zone.Zone_z2,
            'na0': qiniu.zone.Zone_na0,
            'as0': qiniu.zone.Zone_as0
        };
        config.zone = QINIU_REGIONS[region] || qiniu.zone.Zone_z0;

        const resumeUploader = new qiniu.resume_up.ResumeUploader(config);
        const putExtra = new qiniu.resume_up.PutExtra();

        // 转换文件为Buffer
        const arrayBuffer = await file.arrayBuffer();
        const buffer = Buffer.from(arrayBuffer);

        console.log('Starting Qiniu resume upload...');

        // 执行分片上传
        return new Promise((resolve, reject) => {
            resumeUploader.putStream(
                uploadToken,
                fullId,
                buffer,
                putExtra,
                async (err, respBody, respInfo) => {
                    if (err) {
                        console.error('Qiniu resume upload error:', err);
                        return resolve(createResponse(`Error: Qiniu resume upload failed - ${err.message}`, { status: 500 }));
                    }

                    if (respInfo.statusCode === 200) {
                        console.log('Qiniu resume upload successful:', respBody);

                        // 更新metadata
                        metadata.Channel = "Qiniu";
                        metadata.ChannelName = qiniuChannel.name || "Qiniu_env";
                        metadata.FileSize = (respBody.fsize / 1024 / 1024).toFixed(2);
                        metadata.QiniuBucket = qiniuChannel.bucket;
                        metadata.QiniuRegion = region;
                        metadata.QiniuDomain = qiniuChannel.domain;
                        metadata.QiniuFileKey = respBody.key;
                        metadata.QiniuFileHash = respBody.hash;
                        
                        // 构建文件访问URL
                        const domain = qiniuChannel.domain || `${qiniuChannel.bucket}.qiniudn.com`;
                        metadata.QiniuFileUrl = `https://${domain}/${respBody.key}`;

                        // 图像审查
                        let moderateUrl = metadata.QiniuFileUrl;
                        metadata.Label = await moderateContent(env, moderateUrl);

                        // 写入KV数据库
                        try {
                            await db.put(fullId, "", { metadata });
                        } catch (error) {
                            console.error('Error writing to KV database:', error);
                            return resolve(createResponse('Error: Failed to write to KV database', { status: 500 }));
                        }

                        // 结束上传
                        waitUntil(endUpload(context, fullId, metadata));

                        // 返回成功响应
                        return resolve(createResponse(
                            JSON.stringify([{ 'src': returnLink }]),
                            {
                                status: 200,
                                headers: { 'Content-Type': 'application/json' }
                            }
                        ));
                    } else {
                        console.error('Qiniu resume upload failed:', respInfo.statusCode, respBody);
                        return resolve(createResponse(`Error: Qiniu resume upload failed - ${respInfo.statusCode} ${JSON.stringify(respBody)}`, { status: respInfo.statusCode }));
                    }
                }
            );
        });

    } catch (error) {
        console.error('Qiniu resume upload error:', error.message);
        return createResponse(`Error: Qiniu resume upload failed - ${error.message}`, { status: 500 });
    }
}

/**
 * 结束上传流程
 * @param {Object} context - 请求上下文
 * @param {string} fullId - 文件ID
 * @param {Object} metadata - 文件元数据
 */
async function endUpload(context, fullId, metadata) {
    const { env, url } = context;
    
    try {
        // 清理临时资源
        console.log('Qiniu upload completed for file:', fullId);
        
        // 如有需要，可在此处添加CDN缓存清理逻辑
        if (metadata.QiniuFileUrl) {
            await purgeCDNCache(env, metadata.QiniuFileUrl, url);
        }
        
    } catch (error) {
        console.error('Error in endUpload:', error);
    }
}
