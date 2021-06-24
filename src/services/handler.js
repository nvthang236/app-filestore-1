'use strict';

const fs = require('fs');
const path = require('path');
const stream = require('stream');

const mkdirp = require('mkdirp');
const rimraf = require('rimraf');
const mv = require('mv');
const uuid = require('uuid');
const sharp = require('sharp');

const Devebot = require('devebot');
const Promise = Devebot.require('bluebird');
const lodash = Devebot.require('lodash');
const pipeline = Promise.promisify(stream.pipeline);

const stringUtil = require('../supports/string-util');

const { S3 } = require('aws-sdk');
const config = {
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  params: { Bucket: process.env.AWS_BUCKET },
};
const s3 = new S3(config);
const s3Handler = new S3Handler(s3);

function Handler(params = {}) {
  const { loggingFactory, mongoManipulator } = params;

  const L = loggingFactory.getLogger();
  const T = loggingFactory.getTracer();

  const pluginCfg = params.sandboxConfig || {};
  const contextPath = pluginCfg.contextPath || '/filestore';
  const uploadDir = pluginCfg.uploadDir;

  this.getFileUrls = function(fileIds = []) {
    return Promise.map(fileIds, function(fileId) {
      const r = mongoManipulator.findOneDocument(pluginCfg.collections.FILE, {fileId});
      return r.then(function(fileData) {
        if (lodash.isEmpty(fileData)) {
          return { fileId }
        } else {
          return lodash.pick(fileData, ['_id', 'fileId', 'fileUrl']);
        }
      })
    }, {concurrency: 4});
  }

  /**
   * 
   * @param {*} args
   *   fileId: UUID
   *   fileType: 'path', 'stream' or 'base64'
   *   fileSource: url, stream, or base64 String
   *   fileInfo: (size, name, path. ...)
   */
  this.saveFile = function(args = {}) {
    let {fileId, fileType, fileSource, fileInfo} = args;

    L.has('debug') && L.log('debug', ' - saveFile: %s', JSON.stringify(args, null, 2));

    fileId = fileId || uuid.v4();
    fileInfo = fileInfo || {};
    fileInfo.name = fileInfo.name || fileId;

    fileInfo.originalName = fileInfo.name;
    fileInfo.name = stringUtil.slugify(fileInfo.name);

    let fileName = fileInfo.name;
    let ctx = {};

    return Promise.resolve()
    .then(function(result) {
      fileInfo.fileId = fileId;
      fileInfo.status = 'intermediate';

      return mongoManipulator.updateDocument(
        pluginCfg.collections.FILE,
        { fileId: fileId }, fileInfo, { multi: true, upsert: true });
    })
    .then(function() {
      if (fileInfo.s3) {
        ctx.uploadDirPath = fileId;
        return;
      }
      ctx.uploadDirPath = path.join(uploadDir, fileId);
      return Promise.promisify(mkdirp)(ctx.uploadDirPath);
    })
    .then(function() {
      if (fileInfo.s3) {
        return s3Handler.upload(fileSource, {
          path: path.join(ctx.uploadDirPath, fileName),
          type: fileInfo.type,
        });
      }
      switch(fileType) {
        case 'path':
        return Promise.promisify(function(done) {
          // fileSource is the path of temporary file in this scenario
          mv(fileSource, path.join(ctx.uploadDirPath, fileName), function(err) {
            done(err);
          });
        })();
        case 'base64':
        // fileSource is the file content in base64 format
        const fs_writeFile = Promise.promisify(fs.writeFile, {context: fs});
        fileSource = fileSource.replace(/^data:image\/[a-zA-Z0-9]*;base64,/, "");
        return fs_writeFile(path.join(ctx.uploadDirPath, fileName), fileSource, {
          encoding: 'base64'
        });
      }
    })
    .then(function() {
      fileInfo.path = path.join(ctx.uploadDirPath, fileName);
      fileInfo.fileUrl = path.join(contextPath, '/download/' + fileId);
      fileInfo.status = 'ok';
      return mongoManipulator.updateDocument(
        pluginCfg.collections.FILE,
        { fileId: fileId }, fileInfo, { multi: true, upsert: false });
    })
    .then(function() {
      if (fileInfo.s3) {
        return Promise.promisify(rimraf)(path.join(uploadDir, fileId));
      } else if (
        !lodash.isEmpty(config.accessKeyId) &&
        !lodash.isEmpty(config.secretAccessKey) &&
        !lodash.isEmpty(config.params.Bucket)
      ) {
        return s3Handler.delete(fileId);
      }
    })
    .then(function() {
      const fileCollection = mongoManipulator.mongojs.collection(pluginCfg.collections.FILE);
      const findOne = Promise.promisify(fileCollection.findOne, { context: fileCollection });
      return findOne({ fileId: fileId });
    })
    .then(function(doc) {
      L.has('debug') && L.log('debug', T.toMessage({
        text: 'The /upload has been done successfully'
      }));
      let returnInfo = {};
      returnInfo['_id'] = doc._id;
      returnInfo['fileId'] = doc.fileId;
      returnInfo['fileUrl'] = doc.fileUrl;
      return returnInfo;
    });
  }

  this.uploadFromDisk = async function(args = {}) {
    L.has('debug') &&
      L.log('debug', ' - uploadFromDisk: %s', JSON.stringify(args, null, 2));

    const { fileSource, fileInfo } = args;
    const { fileId, name: fileName } = fileInfo;

    fileInfo.path = path.join(fileId, fileName);
    await s3Handler.upload(fileSource, fileInfo);

    fileInfo.s3 = true;
    await mongoManipulator.updateDocument(
      pluginCfg.collections.FILE,
      { fileId: fileId },
      fileInfo,
      { multi: true, upsert: false }
    );

    if (fileSource.match(uploadDir)) {
      await Promise.promisify(rimraf)(path.dirname(fileSource));
    }

    L.has('debug') &&
      L.log('debug', ' - uploadFromDisk has been done successfully');
  };

  this.getFileS3 = function(args = {}) {
    const { path, writable } = args;
    return new Promise((resolve, reject) => {
      const s3Stream = s3Handler.readStream(path);
      s3Stream.on('end', function() {
        L.has('silly') && L.log('silly', ' - the file has been full-loaded');
      });
      pipeline(s3Stream, writable).then(resolve).catch(reject);
    });
  };

  this.resizePicture = function(args = {}) {
    const { fileInfo, width, height, thumbnailFile } = args;
    return s3Handler.exists(thumbnailFile).then((exists) => {
      if (exists) {
        return;
      }
      return new Promise((resolve, reject) => {
        const s3GetStream = s3Handler.readStream(fileInfo.path);
        const resizeStream = sharp().resize({
          width: +width,
          height: +height,
          fit: 'cover',
        });
        const { writeStream, uploaded } = s3Handler.writeStream(
          thumbnailFile,
          fileInfo.type
        );
        pipeline(s3GetStream, resizeStream, writeStream).catch(reject);
        uploaded.then(resolve).catch(reject);
      });
    });
  };
}

function S3Handler(S3) {
  this.upload = function(source, { path, type }) {
    return new Promise((resolve, reject) => {
      const readStream = fs.createReadStream(source);
      const { writeStream, uploaded } = this.writeStream(path, type);
      pipeline(readStream, writeStream).catch(reject);
      uploaded.then(resolve).catch(reject);
    });
  };

  this.readStream = function(path) {
    return S3.getObject({ Key: path }).createReadStream();
  };

  this.writeStream = function(path, contentType) {
    const passThrough = new stream.PassThrough();
    return {
      writeStream: passThrough,
      uploaded: S3.upload({
        ContentType: contentType,
        Body: passThrough,
        Key: path,
      }).promise(),
    };
  };

  this.exists = function(path) {
    return S3.headObject({
      Key: path,
    })
      .promise()
      .then(
        () => true,
        (error) => {
          if (error.code === 'NotFound') {
            return false;
          }
          throw error;
        }
      );
  };

  this.delete = async function (path, type) {
    if (type === 'file') {
      return S3.deleteObject({ Key: path }).promise();
    }
    const listedObjects = await S3.listObjectsV2({ Prefix: path }).promise();
    if (listedObjects.Contents.length === 0) {
      return;
    }
    const deleteObjects = listedObjects.Contents.map(({ Key }) => ({ Key }));
    await S3.deleteObjects({ Delete: { Objects: deleteObjects } }).promise();
    if (listedObjects.IsTruncated) {
      await this.delete(path);
    }
  };
}

function base64MimeType(encoded) {
  var result = null;
  if (typeof encoded !== 'string') {
    return result;
  }
  var mime = encoded.match(/data:([a-zA-Z0-9]+\/[a-zA-Z0-9-.+]+).*,.*/);
  if (mime && mime.length) {
    result = mime[1];
  }
  return result;
}

Handler.referenceHash = {
  initializer: 'initializer',
  errorManager: 'app-errorlist/manager',
  mongoManipulator: "mongojs#manipulator"
};

module.exports = Handler;

