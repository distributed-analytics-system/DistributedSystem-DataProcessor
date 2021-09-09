'use strict';

const config = require('../config');
const { environments } = require('../constants');

const AWS = require('aws-sdk');

const s3Config = {};
if (config.nodeEnv === environments.dev) {
  s3Config.endpoint = process.env.AWS_ENDPOINT;
  s3Config.s3ForcePathStyle = true;
}

const s3 = new AWS.S3(s3Config);

const write = (options) => {
  return new Promise((resolve, reject) => {
    s3.upload({ Bucket: options.bucketName, Key: options.key, Body: Buffer.from(options.data) }, function(err, data) {
      if(err) {
        return reject(err);
      }

      return resolve(data);
    });
  });
}

const readObject = (options) => {
  return new Promise((resolve, reject) => {
    s3.getObject({ Bucket: options.bucketName, Key: options.key }, function(err, data) {
      if(err) {
        return reject(err);
      }

      return resolve(data);
    });
  });
}

const read = (options) => {
  return new Promise((resolve, reject) => {
    /*
    {
      Bucket: 'STRING_VALUE', required
      Delimiter: 'STRING_VALUE',
      EncodingType: url,
      ExpectedBucketOwner: 'STRING_VALUE',
      Marker: 'STRING_VALUE',
      MaxKeys: 'NUMBER_VALUE',
      Prefix: 'STRING_VALUE',
      RequestPayer: requester
    }
    */

    s3.listObjectsV2({ Bucket: options.bucketName, ContinuationToken: options.token, Prefix: options.prefix }, function(err, data) {
      if(err) {
        return reject(err);
      }

      let nextContinuationToken = null;
      if (data.IsTruncated) {
        nextContinuationToken = data.NextContinuationToken;
      }
      let dataRequests = data.Contents.map(e => e.Key).map(key => readObject({ bucketName: options.bucketName, key }));
      return resolve({ nextContinuationToken, data: Promise.all(dataRequests) });
    });
  });
}

module.exports = {
  write,
  read
};

/* delete later TODO
read({ bucketName: 'distributed-system-analytics', prefix: 'test' }).then((d) => {
  console.log(d)
  console.log(d.data.map(dd => dd.Body.toString()));
}).catch((err) => {
  console.error(err);
})
*/
