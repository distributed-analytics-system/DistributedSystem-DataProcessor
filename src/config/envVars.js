const { str, port, num } = require('envalid');

module.exports = {
  nodeEnv: {
    name: 'NODE_ENV',
    validator: str({choices: ['development', 'production']})
  },
  /** ************** AWS credentials *********************/
  awsAccessKeyId: {
    name: 'AWS_ACCESS_KEY_ID',
    validator: str()
  },
  awsSecretAccessKey: {
    name: 'AWS_SECRET_ACCESS_KEY',
    validator: str()
  },
  awsS3BucketName: {
    name: 'AWS_S3_BUCKET_NAME',
    validator: str()
  },
  awsRegion: {
    name: 'AWS_REGION',
    validator: str()
  }
};
