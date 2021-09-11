'use strict';

const { environments, databaseTableName } = require('../constants');
const config = require('../config');

const AWS = require("aws-sdk");
const { awsRegion } = require('../config');

AWS.config.update({
  region: awsRegion
});

const dynamoConfig = {};
if (config.nodeEnv === environments.dev) {
  dynamoConfig.endpoint = process.env.AWS_ENDPOINT;
}

const dynamodb = new AWS.DynamoDB(dynamoConfig);

const createTable = (cb) => {
  dynamodb.listTables(function(err, data) {
    if (err) {
      console.error({ message: err.message });
      cb(false);
    } else {
      let tableAlreadyExists = false;
      for (let tableName of data.TableNames) {
        if (tableName === databaseTableName) {
          tableAlreadyExists = true;
          break;
        }
      }

      if (!tableAlreadyExists) {
        const params = {
          TableName : databaseTableName,
          KeySchema: [       
              { AttributeName: 'UserUuid', KeyType: "HASH"}
          ],
          AttributeDefinitions: [       
              { AttributeName: "UserUuid", AttributeType: "S" }
          ],
          ProvisionedThroughput: {       
              ReadCapacityUnits: 10, 
              WriteCapacityUnits: 10
          }
        };

        dynamodb.createTable(params, function(err, data) {
          if (err) {
            console.error({ message: err.message });
            cb(false);
          } else {
            cb(true);
          }
        });
      } else {
        cb(true);
      }
    }
  });
}

const generateItemsRequest = (events) => {
  let response = [];
  for (let key in events) {
    response.push({
      PutRequest: {
        Item: {
          'UserUuid': {
            S: key
          },
          'Data': {
            S: JSON.stringify(events[key])
          }
        }
      }
    });
  }

  return response;
}

const generateGetItemsRequest = (keys) => {
  return keys.map((key) => {
    return {
       "UserUuid": {
         S: key
      }
    }
  });
}

const save = (options) => {
  return new Promise((resolve, reject) => {
    let params = {
      RequestItems: {}
    };
    params.RequestItems[options.tableName] = generateItemsRequest(options.data);

    console.debug(`Sending batchWriteItem with the following params: ${JSON.stringify(params)}`);
    dynamodb.batchWriteItem(params, function(err, data) {
      if (err) {
        console.error("Unable to add item: ", JSON.stringify(err));
        return reject(err);
      } else {
        console.debug("Added item: ", JSON.stringify(data));
        return resolve(data);
      }
    });
  });
};

const get = (options) => {
  return new Promise((resolve, reject) => {
    // first create table if not exists
    createTable((created) => {
      if (created) {
        let params = {
          RequestItems: {}
        };
        params.RequestItems[options.tableName] = {
          Keys: generateGetItemsRequest(options.keys)
        };

        console.debug(`Sending batchGetItem with the following params: ${JSON.stringify(params)}`);
        dynamodb.batchGetItem(params, function(err, data) {
          if (err) {
            console.error("Unable to get item", JSON.stringify(err));
            return reject(err);
          } else {
            return resolve(data.Responses[options.tableName]);
          }
        });
      } else {
        console.error('Could not create table')
        return reject({ message: 'Failed to create table' });
      }
    })
  });
}

module.exports = {
  save,
  get
}
