'use strict';

const { environments, databaseTableName, maxItemsCountToGetFromDatabase } = require('../constants');
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

const getItems = (options) => {
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

const get = async (options) => {
  const chunkedKeys = options.keys.reduce((resultArray, item, index) => { 
    const chunkIndex = Math.floor(index / maxItemsCountToGetFromDatabase);

    if (!resultArray[chunkIndex]) {
      resultArray[chunkIndex] = [] // start a new chunk
    }

    resultArray[chunkIndex].push(item)

    return resultArray
  }, []);


  let result = [];
  for (let chunk of chunkedKeys) {
    const data = await getItems({ tableName: options.tableName, keys: chunk });
    result.push(data);
  }

  return result;
}

module.exports = {
  save,
  get
}
