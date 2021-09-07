'use strict';

const AWS = require("aws-sdk");
const { awsRegion } = require('../config');

AWS.config.update({
  region: awsRegion
});

const dynamodb = new AWS.DynamoDB();

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
    let params = {
      RequestItems: {}
    };
    params.RequestItems[options.tableName] = {
      Keys: generateGetItemsRequest(options.keys)
    };

    dynamodb.batchGetItem(params, function(err, data) {
      if (err) {
        console.error("Unable to get item", JSON.stringify(err));
        return reject(err);
      } else {
        return resolve(data.Responses[options.tableName]);
      }
    });
  });
}

module.exports = {
  save,
  get
}
