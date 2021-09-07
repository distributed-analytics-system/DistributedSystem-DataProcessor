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

const save = (options) => {
  return new Promise((resolve, reject) => {
    let params = {
      RequestItems: {}
    };
    params.RequestItems[options.tableName] = generateItemsRequest(options.data);

    dynamodb.batchWriteItem(params, function(err, data) {
      if (err) {
        console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
        return reject(err);
      } else {
        console.log("Added item:", JSON.stringify(data, null, 2));
        return resolve(data);
      }
    });
  });
};

module.exports = {
  save
}
