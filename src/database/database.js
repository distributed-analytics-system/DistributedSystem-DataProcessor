'use strict';

const AWS = require("aws-sdk");
AWS.config.update({
  region: "us-east-2" // TODO: move to constants
});

const dynamodb = new AWS.DynamoDB();

const generateItemsRequest = (events) => {
  return events.map((event) => {
    return {
      PutRequest: {
        Item: {
          'UserUuid': {
            S: event['user-uuid']
          },
          'Timestamp': {
            N: event.timestamp
          },
          'Screen': {
            S: event.screen
          }
        }
      }
    }
  });
}

const save = (options) => {
  return new Promise((resolve, reject) => {
    const params = {
      RequestItems: {
        'UserScreenTime': generateItemsRequest(options.data) // TODO: fix table name
      }
    };

    console.log(JSON.stringify(params))
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
