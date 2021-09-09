'use strict';

const { environments } = require('./src/constants');
const path = require('path');

if (!process.env.NODE_ENV || process.env.NODE_ENV === environments.dev) {
  require('dotenv').config({ path: path.resolve(process.cwd(), '.env-dev') });
}

const { dataHandler } = require('./src/datahandler');

exports.handler = async (event) => {
  try {
    await dataHandler();
    console.log('Events successfully processed');
  } catch(err) {
    console.error('There was an error while processing events: ', err.message);
  }

  return {
    statusCode: 200
  };
};

if (process.env.NODE_ENV === environments.dev) {
  // Calling the lambda function every 30seconds to simulate auto scheduling on AWS
  setInterval(exports.handler, 30000);
}

