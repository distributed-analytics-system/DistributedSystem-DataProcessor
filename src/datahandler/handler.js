'use strict';

const datastore = require('../datastore');
const config = require('../config');
const { markerKey } = require('../constants');

/* delete later TODO
const path = require('path');
require('dotenv').config({ path: path.resolve(process.cwd(), '.env-dev') });
*/

const getMarker = async () => {
  let marker = null;
  const markerDataPromise = await datastore.read({ bucketName: config.awsS3BucketName, prefix: markerKey });
  const markerData = await markerDataPromise.data;
  if (markerData.length !== 0 && markerData[0].Body) {
    marker = JSON.parse(markerData[0].Body.toString());
  }

  return marker;
}

module.exports = async () => {
  // Read the marker
  const marker = await getMarker();

  // Read the events
  const eventsPromise = await datastore.read({ bucketName: config.awsS3BucketName, token: marker })
  const events = await eventsPromise.data;

  // Save next marker
  const nextMarker = eventsPromise.nextContinuationToken;
  if (nextMarker) {
    await datastore.write({ bucketName: config.awsS3BucketName, key: markerKey, data: nextMarker });
  }

  // TODO: save into dynamo db
  console.log(events);
}
