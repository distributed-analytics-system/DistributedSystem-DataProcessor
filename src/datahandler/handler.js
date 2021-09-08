'use strict';

const datastore = require('../datastore');
const config = require('../config');
const { markerKey } = require('../constants');
const { saveEventsByChunks, parseEvents, normalizeEvents, aggregateEvents, mergeEvents, getMarker } = require('./utilities');

module.exports = async () => {
  // Read the marker
  const marker = await getMarker();

  // Read the events
  const eventsPromise = await datastore.read({ bucketName: config.awsS3BucketName, token: marker })
  let events = await eventsPromise.data;
  // resolve bodies
  events = events.map(e => e.Body.toString());
  // parse bodies
  events = parseEvents(events);
  // flatten the nested events
  events = events.flat(Infinity);

  // Save next marker
  const nextMarker = eventsPromise.nextContinuationToken;
  if (nextMarker) {
    await datastore.write({ bucketName: config.awsS3BucketName, key: markerKey, data: nextMarker });
  }

  // Normalize events
  events = normalizeEvents(events);

  // Aggregate events
  events = aggregateEvents(events);

  // merge with existing data
  events = await mergeEvents(events);

  // save into database
  await saveEventsByChunks(events);
}
