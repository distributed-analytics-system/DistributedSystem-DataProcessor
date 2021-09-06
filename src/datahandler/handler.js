'use strict';

const datastore = require('../datastore');
const config = require('../config');
const { markerKey } = require('../constants');

const { validate: validateUuid } = require('uuid');

const validateTimestamp = (timestamp) => {
  return (new Date(timestamp)).getTime() > 0;
}

const parseEvents = (events) => {
  return events.map((event) => {
    try {
      const e = JSON.parse(event);
      return e;
    } catch (e) {
      console.debug('Invalid event: ', event);
    }
  }).filter(e => e);
}

const getMarker = async () => {
  let marker = null;
  const markerDataPromise = await datastore.read({ bucketName: config.awsS3BucketName, prefix: markerKey });
  const markerData = await markerDataPromise.data;
  if (markerData.length !== 0 && markerData[0].Body) {
    marker = JSON.parse(markerData[0].Body.toString());
  }

  return marker;
}

// TODO: fix issue
const normalizeEvents = (events) => {
  let validEvents = [];
  for (let event of events) {
    if (validateUuid(event['user-uuid']) && (new Date(event.timestamp)).getTime() > 0) {
      validEvents.push(event);
    }
  }

  return validEvents;
}

// TODO finalize issue
const aggregateEvents = (events) => {
  return events;
}

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

  // TODO: save into dynamo db
  console.log(events);
}
