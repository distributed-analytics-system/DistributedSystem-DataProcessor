'use strict';

const datastore = require('../datastore');
const database = require('../database');
const config = require('../config');
const { markerKey, databaseTableName } = require('../constants');

const { validate: validateUuid } = require('uuid');
const moment = require('moment');
const _ = require('lodash');

const validateTimestamp = (timestamp) => {
  const date = moment.unix(timestamp / 1000);
  return date.isValid() && date.isBefore();
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

const normalizeEvents = (events) => {
  // Remove invalids
  let validEvents = [];
  for (let event of events) {
    if (validateUuid(event['user-uuid']) && validateTimestamp(event.timestamp)) {
      validEvents.push(event);
    }
  }

  // Remove duplicates
  validEvents = validEvents.filter((v,i,a) => a.findIndex( t => (t['user-uuid'] === v['user-uuid'] && t.timestamp === v.timestamp && t.screen && v.screen)) === i);

  return validEvents;
}

const aggregateEvents = (events) => {
  // group elements by user-uuid
  events = _.mapValues(_.groupBy(events, 'user-uuid'), clist => clist.map(event => _.omit(event, 'user-uuid')));

  const sortOperand = (a, b) => {
    if (parseInt(a.timestamp) < parseInt(b.timestamp)) {
      return -1;
    }
    if (parseInt(a.timestamp) > parseInt(b.timestamp)){
      return 1;
    }

    return 0;
  }

  // calculate clicks count and time spent
  for (let key in events) {
    // sort events by timestamp
    events[key] = events[key].sort(sortOperand);

    // calculate clicks count
    let groupedClicks = _.mapValues(_.groupBy(events[key], 'screen'), clist => clist.map(click => _.omit(click, 'screen')));
    for (let c in groupedClicks) {
      const clickCount = groupedClicks[c].length;
      groupedClicks[c] = { clicks: clickCount, timespent: 0 };
    }

    // calculate time spent
    if (Array.isArray(events[key]) && events[key].length !== 0) {
      let lastUsedScreen = events[key][0].screen;
      let firstT = events[key][0].timestamp;
      for (let i = 1; i < events[key].length; ++i) {
        if(lastUsedScreen !== events[key][i].screen) {
          groupedClicks[lastUsedScreen].timespent += events[key][i].timestamp - firstT;

          lastUsedScreen = events[key][i].screen;
          firstT = events[key][i].timestamp;
        }
      }
    }

    events[key] = groupedClicks;
  }

  return events;
}

const handler = async () => {
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
  await database.save({ tableName: databaseTableName, data: events });
  console.log(events);
}

module.exports = handler;
