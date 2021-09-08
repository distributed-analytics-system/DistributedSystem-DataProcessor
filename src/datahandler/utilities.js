'use strict';

const { markerKey, databaseTableName, maxItemsCountToSaveIntoDatabase } = require('../constants');
const config = require('../config');

const datastore = require('../datastore');
const database = require('../database');

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

const mergeEvents = async (events) => {
  const exisitingData = await database.get({ tableName:databaseTableName, keys: Object.keys(events) });
  let exisitingEvents = {};
  for (let ed of exisitingData) {
    exisitingEvents[Object.values(ed['UserUuid'])] = JSON.parse(Object.values(ed.Data));
  }

  for (let key in events) {
    if (exisitingEvents[key]) {
      for (let k in events[key]) {
        if (exisitingEvents[key][k]) {
          events[key][k].clicks += exisitingEvents[key][k].clicks;
          events[key][k].timespent += exisitingEvents[key][k].timespent;
        }
      }
    }
  }

  return events;
}

// Workaround for limitation from DynamoDB - max 25 items for batchPutItem
const saveEventsByChunks = async (events) => {
  const eventsArray = Object.keys(events).map((key) => {
    return {
      'user-uuid': key,
      ...events[key]
    }
  });

  const chunkedEvents = eventsArray.reduce((resultArray, item, index) => { 
    const chunkIndex = Math.floor(index / maxItemsCountToSaveIntoDatabase);

    if (!resultArray[chunkIndex]) {
      resultArray[chunkIndex] = [] // start a new chunk
    }

    resultArray[chunkIndex].push(item)

    return resultArray
  }, []);


  for (let chunk of chunkedEvents) {
    let eventsToBeSaved = {};
    for (let event of chunk) {
      eventsToBeSaved[event['user-uuid']] = _.omit(event, 'user-uuid');
    }

    await database.save({ tableName: databaseTableName, data: eventsToBeSaved });
  }
}

module.exports = {
  parseEvents,
  normalizeEvents,
  aggregateEvents,
  mergeEvents,
  getMarker,
  saveEventsByChunks
}
