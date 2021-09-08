module.exports = {
  environments: {
    dev: 'development',
    prod: 'production'
  },
  markerKey: 'next_event_marker_key',
  databaseTableName: 'UserScreenTime',
  maxItemsCountToSaveIntoDatabase: 25,
  maxItemsCountToGetFromDatabase: 100
};
