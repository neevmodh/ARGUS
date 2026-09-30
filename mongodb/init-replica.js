// Idempotent: initiate the 3-node replica set, then create collections + indexes.
// Run by the mongo-init container: mongosh --host mongo1 /scripts/init-replica.js

try {
  rs.status();
  print("[argus] replica set already initiated");
} catch (e) {
  rs.initiate({
    _id: "rs0",
    members: [
      { _id: 0, host: "mongo1:27017", priority: 2 },
      { _id: 1, host: "mongo2:27017", priority: 1 },
      { _id: 2, host: "mongo3:27017", priority: 1 },
    ],
  });
  print("[argus] replica set rs0 initiated");
}

let tries = 0;
while (!db.hello().isWritablePrimary && tries < 60) {
  sleep(1000);
  tries++;
}
if (!db.hello().isWritablePrimary) {
  // Connected to a secondary: hand off to the primary for the DDL below.
  db = connect(`mongodb://${db.hello().primary}/argus`);
}

db = db.getSiblingDB("argus");

const ensure = (name, opts = {}) => {
  if (!db.getCollectionNames().includes(name)) db.createCollection(name, opts);
};

// Batch-layer outputs
ensure("hotspots");
db.hotspots.createIndex({ location: "2dsphere" });
db.hotspots.createIndex({ risk_index: -1 });

ensure("rules");
db.rules.createIndex({ lift: -1 });

ensure("forecasts");
db.forecasts.createIndex({ place: 1, date: 1 }, { unique: true });

ensure("weather_risk");
db.weather_risk.createIndex({ bucket: 1 }, { unique: true });

ensure("models");
db.models.createIndex({ name: 1 }, { unique: true });

ensure("stats_tests");
ensure("meta");
ensure("mr_state_hour");
db.mr_state_hour.createIndex({ state: 1, hour: 1 });
ensure("mr_severity_stats");
db.mr_severity_stats.createIndex({ group_by: 1, key: 1 });
ensure("mr_top_streets");
db.mr_top_streets.createIndex({ rank: 1 });

// Speed-layer outputs. Live events expire after 24h (TTL index).
ensure("live_events");
db.live_events.createIndex({ location: "2dsphere" });
db.live_events.createIndex({ ingested_at: 1 }, { expireAfterSeconds: 86400 });
db.live_events.createIndex({ risk_score: -1 });

ensure("live_windows");
db.live_windows.createIndex({ state: 1, window_start: 1 }, { unique: true });
db.live_windows.createIndex({ window_start: -1 });

print("[argus] collections and indexes ready: " + db.getCollectionNames().sort().join(", "));
