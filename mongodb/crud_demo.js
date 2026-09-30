// Unit V walkthrough: MongoDB data types + CRUD + aggregation + geo query + explain.
// make mongo-crud   (runs: mongosh "<replica-set-uri>" /scripts/crud_demo.js)

db = db.getSiblingDB("argus");
const c = db.crud_demo;
c.drop();
const h = (t) => print(`\n==== ${t} ====`);

h("1. CREATE - insertOne with every common BSON data type");
c.insertOne({
  _id: ObjectId(),                          // ObjectId: 12-byte id (timestamp + random + counter)
  report_id: "A-1001",                      // String
  severity: NumberInt(3),                   // Int32
  vehicles_involved: NumberLong(2),         // Int64
  distance_mi: 0.42,                        // Double
  repair_cost_usd: NumberDecimal("12450.75"), // Decimal128: exact money arithmetic
  is_night: true,                           // Boolean
  reported_at: new Date("2023-01-14T18:25:00Z"), // Date (UTC milliseconds)
  location: { type: "Point", coordinates: [-80.19, 25.76] }, // Embedded doc (GeoJSON)
  road: { street: "I-95 N", city: "Miami", state: "FL", features: ["junction", "traffic_signal"] }, // nested + Array
  wind_chill_f: null,                       // Null: sensor had no reading
  notes: /lane\s+blocked/i,                 // Regular expression
});
printjson(c.findOne({}, { _id: 0 }));

h("insertMany");
c.insertMany([
  { report_id: "A-1002", severity: NumberInt(2), is_night: false, reported_at: new Date("2023-01-15T08:10:00Z"),
    location: { type: "Point", coordinates: [-80.21, 25.78] }, road: { street: "US-1", city: "Miami", state: "FL", features: ["crossing"] } },
  { report_id: "A-1003", severity: NumberInt(4), is_night: true, reported_at: new Date("2023-01-15T23:40:00Z"),
    location: { type: "Point", coordinates: [-95.37, 29.76] }, road: { street: "I-10 W", city: "Houston", state: "TX", features: ["junction"] } },
  { report_id: "A-1004", severity: NumberInt(2), is_night: false, reported_at: new Date("2023-01-16T17:05:00Z"),
    location: { type: "Point", coordinates: [-95.40, 29.74] }, road: { street: "Main St", city: "Houston", state: "TX", features: [] } },
]);
print(`documents: ${c.countDocuments()}`);

h("2. READ - filter, projection, sort, limit");
c.find({ severity: { $gte: 3 }, is_night: true }, { _id: 0, report_id: 1, severity: 1, "road.city": 1 })
  .sort({ severity: -1 }).limit(5).forEach(printjson);

h("READ - array operator ($in on array field) and $type");
c.find({ "road.features": { $in: ["junction"] } }, { _id: 0, report_id: 1 }).forEach(printjson);
print(`docs where wind_chill_f is explicitly null: ${c.countDocuments({ wind_chill_f: { $type: "null" } })}`);

h("3. UPDATE - $set, $inc, $push, upsert");
c.updateOne({ report_id: "A-1002" }, { $set: { severity: NumberInt(3) }, $push: { "road.features": "stop" } });
c.updateMany({ "road.state": "TX" }, { $inc: { vehicles_involved: NumberLong(1) } });
c.updateOne({ report_id: "A-9999" }, { $setOnInsert: { severity: NumberInt(1), created_by: "upsert" } }, { upsert: true });
printjson(c.findOne({ report_id: "A-1002" }, { _id: 0, severity: 1, "road.features": 1 }));

h("replaceOne");
c.replaceOne({ report_id: "A-9999" }, { report_id: "A-9999", severity: NumberInt(1), replaced: true });
printjson(c.findOne({ report_id: "A-9999" }, { _id: 0 }));

h("4. DELETE - deleteOne / deleteMany");
print(`deleted ${c.deleteOne({ report_id: "A-9999" }).deletedCount} (deleteOne)`);
print(`deleted ${c.deleteMany({ severity: { $lt: 2 } }).deletedCount} (deleteMany severity<2)`);

h("5. AGGREGATION pipeline - accidents & avg severity per state");
c.aggregate([
  { $group: { _id: "$road.state", accidents: { $sum: 1 }, avg_severity: { $avg: "$severity" } } },
  { $sort: { accidents: -1 } },
]).forEach(printjson);

h("6. GEO - accidents within 5 km of downtown Miami ($near needs a 2dsphere index)");
c.createIndex({ location: "2dsphere" });
c.find({ location: { $near: { $geometry: { type: "Point", coordinates: [-80.19, 25.76] }, $maxDistance: 5000 } } },
  { _id: 0, report_id: 1, "road.street": 1 }).forEach(printjson);

h("7. INDEXES + explain() - COLLSCAN vs IXSCAN");
const plan = () => c.find({ severity: 4 }).explain("executionStats").queryPlanner.winningPlan;
const stage = (p) => (p.inputStage ? `${p.stage} <- ${stage(p.inputStage)}` : p.stage);
print(`before index: ${stage(plan())}`);
c.createIndex({ severity: 1 });
print(`after index:  ${stage(plan())}`);

h("8. Real ARGUS data (if the pipeline has run)");
const hs = db.hotspots.find({}, { _id: 0, city: 1, state: 1, accidents: 1, risk_index: 1 }).sort({ risk_index: -1 }).limit(3).toArray();
if (hs.length) hs.forEach(printjson); else print("no hotspots yet - run `make ml`");
