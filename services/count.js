const config = require("config");
const indexName = config.get("elasticsearch.index_name");

exports.count = async (client, from, to, callback) => {
  const res = await client.count({
    index: indexName,
    body: {
      query: {
        range: {
          "@timestamp": {
            format: "yyyy-MM-dd",
            gte: from,
            lte: to
          }
        }
      }
    }
  });

  callback({
    count: res.body.count
  });
};

exports.countAround = (client, lat, lon, radius, callback) => {
  callback({
    count: 0
  });
};
