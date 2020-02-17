const config = require("config");
const indexName = config.get("elasticsearch.index_name");

exports.count = async (client, from, to, callback) => {
  callback({
    count: 0
  });
};

exports.countAround = (client, lat, lon, radius, callback) => {
  // TODO Compter le nombre d'anomalies autour d'un point géographique, dans un rayon donné
  callback({
    count: 0
  });
};
