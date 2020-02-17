const config = require("config");
const indexName = config.get("elasticsearch.index_name");

exports.statsByArrondissement = async (client, callback) => {
  const res = await client.search({
    size: 0,
    index: indexName,
    body: {
      aggs: {
        arrondissement: {
          terms: {
            field: "arrondissement.keyword"
          }
        }
      }
    }
  });

  callback(res.body.aggregations.arrondissement.buckets);
};

exports.statsByType = async (client, callback) => {
  callback([]);
};

exports.statsByMonth = async (client, callback) => {
  // TODO Trouver le top 10 des mois avec le plus d'anomalies
  callback([]);
};

exports.statsPropreteByArrondissement = async (client, callback) => {
  // TODO Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
  callback([]);
};
