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

  const result = res.body.aggregations.arrondissement.buckets.map(bucket => ({
    arrondissement: bucket.key,
    count: bucket.doc_count
  }));

  callback(result);
};

exports.statsByType = async (client, callback) => {
  const res = await client.search({
    size: 0,
    index: indexName,
    body: {
      aggs: {
        type: {
          terms: {
            field: "type.keyword"
          },
          aggs: {
            sous_type: {
              terms: {
                field: "sous_type.keyword"
              }
            }
          }
        }
      }
    }
  });

  const result = res.body.aggregations.type.buckets.map(bucket => ({
    type: bucket.key,
    count: bucket.doc_count,
    sous_types: bucket.sous_type.buckets.map(sous_type_bucket => ({
      sous_type: sous_type_bucket.key,
      count: sous_type_bucket.doc_count
    }))
  }));
  callback(result);
};

exports.statsByMonth = async (client, callback) => {
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
  console.log(res);
  callback([]);
};

exports.statsPropreteByArrondissement = async (client, callback) => {
  // TODO Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
  callback([]);
};
