const config = require("config");
const { flatMap } = require("lodash/fp");
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

  const formattedResult = res.body.aggregations.arrondissement.buckets.map(
    bucket => ({
      arrondissement: bucket.key,
      count: bucket.doc_count
    })
  );

  callback(formattedResult);
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

  const formattedResult = res.body.aggregations.type.buckets.map(bucket => ({
    type: bucket.key,
    count: bucket.doc_count,
    sous_types: bucket.sous_type.buckets.map(sous_type_bucket => ({
      sous_type: sous_type_bucket.key,
      count: sous_type_bucket.doc_count
    }))
  }));
  callback(formattedResult);
};

exports.statsByMonth = async (client, callback) => {
  const res = await client.search({
    size: 0,
    index: indexName,
    body: {
      aggs: {
        annee_declaration: {
          terms: {
            field: "annee_declaration.keyword"
          },
          aggs: {
            mois_declaration: {
              terms: {
                field: "mois_declaration.keyword"
              }
            }
          }
        }
      }
    }
  });
  const formattedResult = flatMap(bucket => {
    return bucket.mois_declaration.buckets.map(mois_bucket => ({
      month: `${mois_bucket.key}/${bucket.key}`,
      count: mois_bucket.doc_count
    }));
  }, res.body.aggregations.annee_declaration.buckets);

  callback(formattedResult);
};

exports.statsPropreteByArrondissement = async (client, callback) => {
  // TODO Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
  callback([]);
};
