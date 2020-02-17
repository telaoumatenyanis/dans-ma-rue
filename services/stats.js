const config = require("config");
const { flatMap, sortBy } = require("lodash/fp");
const indexName = config.get("elasticsearch.index_name");

exports.statsByArrondissement = async (client, callback) => {
  const res = await client.search({
    size: 0,
    index: indexName,
    body: {
      aggs: {
        arrondissement: {
          terms: {
            field: "arrondissement.keyword",
            order: { _count: "desc" },
            size: 100 // Because otherwise it only takes 10 values
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
            field: "type.keyword",
            order: { _count: "desc" },
            size: 5
          },
          aggs: {
            sous_type: {
              terms: {
                field: "sous_type.keyword",
                order: { _count: "desc" },
                size: 5
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
  // This solution would be using a date histogram, which is a bit clumsy, but at least does not require a new field in the documents
  //   const res = await client.search({
  //     size: 0,
  //     index: indexName,
  //     body: {
  //       aggs: {
  //         statsByMonth: {
  //           date_histogram: {
  //             field: "@timestamp",
  //             calendar_interval: "1M",
  //             format: "MM/yy",
  //             order: { _count: "desc" }
  //           }
  //         }
  //       }
  //     }
  //   });

  const res = await client.search({
    size: 0,
    index: indexName,
    body: {
      aggs: {
        date_declaration: {
          terms: {
            field: "date_declaration.keyword",
            order: { _count: "desc" },
            size: 10
          }
        }
      }
    }
  });

  const formattedResult = res.body.aggregations.date_declaration.buckets.map(
    bucket => ({
      month: bucket.key,
      count: bucket.doc_count
    })
  );

  callback(formattedResult);
};

exports.statsPropreteByArrondissement = async (client, callback) => {
  const res = await client.search({
    size: 0,
    index: indexName,
    body: {
      query: {
        bool: {
          must: {
            match: {
              type: "Propreté"
            }
          }
        }
      },
      aggs: {
        arrondissement: {
          terms: {
            field: "arrondissement.keyword",
            size: 3,
            order: { _count: "desc" }
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
