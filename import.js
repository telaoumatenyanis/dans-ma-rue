const config = require("config");
const csv = require("csv-parser");
const fs = require("fs");
const { Client } = require("@elastic/elasticsearch");
const moment = require("moment");
const { chunk } = require("lodash/fp");
const bluebird = require("bluebird");

const indexName = config.get("elasticsearch.index_name");
const MAX_CHUNK_SIZE = 10000;

async function run() {
  // Create Elasticsearch client
  const client = new Client({ node: config.get("elasticsearch.uri") });

  // Index creation
  try {
    if (!(await checkIfIndexExists(client, indexName))) {
      await client.indices.create({
        index: indexName,
        body: {
          mappings: {
            properties: {
              location: { type: "geo_point" }
            }
          }
        }
      });
      console.log("Created index 'anomalies'");
    } else {
      console.log("Index already exists, skipping index creation");
    }
  } catch (err) {
    console.trace(err.message);
  }

  const anomalies = [];
  let i = 0;
  // Read CSV file
  fs.createReadStream("dataset/dans-ma-rue.csv")
    .pipe(
      csv({
        separator: ";"
      })
    )
    .on("data", data => {
      // Push current data
      anomalies.push({
        "@timestamp": data.DATEDECL,
        object_id: data.OBJECTID,
        annee_declaration: data["ANNEE DECLARATION"],
        mois_declaration: data["MOIS DECLARATION"],
        type: data.TYPE,
        sous_type: data.SOUSTYPE,
        code_postal: data.CODE_POSTAL,
        ville: data.VILLE,
        arrondissement: data.ARRONDISSEMENT,
        prefixe: data.PREFIXE,
        intervenant: data.INTERVENANT,
        conseil_de_quartier: data["CONSEIL DE QUARTIER"],
        location: data.geo_point_2d
      });
    })
    .on("end", async () => {
      try {
        /**
         * It is needed to chunk the data in order to not exceed the elasticsearch query limit.
         * In order to limit load on ES, maximum concurrent running bulk are 10
         */
        console.log("Inserting " + anomalies.length + " anomalies");
        await bluebird.map(
          chunk(MAX_CHUNK_SIZE, anomalies),
          anomalyChunk => {
            return client.bulk(createBulkInsertQuery(anomalyChunk));
          },
          { concurrency: 10 }
        );
      } catch (err) {
        console.trace(err);
      } finally {
        client.close();
      }
    });
}

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(anomalies) {
  const body = anomalies.reduce((acc, anomaly) => {
    const { object_id, ...params } = anomaly;
    acc.push({
      index: { _index: indexName, _type: "_doc", _id: object_id }
    });
    acc.push(params);
    return acc;
  }, []);

  return { body };
}

/**
 * Check if an index already exists
 *
 * @param   {any}     client      The elasticsearch client
 * @param   {String}  indexName   The name of the index to check
 *
 * @return  {Boolean}             true if index already exists, false otherwise
 */
async function checkIfIndexExists(client, indexName) {
  return (await client.indices.exists({ index: indexName })).body;
}

run().catch(console.error);
