
import { ApiResponse, Client } from "@elastic/elasticsearch";
import axios from "axios";
import log4js from "log4js";
import { Job, scheduleJob } from "node-schedule";
import xml2js from "xml2js";
import { parseBooleans } from "xml2js/lib/processors";
import fences from "./fences.json";

const nvasUrl: string = "http://webservices.nextbus.com/service/publicXMLFeed";

interface IVehicleLocationParameters {
  command: string;
  a: string;
  t?: number;
  r?: string;
}

type WriterFunction = (location: any[]) => Promise<ApiResponse<any, any>>;

function fetchNvasLocations( write: WriterFunction ): () => void {

  let lastTimeMillis: number = 0;

  return async () => {
    const vlParams: IVehicleLocationParameters = {
      command: "vehicleLocations",
      a: "ttc",
      t: lastTimeMillis,
    };

    const response = await axios.get(nvasUrl, { params : vlParams, headers: { "Accept-Encoding": "gzip, deflate" } });

    if (response.status === 200) {

      const xmlParser = (new xml2js.Parser()).parseString;

      xmlParser( response.data,  async (e: any, r: any) => {

        lastTimeMillis = parseInt(r.body.lastTime[0].$.time, 10);
        const locations: any[] = [];
        r.body.vehicle.forEach( (l: { $: any; }) => {

          const locationDoc = {
            location: { lat: parseFloat(l.$.lat), lon: parseFloat(l.$.lon) },
            secsSinceReport: parseInt(l.$.secsSinceReport, 10),
            nvasLastTimestamp: new Date( lastTimeMillis),
            routeTag: l.$.routeTag,
            dirTag: l.$.dirTag,
            vehicleId: l.$.id,
            predictable: parseBooleans(l.$.predictable),
            heading: parseInt(l.$.heading, 10),
            speedKmHr: parseInt(l.$.speedKmHr, 10),
          };

          locations.push(locationDoc);
        });

        try {
          const writeResponse = await write(locations);
          logger.info(`wrote ${locations.length} vehicle location updates to ES`);
        } catch (e) {
          logger.warn(e);
        }
      });
    }
  };
}

function fetchGeoFenceHitsAndClearIndex( client: Client,
                                         indexHits: string,
                                         indexLocations: string,
                                         write: WriterFunction ): () => void {

  return async () => {

    const query = {
      index: indexLocations,
      size: 1000,
      body: {
        query: {
          bool: {
            should: fences.features.map( (f) => {
              return {
                bool: {
                  filter: {
                    geo_polygon: {
                      location: {
                        points: f.geometry.coordinates[0],
                      },
                    },
                  },
                },
              };
            }),
            minimum_should_match: 1,
          },
        },
      },
    };
    // query all the geofences
    try {
      const resultInsideFences = await client.search( query );
      await client.deleteByQuery(
        {
          index: indexHits,
          body: { query: { match_all: {} }}},
      );

      const hitsUpdate = resultInsideFences.body.hits.hits.map( (h: { _source: any; }) => {

        const action = {
          update : {
            _id: h._source.vehicleId,
            _index: indexHits,
            _type: "_doc",
          },
        };

        const data = {
          doc : {
            vehicleId: h._source.vehicleId,
            hitValue: 10,
          },
          doc_as_upsert: true,
        };

        return `${JSON.stringify(action)}\n${JSON.stringify(data)}`;
      }).join("\n") + "\n";

      client.bulk({
        index: indexHits,
        refresh: "true",
        body: hitsUpdate,
      });

    } catch (e) {
      // console.log(e);
    }

  };

}

async function checkIndex(client: Client, idxName: string, mapping: object) {

  logger.info(`Checking for index: ${idxName}`);
  const response: ApiResponse =  await client.indices.exists({index: idxName});

  if (response.statusCode === 200) {
    logger.info(`${idxName} already exists`);
  }

  if (response.statusCode === 404) {
    const createResp: ApiResponse = await client.indices.create( {
        index: idxName,
        body: {
          mappings: {
            properties: mapping,
          },
        },
    });
    logger.info( createResp.statusCode === 200 ? `created index ${idxName}` : `failed to create index ${idxName}`);
  }
}

function esWriter(client: Client, idxName: string): WriterFunction {
  return async ( locations: any[] ): Promise<ApiResponse<any, any>> => {

    const bulk = locations.map( (l) => locationToBulkUpdate(l, idxName) ).join("\n") + "\n";
    // logger.info( bulkUpdate );
    return client.bulk({
      index: idxName,
      refresh: "true",
      body: bulk,
    });
  };
}

function locationToBulkUpdate( doc: any, idxName: string ): string {

  const action = {
    update : {
      _id: doc.vehicleId,
      _index: idxName,
      _type: "_doc",
    },
  };

  const data = {
    doc,
    doc_as_upsert: true,
  };

  return `${JSON.stringify(action)}\n${JSON.stringify(data)}`;

}

// ----------------

const esClient = new Client({
  node: "http://elastic:changeme@localhost:9200",
});

log4js.addLayout("json", (config) => {
  return (logEvent) => JSON.stringify(logEvent);
});

log4js.configure({
  appenders: {
    out: { type: "stdout", layout: { type: "json" } },
    // file: { type: "file", filename: "nvas-locations.log", layout: { type: "json" } },
  },
  categories: {
    default: { appenders: ["out" /*, "file"*/ ], level: "info" },
  },
});

const logger: log4js.Logger = log4js.getLogger();

const vehicleLocationsIndex: string = "poc-vehicle-locations";
const vehicleLocationsMapping = {
  location: { type: "geo_point" },
  secsSinceReport: { type: "integer" },
  nvasLastTimestamp: { type: "date" },
  routeTag: { type: "text" },
  dirTag: { type: "keyword" },
  vehicleId: { type: "keyword" },
  predictable: { type: "boolean" },
  heading: { type: "integer" },
  speedKmHr: { type: "integer" },
};

const vehicleHitsIndex: string = "poc-vehicle-hits";
const vehicleHitsMapping = {
  vehicleId: { type: "keyword" },
  hitValue: { type: "integer" },
};

const geoFenceIndex: string = "poc-geo-fences";
const geoFenceMapping = {
  coordinates: { type: "geo_shape" },
  name: { type: "keyword" },
  region: { type: "long" },
};

checkIndex(esClient, vehicleLocationsIndex, vehicleLocationsMapping);
checkIndex(esClient, vehicleHitsIndex, vehicleHitsMapping);
checkIndex(esClient, geoFenceIndex, geoFenceMapping);

const bulkUpdate = fences.features.map( (f) => {
  const action = {
    update : {
      _id: f.id,
      _index: geoFenceIndex,
      _type: "_doc",
    },
  };

  const data = {
    doc : {
      coordinates: f.geometry,
      name: f.properties.name,
      region: f.properties.region,
    },
    doc_as_upsert: true,
  };

  return `${JSON.stringify(action)}\n${JSON.stringify(data)}`;
}).join("\n") + "\n";

esClient.bulk({
  index: geoFenceIndex,
  refresh: "true",
  body: bulkUpdate,
});

const writer = esWriter( esClient, vehicleLocationsIndex );
const fetch = fetchNvasLocations( writer );
const locationJob: Job = scheduleJob("*/15 * * * * *", fetch  );

const hitsWriter = esWriter( esClient, vehicleLocationsIndex );
const alert = fetchGeoFenceHitsAndClearIndex( esClient, vehicleHitsIndex, vehicleLocationsIndex, hitsWriter );
const alertJob: Job = scheduleJob("*/5 * * * * *", alert  );
