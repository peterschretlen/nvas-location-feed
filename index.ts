
import axios from "axios";
import log4js from "log4js";
import { Job, scheduleJob } from "node-schedule";
import xml2js from "xml2js";

const url: string = "http://webservices.nextbus.com/service/publicXMLFeed";
const xmlParser = (new xml2js.Parser()).parseString;

log4js.addLayout("json", (config) => {
    return (logEvent) => JSON.stringify(logEvent);
  });

log4js.configure({
    appenders: {
      out: { type: "stdout", layout: { type: "json" } },
    },
    categories: {
      default: { appenders: ["out"], level: "info" },
    },
  });

const logger: log4js.Logger = log4js.getLogger();

export interface IVehicleLocationParameters {
    command: string;
    a: string;
    r: string;
    t?: number;
}

export function fetch(): void {

    const vlParams: IVehicleLocationParameters = {
        command: "vehicleLocations",
        a: "ttc",
        r: "65",
        // t: "1495374664331",
    };

    axios.get(url, { params : vlParams } )
        .then( (response) => {
        xmlParser( response.data,  (e: any, r: any) => {

            const timeMillis = r.body.lastTime[0].$.time;
            const timestamp = new Date(parseInt( timeMillis, 10 ));
            // console.log( timeMillis, new Date(parseInt( timeMillis, 10 )));
            // console.log( Date.now() );
            // console.log( JSON.stringify(r.body.vehicle));

            r.body.vehicle.forEach( (l: { $: any; }) => {

                const location = {
                    lastTimeMillis: timeMillis,
                    lastTimeTimestamp: timestamp,
                };

                Object.assign( location, l.$);
                logger.info(location);
            });

            //console.log(locations);

        });
    } );

}

const job: Job = scheduleJob("*/30 * * * * *", fetch);
