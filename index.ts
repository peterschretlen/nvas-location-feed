
import axios from "axios";
import xml2js from "xml2js";

const url = "http://webservices.nextbus.com/service/publicXMLFeed";
const routeParams = {
    command: "routeConfig",
    a: "ttc",
    r: "65",
};
const xmlParser = (new xml2js.Parser()).parseString;

const vehicleLocationParms = {
    command: "vehicleLocations",
    a: "ttc",
    r: "65",
    // t: "1495374664331",
};

export function fetch(): void {

    axios.get(url, { params : vehicleLocationParms } )
        .then( (response) => {
        xmlParser( response.data,  (e: any, r: any) => {

            const timeMillis = r.body.lastTime[0].$.time;
            console.log( timeMillis, new Date(parseInt( timeMillis, 10 )));
            console.log( Date.now() );
            // console.log( JSON.stringify(r.body.vehicle));

            const locations = r.body.vehicle.map( (l: { $: any; }) => l.$ );

            console.log(locations);

        });
    } );

}

fetch();
