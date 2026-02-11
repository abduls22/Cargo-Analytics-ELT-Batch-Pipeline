from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op
from datetime import datetime, timezone
import requests


def schema(configuration: dict):
    return [
        {
            "table": "flight_records",
            "primary_key": [
                "icao24",
                "last_contact",
            ],
            "columns": {
                "icao24": "STRING",
                "callsign": "STRING",
                "origin_country": "STRING",
                "time_position": "UTC_DATETIME",
                "last_contact": "UTC_DATETIME",
                "longitude": "DOUBLE",
                "latitude": "DOUBLE",
                "baro_altitude": "DOUBLE",
                "on_ground": "BOOLEAN",
                "velocity": "DOUBLE",
                "vertical_rate": "DOUBLE",
            },
        },
    ]


def update(configuration: dict, state: dict):
    log.info("Fetching OpenSky API data...")
    opensky_api_url = "https://opensky-network.org/api/states/all"
    try:
        response = requests.get(opensky_api_url, timeout=60)
        response.raise_for_status()
        flight_data = response.json()

        cargo_identifiers = ['FDX', 'UPS', 'DHL', 'GTI', 'PAC', 'ABX', 'ATI']
        record_count = 0

        for flight_record in flight_data["states"]:
            raw_callsign = flight_record[1] if flight_record[1] else ""
            carrier_code = raw_callsign[:3]
            if carrier_code in cargo_identifiers:
                yield op.upsert(
                    table="flight_records",
                    data={
                        "icao24": flight_record[0],
                        "callsign": raw_callsign,
                        "origin_country": flight_record[2],
                        "time_position": datetime.fromtimestamp(flight_record[3], tz=timezone.utc).isoformat() if flight_record[3] else None,
                        "last_contact": datetime.fromtimestamp(flight_record[4], tz=timezone.utc).isoformat() if flight_record[4] else None,
                        "longitude": flight_record[5],
                        "latitude": flight_record[6],
                        "baro_altitude": flight_record[7],
                        "on_ground": flight_record[8],
                        "velocity": flight_record[9],
                        "vertical_rate": flight_record[11],
                    },
                )
                record_count += 1

        log.info(f"Data synced successfully. Ingested {record_count} OpenSky flight records.")

        yield op.checkpoint(
            state={"last_synced_at": datetime.now(timezone.utc).isoformat()}
        )


    except Exception as e:
        log.error(f"Critical error during sync: {e}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
