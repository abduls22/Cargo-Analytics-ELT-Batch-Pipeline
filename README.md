# Cargo Logistics & Fleet Command Center

## An End-to-End Analytics Pipeline for Near-Real-Time Global Supply Chain Visibility

### Batch ELT Architecture: Fivetran | Airflow | dbt | BigQuery | Power BI

#### This project implements a near-real-time (15-minute sync) batch ELT data pipeline that transforms raw aircraft state vectors from the OpenSky Network API into key logistics insights for global cargo carriers like FedEx, UPS, and DHL.

#### By utilizing micro-batch processing rather than a traditional streaming architecture, the system provides high-integrity snapshots that allow cargo logistics coordinators to monitor fleet performance, anticipate hub congestion, and mitigate delays through data-driven operational adjustments.

#### Power BI Dashboard:
<img width="1340" height="755" alt="dashboard" src="https://github.com/user-attachments/assets/45e060fc-df10-42cd-928a-d15c1c74fc26" />


### Pipeline Overview

- **Ingestion:** Developed a custom Fivetran Connector using the Fivetran Connector SDK (Python), as no native OpenSky connector exists. This script manages state-based incremental data retrieval and schema mapping. To optimize downstream processing and minimize storage costs, the connector performs source-side filtering to capture only specific cargo carriers:

  * FDX: FedEx Express

  * UPS: United Parcel Service

  * DHL: DHL Aviation

  * GTI: Atlas Air

  * PAC: Polar Air Cargo

  * ABX: ABX Air

  * ATI: Air Transport International

- **Storage:** Utilizes Google Cloud's BigLake architecture to decouple storage from compute. Raw flight records are ingested into Google Cloud Storage (GCS) as Parquet files and mapped as external tables in BigQuery. This maintains a cost-effective hybrid storage layer while enabling high-performance SQL analysis directly on the raw files without data duplication.

- **Orchestration:** Airflow running in Docker serves as the primary scheduler. It coordinates the end-to-end workflow, triggering the Fivetran sync followed by dbt transformations every 15 minutes to maintain low-latency updates.

- **Transformation:** dbt manages the "Raw to Silver to Gold" data modeling lifecycle. It handles incremental logic, executes geospatial math, and derives high-value logistical metrics for global cargo operations.

- **Visualization:** Power BI provides an executive-level Command Center view. The dashboard uses the `is_latest_sync` flag to show a precise 1:1 snapshot of the most recent data batch.


### Data Modeling & Transformation

1. **Staging Layer** (`stg_cargo_flights`)

   - **Materialization:** Incremental load based on `time_position` to handle high-frequency positional pings efficiently.

   - **Cleaning:** Standardizes callsigns and extracts the 3-digit carrier code.

   - **Filtering:** Implements data quality guards to remove records with null `time_position` values.

   - **Velocity Normalization:** Converts velocity from m/s to km/h ($v_{km/h} = v_{m/s} \times 3.6$).

   - **Geospatial Point Generation:** Converts raw Longitude/Latitude into `ST_GEOGPOINT` objects.

2. **Fact Layer** (`fct_cargo_logistics`)

   - **Flight Phase Logic:** Categorizes aircraft into phases such as Cruising, Climbing, Descending, or Terminal Operations based on altitude and vertical rate thresholds.

   - **Hub Proximity:** Uses `ST_DISTANCE` to calculate the distance (in km) to four of the major global cargo hubs: Memphis (FDX), Louisville (UPS), Cincinnati (DHL), and Anchorage (Gateway).

   - **Convergence Categorization:** Buckets flights into 'In Transit' (>=250km), 'Near Hub' (50-250km), or 'Arrival/Departure' (<50km).

   - **Fleet Analytics:** Employs Window Functions (`AVG(speed_kmh) OVER PARTITION BY carrier_code`) to calculate `average_fleet_speed` and `fleet_speed_deviation`, quantifying real-time flight performance against fleet averages to surface anomalies like headwinds or holding pattern delays.

   - **Sync Integrity:** The `is_latest_sync` column uses `MAX(processed_at) OVER ()` to identify the most recent batch regardless of transponder time drift.


### Dashboard Metrics

The Power BI dashboard serves as a Logistics Command Center with the following visual components:

- **Data Last Processed (UTC):** A card showing the exact timestamp of the last successful dbt run in UTC time.

  * _Coordinator Insight:_ Used to verify data "freshness" before making time-sensitive routing or staffing decisions.

- **Total Flights Being Tracked:** A real-time count of active cargo flight vectors in the current sync.

  * _Coordinator Insight:_ Helps gauge total network volume and identify if system filters or API outages are affecting visibility.

- **Avg. Flight Velocity (km/h):** A gauge visual tracking the current average speed of all flights.

  * _Coordinator Insight:_ Provides a high-level "health check" of global weather conditions; a sudden drop in average velocity may indicate widespread headwinds or air traffic control restrictions.

- **Logistics Hub Convergence:** A stacked bar chart visualizing aircraft distance tiers (In Transit, Near Hub, etc.) grouped by their nearest hub.

  * _Coordinator Insight:_ Crucial for hub managers to anticipate "the push", the concentrated period of arrivals, allowing them to scale ground crew and sorting resources accordingly.

- **Fleet Speed Performance Category:** A bar chart segmenting the fleet into "High Speed," "Standard Cruise," and "Reduced Speed" based on velocity thresholds.

  * _Coordinator Insight:_ Allows coordinators to prioritize monitoring for "Reduced Speed" flights which are likely to miss their scheduled sorting windows.

- **Flight Phase:** A horizontal bar chart showing the distribution of aircraft across phases (Cruising, Terminal Ops, etc.).

  * _Coordinator Insight:_ Helps identify congestion in "Terminal Operations" which could signal runway delays or taxing bottlenecks at major ports.

- **Current Aircraft Status:** A donut chart showing the percentage of the fleet currently In Air vs. On Ground.

  * _Coordinator Insight:_ Useful for tracking asset utilization and ensuring that aircraft aren't spending excessive "dead time" on the ground.

- **Avg. Fleet Speed Deviation Per Flight:** A diverging bar chart highlighting specific flights flying significantly faster (Green) or slower (Red) than their carrier's current average. Note: you need to scroll down on the visual to see the rest of the flights, particularly the ones that are slower (red).

  * _Coordinator Insight:_ Directly identifies specific high-risk flights; red bars signal a need to check for mechanical issues, weather detours, or potential fuel-burn concerns.
