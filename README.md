# 🛫 SkyEco: Real-Time Aviation Emissions Intelligence

**SkyEco** is a high-performance data engineering pipeline that monitors global aviation emissions in real-time. By fusing live flight telemetry with meteorological data, the system calculates dynamic carbon intensity metrics via a modern ELT stack.

![Project Status](https://img.shields.io/badge/Status-Live-success)
![dbt](https://img.shields.io/badge/dbt-Enabled-orange)
![Kafka](https://img.shields.io/badge/Kafka-Streaming-black)
![MotherDuck](https://img.shields.io/badge/MotherDuck-Cloud--DuckDB-000000?logo=duckdb)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-blue)

---

## 🏗️ System Architecture

The project follows a decoupled **ELT (Extract, Load, Transform)** pattern:

1.  **Extract & Enrich:** A Python producer fetches OpenSky state vectors and enriches them with OpenWeatherMap API data (Temperature/Wind).
2.  **Stream:** Enriched payloads are published to **Apache Kafka** to decouple ingestion from storage.
3.  **Load:** A Python bridge consumes Kafka events and performs a bulk insert into **MotherDuck** (`raw_flights` table).
4.  **Transform (dbt):** dbt models clean the raw data, handle deduplication, and calculate CO2 metrics to produce the final analytical layer.
5.  **Visualize:** A Streamlit dashboard queries the dbt-transformed models for real-time intelligence.

---
## 🛠️ Tech Stack

-   **Data Storage:** [MotherDuck](https://motherduck.com/) (Serverless Cloud DuckDB)
-   **Stream Processing:** [Apache Kafka](https://kafka.apache.org/) (Confluent Cloud)
-   **Dashboard:** [Streamlit](https://streamlit.io/) & [Plotly](https://plotly.com/)
-   **Language:** Python 3.12
-   **Data Sources:** OpenSky Network (ADS-B), OpenWeatherMap API

---

## 📊 Key Features

- **Real-time Map:** Spatial visualization of flight assets with color-coded CO2 intensity markers.
- **Hourly Granularity:** Toggle between broad historical ranges and specific hourly windows.
- **Weather Analysis:** Tracks how "Wind Load" and "Ambient Temp" impact fuel drag and efficiency.
- **SQL Efficiency:** Powered by **DuckDB** for sub-second analytical queries on time-series data.

---

## ⚖️ License & Attribution

- **Code:** Licensed under the [MIT License](https://opensource.org/licenses/MIT).
- **Aviation Data:** Provided by [OpenSky Network](https://opensky-network.org/) under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).
- **Weather Data:** Provided by [OpenWeatherMap](https://openweathermap.org/).

---

## 👤 Author

**Sachin Yadav**
- **LinkedIn:** [sachin-yadav](https://www.linkedin.com/in/linearsachin/)
- **GitHub:** [@linearsachin](https://github.com/linearsachin)