# ETL-Based-Real-Time-Air-Quality-Monitoring-System


## Project Description

This project implements a real-time ETL (Extract, Transform, Load) pipeline to monitor air quality data. It collects live data streams, stores them efficiently in HDFS, processes the data using Apache Spark, and visualizes key metrics and trends through an interactive Streamlit dashboard. This system enables timely insights into air pollution and environmental conditions, aiding in public health and environmental decision-making.

## Project Summary

* Real-time ingestion of air quality data using Kafka producers and consumers.
* Data persistence by sinking Kafka streams into Hadoop Distributed File System (HDFS).
* Data transformation, aggregation, and analysis with Apache Spark.
* Interactive data visualization and monitoring dashboard built using Streamlit.
* Modular design to ensure scalability, maintainability, and extensibility.

## Project Flow

1. **Data Ingestion:** Continuous ingestion of air quality data streams through Kafka producer (`producer.py`) and consumption via Kafka consumer (`consumer.py`).
2. **Data Storage:** Kafka messages are saved to HDFS using the `kafka_to_hdfs.py` script, ensuring reliable and distributed storage.
3. **Data Processing:** Spark processes the stored data (`spark_processor.py`), performing transformations and aggregations to extract meaningful insights.
4. **Data Visualization:** The Streamlit app (`dashboard.py`) reads processed data and renders real-time interactive dashboards for user-friendly monitoring and analysis.

## Folder Structure

```
air_quality_etl/
├── data_ingestion/
│   ├── producer.py            # Kafka producer for data ingestion
│   └── consumer.py            # Kafka consumer for stream consumption
├── hdfs_sink/
│   └── kafka_to_hdfs.py       # Script to sink Kafka streams into HDFS
├── spark_processing/
│   └── spark_processor.py     # Apache Spark processing script for ETL transformations
├── streamlit_ui/
│   └── dashboard.py           # Streamlit-based dashboard UI for visualization
```

## Technologies Used

* **Apache Kafka:** For real-time data streaming and messaging.
* **Hadoop HDFS:** Distributed file system for scalable data storage.
* **Apache Spark:** Big data processing framework for ETL transformations.
* **Streamlit:** Python framework for building interactive web dashboards.
* **Python:** Core programming language used across all components.

## Future Improvements

* **Automated Data Quality Checks:** Integrate validation and anomaly detection during data ingestion and processing.
* **Alert System:** Implement threshold-based alerts and notifications for hazardous air quality conditions.
* **Advanced Analytics:** Incorporate machine learning models for predicting air quality trends.
* **Scalability Enhancements:** Use Kubernetes or other container orchestration for better deployment and scaling.
* **User Authentication:** Secure the dashboard with user login and role-based access controls.
* **Extended Data Sources:** Integrate additional environmental sensors or APIs for a broader data spectrum.
