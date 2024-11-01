
# End-to-End Real-Time Analytics Pipeline on GCP

![alt text](<screenshots/Architecture Diagram.png>)


## 1. Project Summary

The "End-to-End Streaming Analytics Pipeline on GCP" is designed to process and analyze real-time streaming data using Google Cloud Platform (GCP) services.This project demonstrates the capability of GCP to handle high-velocity data, generate actionable insights, and visualize metrics on a dashboard. The primary objective is to simulate an environment where real-time data streams are ingested, processed, aggregated, and then visualized for data-driven decision-making.

## 2. Motivation and Goals

The motivation behind this project is to showcase how real-time streaming analytics can address business needs for immediate insights. The project focuses on:
- Leveraging Apache Beam and Dataflow to process real-time data streams.
- Building a scalable and cost-effective pipeline that integrates with other GCP services.
- Presenting real-time analytics on user demographics and engagement to facilitate quick insights for stakeholders.

## 3. Architecture and Components

The project utilizes the following GCP services and tools, which form an interconnected pipeline:

### Google Cloud Pub/Sub
- **Purpose**: Acts as the data ingestion layer, receiving streaming data from APIs or simulated sources.
- **Role in Pipeline**: Pub/Sub serves as a message broker that captures raw data in real time, ensuring reliable delivery to subsequent processing steps.

### Google Cloud Dataflow (with Apache Beam)
- **Purpose**: Executes the core data processing tasks.
- **Role in Pipeline**: A pipeline built with Apache Beam (running on Dataflow) reads data from Pub/Sub, applies transformations and aggregations, and sends processed data to Google BigQuery.
- **Key Tasks**: The pipeline performs aggregations on user demographics, location, and engagement metrics, supporting windowed computations to capture trends over time.

### Airflow (Cloud Composer)
- **Purpose**: Apache Airflow orchestrates and manages the entire data pipeline, ensuring each component runs in a specific sequence. It is crucial in automating the workflow and defining dependencies between tasks, enabling a seamless data flow from ingestion to visualization.
- **Role in Pipeline**:
Triggering Data Ingestion: Airflow initiates data >> ingestion from the streaming API.\
    - Orchestrating Transformations:It triggers Apache Beam for data transformations and aggregation.\
    - Creating Required Service like BigQuery dataset, Google cloud bucket and Pub/Sub Topic
    - Deploying and Canceling DataFlow Job


### Google BigQuery
- **Purpose**: Stores processed data and enables quick querying and analytics.
- **Role in Pipeline**: Acts as the data warehouse where aggregated insights are stored, enabling efficient querying for real-time and historical insights.

### Tableau 
- **Purpose**: Visualization layer for stakeholders.
- **Role in Pipeline**: Displays key insights and metrics from BigQuery in a user-friendly, customizable dashboard format. Metrics include user counts, gender distribution and other insights.

## 4. Data Flow and Processing Steps

![alt text](<screenshots/Workflow .png>)

The following steps outline the data journey from ingestion to visualization:

1. **Data Ingestion**: Pub/Sub ingests data from the streaming source, such as a public API or a simulated user data stream.
2. **Stream Processing**: Dataflow, powered by Apache Beam, processes incoming data from Pub/Sub in real time. Key transformations include:
   - **Filtering**: Excludes irrelevant data fields.
   - **Aggregation**: Groups data by criteria like gender, location, and timestamp to produce insights on user demographics and behavior.
   - **Windowing**: Applies a sliding or tumbling window, creating real-time summaries of activity (e.g., 1-minute intervals).
3. **Data Storage**: Processed data is loaded into BigQuery for efficient querying.
4. **Data Visualization**: Tableau retrieves aggregated data from BigQuery, displaying interactive visuals for stakeholders.

## 5. Insights and Visualizations

The dashboard provides insights critical for understanding user demographics and engagement patterns. Key metrics include:
- **Total Number of Users**: A running count of users. over time.
- **Gender Distribution**: Real-time updates on the ratio of male to female users.
- **Average Age**: A dynamic view of the average age of users, segmented by location and gender.
- **Average Reg Age**: A dynamic view of the average age of users, segmented by location and gender.

![alt text](<screenshots/Tableau Dashboard .png>)

## 6. Code


Key folders include:

- **pub_sub_files**: Manages Pub/Sub message flow.
- **dataflow_files**: Houses Apache Beam scripts for real-time transformations.
- **bigquery_files**: Defines tables and schemas for BigQuery storage.
- **dag_files**: Contains Airflow DAGs for orchestrating pipeline tasks.

## 7. Use Cases and Applications

The project is relevant for industries requiring real-time insights, such as:
- **E-commerce**: Monitoring user activity to adjust inventory and offers based on real-time engagement.
- **Social Media**: Understanding user demographics and engagement patterns for content and ad targeting.
- **Healthcare**: Tracking patient engagement or health metrics in real time to support timely interventions.

## 8. Future Enhancements

Potential improvements to enhance functionality and scalability include:
- **Additional Data Sources**: Integrate multiple streaming sources, such as social media APIs or IoT data.
- **Machine Learning Integration**: Use BigQuery ML or AI Platform to predict trends based on historical data.
- **Alerting System**: Incorporate real-time alerts using Pub/Sub notifications for specific user engagement thresholds.

---