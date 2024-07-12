# ETL - Airflow 
## Project Overview

This project sets up a scalable and efficient data pipeline for crawling, extracting, transforming, and loading data from a specific website (`https://nhacnheo.com/`). It uses Docker containers for deployment, Apache Airflow for orchestrating the ETL processes, and Firebase for data storage.

## Technologies Used

- **Docker**: Containerization to ensure consistency across various environments.
    - Docker Compose Configuration:
        - `PostgreSQL`: Acts as the backend database for Airflow.
        - Airflow `Webserver`: Provides the user interface for monitoring and managing workflows.
        - Airflow `Scheduler`: Schedules and monitors the DAGs.
- **Apache Airflow**: Workflow management platform to define, schedule, and monitor ETL tasks.
- **Python Libraries**:
  - `pandas`: For data manipulation and storage.
  - `requests`: To handle HTTP requests.
  - `BeautifulSoup`: To parse HTML and extract data.
  - `firebase_admin`: To interact with Firebase services.
- **Firebase**: Used for storing and managing song data.
- **PostgreSQL**: Database for Airflow metadata.
## ETL Process:
1. Data is crawled from the target website using the requests library for making HTTP requests and BeautifulSoup for parsing the HTML content. The extracted data includes song titles, authors, lyrics, and associated metadata.
2. The transformation step further processes the extracted data to include additional details such as PDF links associated with each song.
3. The final step involves loading the transformed data into Firebase. The data is checked for duplicates before being uploaded.

**A DAG (Directed Acyclic Graph) is defined in Airflow to schedule and manage the ETL tasks.**
