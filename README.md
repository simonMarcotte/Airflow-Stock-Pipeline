# Airflow Stock Data Pipeline

This project uses Apache Airflow to automate historical stock anaylsis for a particular stock over the last 20 years. 
The DAG is run daily, grabbing historical stock data (currently for GOOGL), and stores it in BigQuery, which then triggers a calculation of the maximum stock profit during that time.

## Prerequisites:

To run this locally, first download docker and fetch the `docker-compose.yaml` file from Apache Airflow.

Then download Astro CLI with `curl -sSL install.astronomer.io | sudo bash -s`, then run `astro dev init`.

Next, run `astro dev start` to start up the docker containers and airflow project.

Then, you can create DAGs in the `dags/` folder. If you are using GCP, you must mount a docker volume to the containers with Astro CLI to get access to your gcloud credentials. Follow the AstroCLI website for instructions.