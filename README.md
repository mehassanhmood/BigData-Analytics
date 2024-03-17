# Data Extraction and Pipeline Project

This repository contains the code and documentation for a data extraction and pipeline project. The project involves extracting data from various resources, transforming it, and loading it into different databases. Below is an overview of the project:

## Overview
- Extracted data from different resources such as API’s, CSVs, JSON.
- Saved the loaded data into different databases:
  - Structured data was stored in SQL databases including SQL Express Server and MySQL.
  - Semi-structured data was stored in MongoDB.
- Built a pipeline to retrieve the data from these sources, perform transformations, such as sentiment analysis on news data using a pretrained model, and load it into a local staging database.
- Utilized PostgreSQL for storing transformed data in the local staging database.
- Used Pyspark to design and implement the pipeline for data processing.
- Shifted the data from the local data warehouse to a cloud-based service, specifically Azure SQL.
- Utilized Power BI for creating visualizations and dashboards to analyze the data.

## Project Structure
The project is structured as follows:
- `models/`: Contains pretrained models used for sentiment analysis.
- `docs/`: Contains project documentation.
- `visualizations/`: Contains visualizations and dashboards created using Power BI.

## Usage
To run the data extraction and pipeline:
1. Install the required dependencies specified in `requirements.txt`.
2. Run the main script to execute different components of the pipeline.
3. Use Power BI to open and explore the visualizations and dashboards in the `visualizations/` directory.



Feel free to contribute by submitting bug fixes, enhancements, or additional features.

## License
This project is licensed under the [MIT License](LICENSE).
