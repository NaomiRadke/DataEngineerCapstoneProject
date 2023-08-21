# Data Engineering Project

This project is based on the requirements of Udacity's Data Engineering Degree Capstone Project.

## Goal

What's the goal? What queries will you want to run? 

In order to allow the U.S Customs & Border Protection Department to more efficiently analyze their immigration data, we are building a data warehouse infrastructure that allows.

?? What's the end use case? (e.g., analytics table, app back-end, source-of-truth database, etc.)

To answer questions such as:
- What is the trend in the number of immigrants and their country of origin over the year?
- are their seasonal trends in the number, country of origin, and city/ state of destination of immigrants?
- is there a correlation between the country of origin and the destination in the US?

## Data
To this end I am using the following data sources:
Data sources:
- US I94 Immigration data
- US City Demographic Data




How would Spark or Airflow be incorporated? 
Why did you choose the model you chose?

## Tools, Technologies and Data Model
Clearly state the rationale for the choice of tools and technologies for the project.
- Spark for big data analytics for distributing load on multiple nodes (a cluster)
- Redshift as data warehouse that holds the tables of data, arranged according to a star schema. Redshift is optimized for OLAP (Online Analytical Processing) workloads, and it can handle complex queries and aggregations on large datasets.
- Airflow to schedule the jobs
- S3 for storing the data that is sent to the dimension model (Redshift)

## Process steps

### Exploring the data
I set up a Jupyter Notebook that allows loading, exploring and cleaning the data.

- exploring and wrangling: https://learn.udacity.com/nanodegrees/nd027/parts/cd0030/lessons/ls1966/concepts/c8f47373-8dab-4bab-bbf1-f8e728c5aba7
- data quality (e.g. missing values, duplicate data etc.)
- e.g. with sql python plugin? (https://learn.udacity.com/nanodegrees/nd027/parts/cd0055/lessons/ls1964/concepts/c535b758-da20-4713-aac3-5d3e021eabea)
- clean the data

### Define the data model
- map out the conceptual data model and explain why i chose it
- list necessary steps to pipeline the data into the data model
- OLAP / Star schema (fact and dimension tables)

### Run ETL to model the data
- Extract data from source (e.g. postgres / s3)
- Transform data (join tables together, change types, add new columns)
- Load it into dimensional model (star schema --> fact and dimension tables)
- create data pipelines and data model
- include data dictionary
- run data quality checks


## Recommedations and assumptions
Propose how often the data should be updated and why.
Include a description of how you would approach the problem differently under the following scenarios:
If the data was increased by 100x.
If the pipelines were run on a daily basis by 7am.
If the database needed to be accessed by 100+ people.