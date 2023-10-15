
# STEDI Analytics Project
Enrique Villarreal


## Project Description

STEDI has developed a step trainer that helps users with their exercise routine. Additionally, it utilized sensors to collect data that will be used to train a machine learning model. It also has a companion mobile app that interacts with the device itself and collects customers' data.

For this project, we will be developing an ETL solution using an AWS-based stack (S3, Glue, Athena) and Spark to clean up and process the data. This will be done in a three-step architecture, as so:


* **Landing Zone**: Will contain the raw data extracted from the customer app and the STEDI device, without any processing.
* **Trusted Zone**: Will contain cleaned and sanitized versions of data from the landing zone. Specifically, we're only extracting data from customers who have agreed to share their data for research purposes.
* **Curated Zone**: Will contain processed datasets suitable for end-user consumption. In this case, the dataset to be used for training the machine learning model, and a final version of the customers table.


