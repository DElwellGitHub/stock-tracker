# Real-time stock tracker
## Overview
This is a real-time streaming application that uses Apache Nifi to call API to gather stock price on Dow Jones Industrial Average companies. Other tools used include Kafka, EMR and Athena. This was my final project for WeCloudData's Data Engineering bootcamp.

### Architecture
![image](https://github.com/DElwellGitHub/stock-tracker/assets/26678347/be4bb137-b3c9-4649-8c20-af8d0dfa578f)

### Part 1: use Nifi to call stock API
![image](https://github.com/DElwellGitHub/stock-tracker/assets/26678347/7a3fe70a-0b2e-4ac7-be36-87317648806c)

### Part 2: Nifi writes to MySQL database in EC2
![image](https://github.com/DElwellGitHub/stock-tracker/assets/26678347/a9b3e85a-349f-4827-af82-bc0d20653955)

### Part 3: Debezium connect brings data from MySQL to Kafka cluster
![image](https://github.com/DElwellGitHub/stock-tracker/assets/26678347/4872df6d-c8ab-41f5-8ebb-8911546a30ad)

### Part 4: Spark job in EMR reads topic in Kafka and writes to S3
![image](https://github.com/DElwellGitHub/stock-tracker/assets/26678347/134a29ec-5843-490b-8515-237f564b4b46)

### Part 5: Athena reads S3 data. Superset reads from Athena
![image](https://github.com/DElwellGitHub/stock-tracker/assets/26678347/77c9b93c-c914-4dd7-8be9-d928585cce98)

![image](https://github.com/DElwellGitHub/stock-tracker/assets/26678347/6bcc6cd1-0a17-436f-bc65-765f127a3eba)
