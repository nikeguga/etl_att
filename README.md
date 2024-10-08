# ETL Booking Pipeline

This project defines an ETL (Extract, Transform, Load) process using Apache Airflow to consolidate data from multiple CSV files into a PostgreSQL database. The ETL process involves loading booking, client, and hotel data, transforming it, and storing it in a PostgreSQL database for further analysis.

## Prerequisites

- Docker
- Docker Compose
- Apache Airflow
- PostgreSQL

## Setup and Installation

1. **Clone the Repository**

   Clone the project repository to your local machine.

   ```sh
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **CSV Files**

   Ensure that the following CSV files are placed in the `/opt/airflow/csv_data/` directory within your Docker environment:

   - `booking.csv`
   - `client.csv`
   - `hotel.csv`

3. **Docker Compose Setup**

   The project includes a `docker-compose.yaml` file to set up Apache Airflow and PostgreSQL containers.

   ```sh
   docker-compose up -d
   ```

   This command starts the required services: PostgreSQL and Airflow (webserver, scheduler).

4. **Access Airflow UI**

   Once the services are up and running, you can access the Airflow UI by navigating to `http://localhost:8080`. Use the default credentials to log in.

## DAG Overview

The DAG (`etl_booking_pipeline`) defines a daily ETL process with the following tasks:

1. **Load Booking Data**: Reads booking data from `booking.csv` and stores it in XCom.
2. **Load Client Data**: Reads client data from `client.csv` and stores it in XCom.
3. **Load Hotel Data**: Reads hotel data from `hotel.csv` and stores it in XCom.
4. **Transform Data**: Merges the booking, client, and hotel data, formats the booking date, removes rows with missing booking costs or currency, and converts all costs to EUR.
5. **Load to DB**: Loads the transformed data into a PostgreSQL database in a table named `merged_booking_data`.

## ETL DAG Definition

The DAG is defined in the file `etl_booking_dag.py`. 

## Querying the Database

Once the ETL pipeline completes successfully, the transformed data is available in the PostgreSQL database under the table `merged_booking_data`. You can connect to the PostgreSQL container using the following command:

```sh
docker exec -it <postgres-container-name> psql -U airflow -d airflow
```

To query the merged data, use:

```sql
SELECT * FROM merged_booking_data;
```

## Example Output

The following is an example of the merged and transformed data stored in the database:

```
 client_id | booking_date | room_type       | hotel_id | booking_cost | currency | age | name_x  | type     | name_y           | address  
-----------+--------------+-----------------+----------+--------------+----------+-----+---------+----------+------------------+----------
 4         | 2016-11-02   | first_class_2_bed| 6        | 3611         | EUR      | 43  | Bianca  | VIP      | The New View     | address6
 2         | 2017-07-13   | balcony_2_bed    | 2        | 2325         | EUR      | 38  | Ben     | standard | Dream Connect    | address2
 ...
```

## Conclusion

This project demonstrates how to implement a simple ETL pipeline using Apache Airflow and PostgreSQL to transform and consolidate data from multiple sources. The resulting data is cleaned, standardized, and stored in a database for easy querying and analysis.

