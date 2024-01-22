## Module 1 Homework

## Docker & SQL

In this homework we'll prepare the environment 
and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command:

```docker build --help```

Do the same for "docker run".

Which tag has the following text? - *Automatically remove the container when it exits* 

- `--delete`
- `--rc`
- `--rmc`
- `--rm`

MY ANSWER: `--rm`

## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use ```pip list``` ). 

What is version of the package *wheel* ?

- 0.42.0
- 1.0.0
- 23.0.1
- 58.1.0

*MY ANSWER*: Used command `docker run -it --entrypoint=bash python:3.9` and then entered `pip list` which shows the *wheel* package version is `0.42.0`.

# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from September 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

*MY PROCESS*: URL for parquet data is this ``https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-09.parquet``, so I set URL variable to this and then ran docker image with ingestion script in it.

## Question 3. Count records 

How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 15767
- 15612
- 15859
- 89009

*MY ANSWER*: 15612 rows provided after using this query
```
SELECT
  lpep_pickup_datetime as start,
  lpep_dropoff_datetime as end
FROM
  green_taxi_data t
WHERE 
 CAST(lpep_pickup_datetime as DATE) = '2019-09-18' AND
 CAST(lpep_dropoff_datetime as DATE) = '2019-09-18';
 ```

## Question 4. Largest trip for each day

Which was the pick up day with the largest trip distance
Use the pick up time for your calculations.

- 2019-09-18
- 2019-09-16
- 2019-09-26
- 2019-09-21

MY ANSWER: `2019-09-26` was found after using this query
```
SELECT
  CAST(lpep_pickup_datetime as DATE) as "day",
  COUNT(1) as "count",
  MAX(trip_distance) as "distance"
FROM
  green_taxi_data t
GROUP BY
  CAST(lpep_pickup_datetime as DATE)
ORDER BY "distance" DESC;
```

## Question 5. Three biggest pick up Boroughs

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
 
- "Brooklyn" "Manhattan" "Queens"
- "Bronx" "Brooklyn" "Manhattan"
- "Bronx" "Manhattan" "Queens" 
- "Brooklyn" "Queens" "Staten Island"

MY ANSWER: "Brooklyn" "Manhattan" "Queens" had the largest sums of total_amounts > 50,000 and that was found with this query
```
SELECT
  SUM(total_amount) as "amount", 
  zpu."Borough"
FROM
  -- Added "LEFT" to the previous statement.
  green_taxi_data t LEFT JOIN zones zpu
  	ON t."PULocationID" = zpu."LocationID"
WHERE
 CAST(lpep_pickup_datetime as DATE) = '2019-09-18'
GROUP BY
 zpu."Borough"
ORDER BY "amount" DESC;
```

## Question 6. Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Central Park
- Jamaica
- JFK Airport
- Long Island City/Queens Plaza

MY ANSWER: JFK Airport had the largest tip and this was found using this query
```
SELECT
  tip_amount, 
  CONCAT(zpu."Borough", ' / ', zpu."Zone") as "pickup_loc",
  CONCAT(zdo."Borough", ' / ', zdo."Zone") as "dropoff_loc"
FROM
  -- Added "LEFT" to the previous statement.
  green_taxi_data t LEFT JOIN zones zpu
  	ON t."PULocationID" = zpu."LocationID"
 LEFT JOIN zones zdo
  	ON t."DOLocationID" = zdo."LocationID"
WHERE
 zpu."Zone" = 'Astoria'
ORDER BY tip_amount DESC;
```

## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.

## Question 7. Creating Resources

After updating the main.tf and variable.tf files run:

```
terraform apply
```

Paste the output of this command into the homework submission form.

*MY ANSWER*: 
google_bigquery_dataset.demo_dataset: Creating...
google_storage_bucket.demo-bucket: Creating...
google_bigquery_dataset.demo_dataset: Creation complete after 1s [id=projects/quantum-age-411318/datasets/demo_dataset]
google_storage_bucket.demo-bucket: Creation complete after 1s [id=terraform-demo-quantum-age-411318-bucket]

## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw01
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 29 January, 23:00 CET