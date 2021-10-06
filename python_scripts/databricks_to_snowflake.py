EXTRACT_LOCATION = "/tmp/northwoods"

dbutils.fs.cp("/FileStore/tables/flight_data_20210927T032903Z_001.zip", "file:/tmp/flightdata.zip")
# File location and type
file_location = "/tmp/flightdata.zip"

import zipfile
with zipfile.ZipFile(file_location, 'r') as zip:
  zip.extractall(EXTRACT_LOCATION)

# CSV options
infer_schema = "true"

airline_path = "file:{}/flight-data/airlines.csv".format(EXTRACT_LOCATION)
airport_path = "file:{}/flight-data/airports.csv".format(EXTRACT_LOCATION)
flights_path = "file:{}/flight-data/flights/".format(EXTRACT_LOCATION)

# The applied options are for CSV files. For other file types, these will be ignored.
airlines = spark.read.format("csv").option("header", True).option("inferSchema", infer_schema).load(airline_path)
airports = spark.read.format("csv").option("header", True).option("inferSchema", infer_schema).load(airport_path)
flights = spark.read.format("csv").option("header", True).option("inferSchema", infer_schema).load(flights_path)


airlines.write.format("parquet").saveAsTable("airlines")
airports.write.format("parquet").saveAsTable("airports")
flights.write.format("parquet").saveAsTable("flights")

url = "https://cka39758.us-east-1.snowflakecomputing.com"
user = "gpickney"

# snowflake connection options
options = {
  "sfUrl": url,
  "sfUser": user,
  "sfPassword": get_password(),
  "sfDatabase": "USER_GPICKNEY",
  "sfSchema": "PUBLIC",
  "sfWarehouse": "INTERVIEW_WH"
}

with open("/tmp/pswd", 'w') as tmp:
  tmp.write("Test_pswd1")


def get_password():
    with open("/tmp/pswd", 'r') as tmp:
        return tmp.read()

df = spark.read.table("airlines")
df.write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "AIRLINES") \
  .save()

df = spark.read.table("airports")
df.write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "AIRPORTS") \
  .save()

df = spark.read.table("flights")
df.write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "FLIGHTS") \
  .save()




