from pyspark.sql.functions import col
from pyspark.sql.types import StringType

url = "https://cka39758.us-east-1.snowflakecomputing.com"
user = "gpickney"
# snowflake connection options
options = {
  "sfUrl": url,
  "sfUser": user,
  "sfPassword": dbutils.fs.head("dbfs:/tmp/sfPassword"),
  "sfDatabase": "USER_GPICKNEY",
  "sfSchema": "PUBLIC",
  "sfWarehouse": "INTERVIEW_WH"
}

# Set up widgets for Airline, Airport, and Month
WIDGETS_DICT = {
  'AIRLINES': ["*"],
  'AIRPORTS': ["*"],
  'MONTHS': ["*"]
}

dbutils.widgets.removeAll()

airlines = spark.read.format("snowflake").options(**options).option("dbtable", "AIRLINES").load()
WIDGETS_DICT.get('AIRLINES').extend(sorted([str(row.AIRLINE) for row in airlines.select('AIRLINE').collect()]))
dbutils.widgets.multiselect("AIRLINES", '*', WIDGETS_DICT.get('AIRLINES'))

airports = spark.read.format("snowflake").options(**options).option("dbtable", "AIRPORTS").load()
WIDGETS_DICT.get('AIRPORTS').extend(sorted([str(row.AIRPORT) for row in airports.select('AIRPORT').collect()]))
dbutils.widgets.multiselect("AIRPORTS", '*', WIDGETS_DICT.get('AIRPORTS'))

WIDGETS_DICT.get("MONTHS").extend([str(x) for x in range(1, 13)])
dbutils.widgets.multiselect("MONTHS", '*', WIDGETS_DICT.get("MONTHS"))

def get_widget_values(widget_name):
  value_list = dbutils.widgets.get(widget_name)
  if value_list:
    if value_list[0] == "*":
      return WIDGETS_DICT.get(widget_name.upper())
    else:
      return [value for value in value_list.split(',') if value != '*']

#REPORT 1a: Total number of flights by airline per month
monthly_agg_flights_airline_df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "MONTHLY_AGG_FLIGHTS_AIRLINE") \
  .load() \
  .persist()

display(
  monthly_agg_flights_airline_df
  .where(col('AIRLINE').isin(get_widget_values('AIRLINES')) &
         col('MONTH').cast(StringType()).isin(get_widget_values('MONTHS')))
)
#REPORT 1b: Total number of flights by airport per month
monthly_agg_flights_airport_df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "MONTHLY_AGG_FLIGHTS_AIRPORT") \
  .load() \
  .persist()

display(
  monthly_agg_flights_airport_df
  .where(col('AIRPORT').isin(get_widget_values('AIRPORTS')) &
         col('MONTH').cast(StringType()).isin(get_widget_values('MONTHS')))
)

#REPORT 2: On time percentage of each airline for the year 2015
on_time_percentage_by_airline_2015 = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "ON_TIME_PERCENTAGE_AIRLINE_2015") \
  .load() \
  .persist()

display(
  on_time_percentage_by_airline_2015
  .where(col('AIRLINE').isin(get_widget_values('AIRLINES')))
)

#REPORT 3: Airlines with the largest number of delays
delays_by_airline_df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "DELAYS_BY_AIRLINE") \
  .load() \
  .persist()

display(
  delays_by_airline_df
  .where(col('AIRLINE').isin(get_widget_values('AIRLINES')))
)

#REPORT 4: Cancellation reasons by airport
cancel_reason_by_airport_df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "CANCELLATIONS_REASONS_BY_AIRPORT") \
  .load() \
  .persist()

display(
  cancel_reason_by_airport_df
  .where(col('AIRPORT').isin(get_widget_values('AIRPORTS')))
)

#REPORT 5: Delay reasons by airport
delay_reasons_by_airport_df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "DELAY_REASONS_BY_AIRPORT") \
  .load() \
  .persist()

display(
  delay_reasons_by_airport_df
  .where(col('AIRPORT').isin(get_widget_values('AIRPORTS')))
)

#REPORT 6: Airline with the most unique routes (Counting BNA -> DEN as one route and DEN -> BNA as another)
unique_routes_by_airline = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "NUMBER_UNIQUE_ROUTES_AIRLINE") \
  .load() \
  .persist()

display(
  unique_routes_by_airline
  .where(col('AIRLINE').isin(get_widget_values('AIRLINES')))
)
