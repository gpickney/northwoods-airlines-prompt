# REPORT 1a: Total number of flights by airline per month
monthly_agg_flights_airline_df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "MONTHLY_AGG_FLIGHTS_AIRLINE") \
  .load()

display(monthly_agg_flights_airline_df)

# REPORT 1b: Total number of flights by airport per month
monthly_agg_flights_airport_df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "MONTHLY_AGG_FLIGHTS_AIRPORT") \
  .load()

display(monthly_agg_flights_airport_df)

# REPORT 2: On time percentage of each airline for the year 2015
on_time_percentage_by_airline_2015 = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "ON_TIME_PERCENTAGE_AIRLINE_2015") \
  .load()

display(on_time_percentage_by_airline_2015)

# REPORT 3: Airlines with the largest number of delays
delays_by_airline_df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "DELAYS_BY_AIRLINE") \
  .load()

display(delays_by_airline_df)

# REPORT 4: Cancellation reasons by airport
cancel_reason_by_airport_df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "CANCELLATIONS_REASONS_BY_AIRPORT") \
  .load()

display(cancel_reason_by_airport_df)

# REPORT 5: Delay reasons by airport
delay_reasons_by_airport_df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "DELAY_REASONS_BY_AIRPORT") \
  .load()

display(delay_reasons_by_airport_df)

# REPORT 6: Airline with the most unique routes (Counting BNA -> DEN as one route and DEN -> BNA as another)
unique_routes_by_airline = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "NUMBER_UNIQUE_ROUTES_AIRLINE") \
  .load()

display(unique_routes_by_airline)