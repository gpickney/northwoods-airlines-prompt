# Northwoods Airlines Prompt

### General Approach
1. Copy CSV data from Google Drive to Databricks DBFS
2. Read in data to dataframes in Databricks via PySpark CSV reader
3. Persist tables in Snowflake using Databricks' Snowflake connector
4. Create views in Snowflake via worksheets
5. Read views in Snowflake from Databricks to display graphs of data in notebook

### Contents of this repo
- Notebook code from Databricks
- Worksheet SQL from Snowflake
- Snowflake connection configuration