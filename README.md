# run-psql-template

## specification

1. load psql database connection settings from json file `connection.json`
2. connect to psql database
3. using command line arguments for file paths for
  - sql template `template.sql`
  - template parameters file csv format `scenarios.csv`
4. stream csv file line by line
  - render raw sql from sql template using specific params
  - execute raw sql
  - (optional) log result to log file