# ETL

Create a file named `.env` in the root directory and set environment variables for the database connection.
There is an example file called .env_sample, which lists the required variables.

The script `etl.sh` first restore the database dump from the snap project.
Then it generates raw tables from the csv files.
Based on this, clean tables and helper tables are generated.

Set executable permission and then execute the shell script:
```
chmod +x src/etl/etl.sh
./src/etl/etl.sh
```


TODO:
- specify pg restore
- paths
- db role