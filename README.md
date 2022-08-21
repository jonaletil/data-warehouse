# Sparkify: ETL Pipeline - Data Warehouse Project

## Project description
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

**Project Goals**: 
- build an ETL pipeline that extracts the data from S3
- stages data in Redshift
- transforms data into a set of dimensional tables for the analytics team 
## Project datasets:
- Song Dataset - this dataset is a subset of real data from the *Million Song Dataset*. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.
- Log Dataset - this dataset consists of log files in JSON format generated by event simulator based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.
## Project files:
- create_tables.py: creates fact and dimension tables for the star schema in Redshift
- etl.py: loads data from S3 into staging tables on Redshift and then process that data into the analytics tables on Redshift
- sql_queries.py: contains all sql queries
- dwh.cfg: contains cluster, role and S3 connection info
- README.md: provides discussion on the project
## Database schema:
For this project I modeled a star schema that is optimized for queries on song play analysis. This includes the following tables:
#### Fact Table
1. songplays - records in log data associated with song plays i.e. records with page *NextSong*
 - songplay_id
 - start_time
 - user_id
 - level
 - song_id
 - artist_id
 - session_id
 - location
 - user_agent
#### Dimension Tables
2. users - users in the app
 - user_id
 - first_name
 - last_name
 - gender
 - level
3. songs - songs in music database
 - song_id
 - title
 - artist_id
 - year
 - duration
4. artists - artists in music database
 - artist_id
 - name
 - location
 - latitude
 - longitude
5. time - timestamps of records in *songplays* broken down into specific units
 - start_time
 - hour
 - day
 - week
 - month
 - year
 - weekday
 
## How to run Python Scripts:

- run *create_tables.py* that will create the database and tables
- run *etl.py* that will load data from S3 into staging tables on Redshift and then process that data into the analytics tables on Redshift