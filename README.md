# Udacity - Data Warehouse Project in AWS Redshift

## Overview
This project is part of the second project of the **Udacity AWS Data Engineering Nanodegree**. The primary goal is to provide Sparkify with a scalable, efficient, and accessible data storage and querying environment that supports in-depth analysis of user activity on their music streaming platform.


<hr/>

## Project Files Description

### `main.py`
- **Purpose**: Automates the creation and setup of AWS resources including IAM roles and the Redshift cluster.
- **Details**:
  - Creates an IAM role with policies to access AWS services (S3).
  - Launches a Redshift cluster based on specifications in `cluster.cfg`.
  - Manages exceptions and provides status updates on AWS resources.

### `create_tables.py`
- **Purpose**: Prepares the database schema in Redshift, essential for staging and analytical querying.
- **Details**:
  - Establishes a connection to Redshift using details in `dwh.cfg`.
  - Drops existing tables to ensure a fresh setup.
  - Creates structured staging, fact, and dimension tables for ETL processing.

### `etl.py`
- **Purpose**: Executes the ETL pipeline, populating the Redshift data warehouse.
- **Details**:
  - Transfers data from S3 buckets into staging tables on Redshift.
  - Transforms data into a set of dimensional and fact tables, optimizing for query performance.

### `sql_queries.py`
- **Purpose**: Consolidates all SQL queries for easy management and potential adjustments.
- **Details**:
  - Contains SQL statements for table creation and deletion.
  - Stores `COPY` commands for staging data and `INSERT` queries for populating fact and dimension tables.
  - Houses data quality check queries to ensure data consistency.

<hr/>

## Configuration Files

### `dwh.cfg`
- Configures Redshift database credentials, IAM role ARN, and S3 data paths which are essential for database connections and external data access.

### `cluster.cfg`
- Contains AWS credentials and cluster configuration parameters used by `main.py` for setting up the AWS environment.

## Setup and Execution Guide

   1. Fill `cluster.cfg` with AWS credentials and cluster configurations.
   2. Start by executing `main.py` to set up the required AWS infrastructure.
   3. Update `dwh.cfg` with Redshift and IAM role information.
   4. Run `create_tables.py` to prepare your database schema.
   5. Launch `etl.py` to process and load the data into Redshift.
   6. Perform data analysis by running SQL queries directly in Redshift or through the notebook connected to the database.

<hr/>

## Analytical Queries

**Print the total number of events and songs in staging**

```python
cur.execute("SELECT COUNT(*) FROM staging_events;")
results = cur.fetchone()
print(f"Total number of events in staging: {results[0]}")

cur.execute("SELECT COUNT(*) FROM staging_songs;")
results = cur.fetchone()
print(f"Total number of songs in staging: {results[0]}")
```

Result:

Total number of events in staging: 8056
Total number of songs in staging: 385252

<hr/>

**Print the most played song**

```python
cur.execute("""
    SELECT a.name, s.title, COUNT(*) as plays
    FROM songplays p
    JOIN artists a ON p.artist_id = a.artist_id
    JOIN songs s ON s.song_id = p.song_id
    GROUP BY a.name, s.title
    ORDER BY plays DESC LIMIT 1;
""")
results = cur.fetchone()
print(f"Most played song with {results[2]} plays is '{results[1]}' by {results[0]}")
```

Result:
Most played song with 110 plays is 'Greece 2000' by Three Drives

<hr/>

**Print the most active location**
```python
cur.execute("""
    SELECT location, COUNT(*) as plays
    FROM songplays
    GROUP BY location
    ORDER BY plays DESC LIMIT 1;
""")
results = cur.fetchone()
print(f"Most active location with {results[1]} plays is {results[0]}")

```

Result:
Most active location with 722 plays is San Francisco-Oakland-Hayward, CA

<hr/>

**20 top listeners and their locations**
```python
cur.execute("""
    SELECT user_id, location, COUNT(songplay_id) AS number_of_plays
    FROM songplays
    GROUP BY user_id, location
    ORDER BY number_of_plays DESC, location, user_id
    LIMIT 20;
""")
print("Top 20 listeners and their locations:")
for row in cur.fetchall():
    print(row)
```

Result:

Top 20 Listeners and Their Locations

- (49, 'San Francisco-Oakland-Hayward, CA', 720)
- (80, 'Portland-South Portland, ME', 710)
- (97, 'Lansing-East Lansing, MI', 581)
- (15, 'Chicago-Naperville-Elgin, IL-IN-WI', 508)
- (44, 'Waterloo-Cedar Falls, IA', 438)
- (29, 'Atlanta-Sandy Springs-Roswell, GA', 368)
- (24, 'Lake Havasu City-Kingman, AZ', 339)
- (73, 'Tampa-St. Petersburg-Clearwater, FL', 312)
- (36, 'Janesville-Beloit, WI', 275)
- (88, 'Sacramento--Roseville--Arden-Arcade, CA', 275)
- (16, 'Birmingham-Hoover, AL', 248)
- (95, 'Winston-Salem, NC', 225)
- (85, 'Red Bluff, CA', 191)
- (30, 'San Jose-Sunnyvale-Santa Clara, CA', 190)
- (25, 'Marinette, WI-MI', 183)
- (58, 'Augusta-Richmond County, GA-SC', 146)
- (42, 'New York-Newark-Jersey City, NY-NJ-PA', 144)
- (26, 'San Jose-Sunnyvale-Santa Clara, CA', 124)
- (82, 'Atlanta-Sandy Springs-Roswell, GA', 81)
- (72, 'Detroit-Warren-Dearborn, MI', 77)

<hr/>

**50 top songs**
```python
cur.execute("""
    SELECT s.title, a.name AS artist, COUNT(sp.songplay_id) AS number_of_plays
    FROM songplays sp
    JOIN songs s ON sp.song_id = s.song_id
    JOIN artists a ON sp.artist_id = a.artist_id
    GROUP BY s.title, a.name
    ORDER BY number_of_plays DESC, a.name, s.title
    LIMIT 50;
""")
print("Top 50 songs:")
for row in cur.fetchall():
    print(row)

```

Result:

Top 50 Songs

- ('Greece 2000', 'Three Drives', 110)
- ('Stronger', 'Kanye West', 84)
- ('Greece 2000', '3 Drives On A Vinyl', 55)
- ('This Fire', 'Franz Ferdinand', 54)
- ('Yellow', 'Coldplay', 48)
- ('Stronger', "Kanye West / Consequence / Cam'Ron", 42)
- ('Stronger', 'Kanye West / GLC / Consequence', 42)
- ('Stronger', 'Kanye West / Jay-Z', 42)
- ('Stronger', 'Kanye West / Jay-Z / J. Ivy', 42)
- ('Stronger', 'Kanye West / John Legend / Consequence', 42)
- ('Stronger', 'Kanye West / Kid Cudi', 42)
- ('Stronger', 'Kanye West / Lil Wayne', 42)
- ('Stronger', 'Kanye West / Mos Def', 42)
- ('Stronger', 'Kanye West / Mos Def / Al Be Back', 42)
- ('Stronger', 'Kanye West / Mos Def / Freeway / The Boys Choir Of Harlem', 42)
- ('Stronger', 'Kanye West / Nas / Really Doe', 42)
- ('Stronger', 'Kanye West / Paul Wall / GLC', 42)
- ('Stronger', 'Kanye West / Syleena Johnson', 42)
- ('Stronger', 'Kanye West / T-Pain', 42)
- ('Stronger', 'Kanye West / Talib Kweli / Q-Tip / Common / Rhymefest', 42)
- ('Stronger', 'Kanye West / Twista / Keyshia Cole / BJ', 42)
- ('Stronger', 'Mary', 42)
- ('Flashing Lights', 'Kanye West', 40)
- ("Don't Stop The Music", 'Rihanna', 40)
- ("You're The One", 'Dwight Yoakam', 37)
- ("You're The One", 'Dwight Yoakam Duet with Maria McKee', 37)
- ("You're The One", 'Dwight Yoakam with Kelly Willis', 37)
- ('Bring Me To Life', 'Evanescence', 35)
- ("Nothin' On You [feat. Bruno Mars] (Album Version)", 'B.O.B.', 32)
- ("Nothin' On You [feat. Bruno Mars] (Album Version)", 'B.o.B', 32)
- ('Bubble Toes', 'Jack Johnson', 32)
- ('Bubble Toes', 'Jack Johnson / Ben Harper', 32)
- ('Bubble Toes', 'Jack Johnson / G. Love', 32)
- ('Bubble Toes', 'Jack Johnson / Kawika Kahiapo', 32)
- ('Bubble Toes', 'Jack Johnson / Matt Costa', 32)
- ('Bubble Toes', 'Jack Johnson / Matt Costa / Zach Gill / Dan Lebowitz / Steve Adams', 32)
- ('Bubble Toes', 'Jack Johnson / Paula Fuga', 32)
- ('Bubble Toes', 'Jack Johnson and Friends / Matt Costa', 32)
- ('Bubble Toes', 'Waylon Jennings', 32)
- ('Dance_ Dance', 'Fall Out Boy', 30)
- ('Dance_ Dance', 'Fall Out Boy / John Mayer', 30)
- ('My Moon My Man', 'Feist', 30)
- ('Just Dance', 'Lady GaGa', 30)
- ('Use Somebody', 'Kings Of Leon', 28)
- ('This Fire', 'Franz Ferdinand / Jane Birkin', 27)
- ('This Fire', 'Franz Ferdinand Feat. The Grizzl', 27)
- ('Revelry', 'Kings Of Leon', 27)
- ('Somebody To Love', 'Justin Bieber', 26)
- ('Somebody To Love', 'Justin Bieber / Jessica Jarrell', 26)
- ('Somebody To Love', 'Justin Bieber / Usher', 26)


<hr/>

**Songs per duration**
```python
cur.execute("""
    SELECT 
        CASE
            WHEN duration < 60 THEN '0-59 Seconds'
            WHEN duration < 120 THEN '60-119 Seconds'
            WHEN duration < 180 THEN '120-179 Seconds'
            WHEN duration < 240 THEN '180-239 Seconds'
            WHEN duration < 300 THEN '240-299 Seconds'
            ELSE '300+ Seconds'
        END AS duration_range,
        COUNT(*) AS songs
    FROM songs
    GROUP BY 1
    ORDER BY 1;
""")
print("Songs per duration range:")
for row in cur.fetchall():
    print(row)
```

Result:

Songs per duration range:
- ('0-59 Seconds', 5408)
- ('120-179 Seconds', 62094)
- ('180-239 Seconds', 126835)
- ('240-299 Seconds', 92783)
- ('300+ Seconds', 80567)
- ('60-119 Seconds', 17308)
