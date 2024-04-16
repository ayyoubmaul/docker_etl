CREATE DATABASE IF NOT EXISTS operational;

SET GLOBAL local_infile=1;

USE operational;

CREATE TABLE IF NOT EXISTS youtube (
`rank` INT(5),
youtuber VARCHAR(50),
subscribers BIGINT,
video_views VARCHAR(50),
category VARCHAR(50),
title VARCHAR(50),
uploads INT(10),
country VARCHAR(50),
abbreviation VARCHAR(50),
channel_type VARCHAR(50),
video_views_rank INT(10),
country_rank INT(11),
channel_type_rank VARCHAR(50),
video_views_for_the_last_30_days INT(10),
lowest_monthly_earnings INT(10),
highest_monthly_earnings INT(10),
lowest_yearly_earnings INT(10),
highest_yearly_earnings INT(10),
subscribers_for_last_30_days INT(10),
created_year INT(10),
created_month VARCHAR(50),
created_date INT(5),
gross_tertiary_education_enrollment_percent FLOAT,
population BIGINT,
unemployment_rate FLOAT,
urban_population BIGINT,
latitude FLOAT,
longitude FLOAT
);

LOAD DATA LOCAL INFILE '/docker-entrypoint-initdb.d/init_data/global_youtube_stat.csv'
INTO TABLE youtube
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
