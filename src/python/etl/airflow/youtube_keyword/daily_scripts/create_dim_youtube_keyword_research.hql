set mapreduce.job.queuename=production;

USE ihr_stg;

CREATE EXTERNAL TABLE IF NOT EXISTS dim_youtube_keyword_research(
    keyword     string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES  TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/data/logs/dim_youtube_keyword_research'
TBLPROPERTIES('serialization.null.format'='');
