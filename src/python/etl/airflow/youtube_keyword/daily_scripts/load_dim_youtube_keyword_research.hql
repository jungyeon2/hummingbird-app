use ihr_stg;

SET mapreduce.job.queuename=production;
SET hive.auto.convert.join=true;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.optimize.sort.dynamic.partition=false;

CREATE TABLE IF NOT EXISTS ihr_dwh.dim_youtube_keyword_research (
    keyword string
) partitioned by (
    year string,
    month string,
    day string,
    hour string
);


INSERT OVERWRITE TABLE ihr_dwh.dim_youtube_keyword_research PARTITION(year='{0}', month='{1}', day='{2}', hour='{3}')
SELECT
      TRANSLATE(keyword, '"', '')
FROM
    ihr_stg.dim_youtube_keyword_research;
