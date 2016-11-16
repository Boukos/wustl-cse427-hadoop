-- data = LOAD 'sample2.txt' -- TODO finish the LOAD statement
data = LOAD 'dualcore/ad_data2.txt' USING PigStorage(',') AS (campaign_id:chararray, date:chararray, time:chararray, display_site:chararray, placement:chararray, was_clicked:int, cpc:int, keyword:chararray);
--DESCRIBE data;

data = DISTINCT data;

data = FOREACH data GENERATE TRIM(campaign_id), TRIM(REPLACE(date, '-', '/')), TRIM(time), TRIM(UPPER(keyword)), TRIM(display_site), TRIM(placement), was_clicked, cpc;

--DUMP data;
STORE data INTO 'ad_data2';
