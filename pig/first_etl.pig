-- data = LOAD 'sample1.txt' -- TODO finish the LOAD
data = LOAD 'dualcore/ad_data1.txt' AS (keyword:chararray, campaign_id:chararray, date:chararray, time:chararray, display_site:chararray, was_clicked:int, cpc:int, country:chararray, placement:chararray);

data = FILTER data BY country == 'USA';

--data = FOREACH data GENERATE campaign_id, date, time, keyword, display_site, placement, was_clicked, cpc;

data = FOREACH data GENERATE TRIM(campaign_id), TRIM(date), TRIM(time), TRIM(UPPER(keyword)), TRIM(display_site), TRIM(placement), was_clicked, cpc;

--DUMP data;
STORE data INTO 'ad_data1';
