-- Load only the ad_data1 and ad_data2 directories
data = LOAD 'ad_data/' AS (campaign_id:chararray,
             date:chararray, time:chararray,
             keyword:chararray, display_site:chararray,
             placement:chararray, was_clicked:int, cpc:int);

-- Include only records where the ad was clicked
clicked = FILTER data BY was_clicked == 1;

-- A: Group everything so we can call the aggregate function
grouped = GROUP data ALL;

-- B: Count the records 
count = FOREACH grouped GENERATE COUNT(grouped.data); 

-- C: Display the result to the screen
--DUMP count;
STORE count INTO 'tcc';

