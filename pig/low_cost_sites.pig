-- TODO (A): Replace 'FIXME' to load the test_ad_data.txt file.
-- test_ad_data.txt
data = LOAD 'ad_data/' AS (campaign_id:chararray,
             date:chararray, time:chararray,
             keyword:chararray, display_site:chararray, 
             placement:chararray, was_clicked:int, cpc:int);

-- TODO (B): Include only records where was_clicked has a value of 1
c1 = FILTER data BY was_clicked == 1;

-- TODO (C): Group the data by the appropriate field
bysite = GROUP c1 BY display_site;

-- TODO (D): Create a new relation which includes only the 
--           display site and the total cost of all clicks 
--           on that site
-- */
dstc = FOREACH bysite GENERATE group, SUM(c1.cpc) AS cost;

-- TODO (E): Sort that new relation by cost (ascending)
sdstc = ORDER dstc BY cost;

-- TODO (F): Display just the first three records to the screen
lsdstc = LIMIT sdstc 4;
--DUMP lsdstc;
STORE lsdstc INTO 'lcs';
