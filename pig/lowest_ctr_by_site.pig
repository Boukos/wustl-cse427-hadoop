-- Load only the ad_data1 and ad_data2 directories -- test_ad_data.txt
data = LOAD 'ad_data/' AS (campaign_id:chararray,
             date:chararray, time:chararray,
             keyword:chararray, display_site:chararray,
             placement:chararray, was_clicked:int, cpc:int);

grouped = GROUP data BY keyword;-- display_site; -- group, data

by_site = FOREACH grouped {
  -- Include only records where the ad was clicked
	gwc = FILTER data BY was_clicked == 1;
  -- count the number of records in this group
	wccount = COUNT(gwc);
	--wccountvalue = FOREACH wccount GENERATE (wccount.gwc)

  /* Calculate the click-through rate by dividing the 
   * clicked ads in this group by the total number of ads
   * in this group.
   */
	ctrval = wccount*100/COUNT(data);
	GENERATE group, ctrval AS ctr;
	
}

-- sort the records in ascending order of clickthrough rate
bso = ORDER by_site BY ctr DESC;

-- show just the first three
bsol = LIMIT bso 3;

DUMP bsol;