data = LOAD 'ad_data/' AS (campaign_id: chararray, date: chararray, 
time: chararray,  keyword: chararray, display_site: chararray, placement: chararray, was_clicked: int, cpc: int);

hcpc = ORDER data BY cpc DESC;
lhcpc = LIMIT hcpc 1;
pncc = FOREACH lhcpc GENERATE cpc*50000;

--DUMP pncc;
STORE pncc INTO 'pncc';
