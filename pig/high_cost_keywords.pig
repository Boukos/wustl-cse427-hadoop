data = LOAD 'ad_data/' AS (campaign_id: chararray, date: chararray, 
time: chararray,  keyword: chararray, display_site: chararray, placement: chararray, was_clicked: int, cpc: int);
 
keyword_group = GROUP data BY keyword; 

keyword_count = FOREACH keyword_group GENERATE group, SUM(data.cpc) AS number;

skc = ORDER keyword_count BY number DESC;

skcl = LIMIT skc 3;
  
--DUMP skc;
STORE skcl INTO 'hck';
