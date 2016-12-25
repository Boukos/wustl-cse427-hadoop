orders = LOAD 'orders' AS (order_id:int,
             cust_id:int,
             order_dtm:chararray);

details = LOAD 'order_details' AS (order_id:int,
             prod_id:int);

-- include only the months we want to analyze
recent = FILTER orders BY order_dtm matches '^2013-0[2345]-.*$';

-- include only the product we're advertising
tablets = FILTER details BY prod_id == 1274348;


-- TODO (A): Join the two relations on the order_id field
od = JOIN recent BY order_id, tablets BY order_id;

-- TODO (B): Create a new relation containing just the month
odm = FOREACH od GENERATE recent::order_id, REGEX_EXTRACT(order_dtm, '^2013-(0[2345])-\\d{2}\\s.*$', 1) AS month;

-- TODO (C): Group by month and then count the records in each group
odmg = GROUP odm BY month;
odmgc = FOREACH odmg GENERATE group, COUNT(odm);

-- TODO (D): Display the results to the screen
--DUMP
STORE odmgc INTO 'odmgc';
