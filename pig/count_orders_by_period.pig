data = LOAD 'orders' AS (order_id:int,
             cust_id:int,
             order_dtm:chararray);

/*
 * Include only records where the 'order_dtm' field matches
 * the regular expression pattern:
 *
 *   ^       = beginning of string
 *   2013    = literal value '2013'
 *   0[2345] = 0 followed by 2, 3, 4, or 5
 *   -       = a literal character '-'
 *   \\d{2}  = exactly two digits
 *   \\s     = a single whitespace character
 *   .*      = any number of any characters
 *   $       = end of string
 *
 * If you are not familiar with regular expressions and would
 * like to know more about them, see the Regular Expression 
 * Reference at the end of the Exercise Manual.
 */
recent = FILTER data by order_dtm matches '^2013-0[2345]-\\d{2}\\s.*$';

-- TODO (A): Create a new relation with just the order's year and month
oym = FOREACH recent GENERATE order_id, 2013 AS year, REGEX_EXTRACT(order_dtm, '^2013-(0[2345])-\\d{2}\\s.*$', 1) AS month;

-- TODO (B): Count the number of orders in each month
oymg = GROUP oym BY month;
oymgc = FOREACH oymg GENERATE group, COUNT(oym);

-- TODO (C): Display the count by month to the screen.
--DUMP oymgc;
STORE oymgc INTO 'oym';

