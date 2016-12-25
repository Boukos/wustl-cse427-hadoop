-- load the data sets
orders = LOAD 'orders' AS (order_id:int,
             cust_id:int,
             order_dtm:chararray);

details = LOAD 'order_details' AS (order_id:int,
             prod_id:int);

products = LOAD 'products' AS (prod_id:int,
             brand:chararray,
             name:chararray,
             price:int,
             cost:int,
             shipping_wt:int);


/*
 * TODO: Find all customers who had at least five orders
 *       during 2012. For each such customer, calculate 
 *       the total price of all products he or she ordered
 *       during that year. Split those customers into three
 *       new groups based on the total amount:
 *
 *        platinum: At least $10,000
 *        gold:     At least $5,000 but less than $10,000
 *        silver:   At least $2,500 but less than $5,000
 *
 *       Write the customer ID and total price into a new
 *       directory in HDFS. After you run the script, use
 *       'hadoop fs -getmerge' to create a local text file
 *       containing the data for each of these three groups.
 *       Finally, use the 'head' or 'tail' commands to check 
 *       a few records from your results, and then use the  
 *       'wc -l' command to count the number of customers in 
 *       each group.
 */
ordersf = FILTER orders BY order_dtm matches '^2012.*$';
ordersg = GROUP orders BY cust_id;
ordersc = FOREACH ordersg GENERATE group, COUNT(orders) AS num_orders;
ordersc5 = FILTER ordersc BY num_orders >= 5;

customer_order = JOIN ordersc5 BY group, ordersf BY cust_id;
customer_order_details = JOIN customer_order BY ordersf::order_id, details BY order_id;
customer_order_products = JOIN customer_order_details BY details::prod_id, products BY prod_id;

customer_prices = FOREACH customer_order_products GENERATE ordersc5::group AS cust_id, products::price AS price;

group_customer_total = GROUP customer_prices BY cust_id;
customer_total = FOREACH group_customer_total GENERATE group, SUM(customer_prices.price) AS total;

SPLIT customer_total INTO
platinum IF total >= 10000,
gold IF total >= 5000 AND total < 10000,
silver IF total >= 2500 AND total < 5000;

STORE platinum INTO 'platinum';
STORE gold INTO 'gold';
STORE silver INTO 'silver';

