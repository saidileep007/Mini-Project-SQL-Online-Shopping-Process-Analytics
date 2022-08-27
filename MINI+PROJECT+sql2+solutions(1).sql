create database mini_project;
use  mini_project;

-- 1.Join all the tables and create a new table called combined_table.(market_fact, cust_dimen, orders_dimen, prod_dimen, shipping_dimen)

create table combined_table (
select m.ord_id,m.prod_id,m.ship_id,m.cust_id,sales,discount,order_quantity,profit,shipping_cost,
product_base_margin,customer_name,province,region,customer_segment,o.order_id,order_date,
order_priority,product_category,product_sub_category,ship_mode,ship_date
 from market_fact m join  cust_dimen c on m.cust_id = c.cust_id 
 join orders_dimen o on m.ord_id =o.ord_id 
 join prod_dimen p on m.prod_id = p.prod_id 
 join shipping_dimen s on m.ship_id =s.ship_id);
select * from combined_table;

-- 2. Find the top 3 customers who have the maximum number of orders
select distinct cust_id,customer_name,sum(order_quantity) over(partition by cust_id)total 
from combined_table order by total desc limit 3 ;
desc combined_table;
-- 3. Create a new column DaysTakenForDelivery that contains the date difference of Order_Date and Ship_Date.
select *,datediff(shp_date,ordr_date)DaysTakenForDelivery 
from (select str_to_date(order_date,'%d-%m-%y')ordr_date,str_to_date(ship_date,'%d-%m-%y')shp_date from combined_table)t;
-- 4.Find the customer whose order took the maximum time to get delivered.
select *,datediff(shp_date,ordr_date)DaysTakenForDelivery
from (select cust_id,customer_name,str_to_date(order_date,'%d-%m-%y')ordr_date,str_to_date(ship_date,'%d-%m-%y')shp_date from combined_table)t order by DaysTakenForDelivery desc limit 1;
-- 5. Retrieve total sales made by each product from the data (use Windows function)
select distinct prod_id,product_category,product_sub_category,sum(sales) over(partition by prod_id) total_sales from combined_table;
-- 6. Retrieve total profit made from each product from the data (use windows function)
select distinct prod_id,product_category,product_sub_category,sum(profit) over(partition by prod_id) total_profit from combined_table;
-- 7. Count the total number of unique customers in January and how many of them came back every month over the entire year in 2011
select distinct t.cust_id,count(t.cust_id) over(partition by t.order_date)unq_cust,t.order_date,c.order_date from
(select distinct cust_id,order_date from combined_table where order_date like '%2011' group by cust_id )t join 
(select distinct cust_id,order_date from combined_table where order_date like '__-01-2011' group by cust_id )c on t.cust_id = c.cust_id group by t.order_date,t.cust_id ;
-- 8. Retrieve month-by-month customer retention rate since the start of the business.(using views)
create view user_visit as(
select cust_id,customer_name,order_date from combined_table);
select * from user_visit;
drop view user_visit;
select *, case 
when gap > 1 then 'irregular'
when gap <= 1 then 'retained'
when gap is null then 'churned'
end as 'categorize'
from (select * ,datediff(ord,lag_)/31 gap from (select *,lag(ord) over(partition by cust_id order by ord)lag_ from
(select cust_id,customer_name,str_to_date(order_date,'%d-%m-%y')ord from user_visit)t)t1)t2 ;
