---Steps for Retail_Order Data Engineer Project---
--1.Goto kaggle.com create account if not there and click on setting then API create api it will download kaggle.json file.
--2. Now pate that file in home user .kaggle folder 
--3. Dataset link https://www.kaggle.com/datasets/ankitbansal06/retail-orders
--4. Remaining things we have done in hk_order_analysis.ipynb file

---Data Extraction from kaggle api>>>Data Cleaning and transformation in python pandas>>>now Data analysis in SQL server
----Now Data analyse after loading in SQL server-----df_orders tables

-- find top 10 highest reveue generating products T
select * from df_orders;
select top 10 product_id, sum(sale_price) sales from df_orders group by product_id order by sales desc;

-- find top 5 highest selling products in each region
with cte as 
(select region,product_id, sum(sale_price) as sales,ROW_NUMBER() over(partition by region order by sum(sale_price) desc) rn from df_orders group by region,product_id)
select * from cte where rn<=5 order by region,sales desc;

-- find month over month growth comparison for 2022 and 2023 sales eg : jan 2022 vs jan 2023
with cte as 
(select year(order_date) order_year,month(order_date) order_month,sum(sale_price) sales from df_orders group by year(order_date), month(order_date))
select order_month,
sum(case when order_year='2022' then sales else 0 end) as sales_2022,
sum(case when order_year='2023' then sales else 0 end) as sales_2023 from cte group by order_month order by order_month;

-- for each category which month had highest sales  ---important query
with cte as
(select sum(sale_price) as sales,category, format(order_date,'yyyyMM') as order_year_month  from df_orders group by category,format(order_date,'yyyyMM'))
select * from (
    select *,ROW_NUMBER() over(partition by category order by sales desc) rn from cte) a where rn=1;

-- which sub category had highest growth by profit in 2023 compare to 2022
with cte as 
(select sub_category,year(order_date) order_year, sum(sale_price) sales from df_orders group by sub_category,year(order_date)),
cte2 as
(select sub_category,
sum(case when order_year='2022' then sales else 0 end) as sales_2022,
sum(case when order_year='2023' then sales else 0 end) as sales_2023 from cte group by sub_category) select top 1 *, (sales_2023-sales_2022)*100/sales_2022 as growth_per from cte2 order by (sales_2023-sales_2022)*100/sales_2022 desc;--if it is only asking growth then sales_2023-sales_2022 is enough


