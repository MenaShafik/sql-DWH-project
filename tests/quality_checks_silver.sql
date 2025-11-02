--check for nulls or duplicates in primary key
----expectation !! ---> no result
select cst_id,
COUNT(cst_id)
from bronze.crm_cust_info
group by cst_id
having COUNT(*) > 1 or cst_id is null

----------the way to make it is rank--------------
select *
from (
select *,
ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag

from bronze.crm_cust_info
)t
where flag =1

----------check unwanted spaces
select cst_first_name
from bronze.crm_cust_info
where cst_first_name != TRIM(cst_first_name)
------------------------------------
--data standardization & consistency
select distinct cst_gndr
from bronze.crm_cust_info
----------------------------
--check for invalid date orders

select *
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt

---------------------
select nullif(sls_order_dt,0) as sls_order_dt 
from bronze.crm_sales_details
where sls_order_dt <=0
or LEN(sls_order_dt) !=8
or sls_order_dt > 20500101
or sls_order_dt < 19000101

---------------
--check data consistency:  between sales,quantity, and price
--> sales = quantity * price
---> values must not be null, zero, or negative numbers

select sls_sales,
sls_price,
sls_quantity
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_price is null
or sls_quantity is null
or sls_sales is null
--------------------------
-- out of range dates

select distinct bdate
from bronze.erp_cust_az12
where bdate <'1924-01-01' or bdate > GETDATE()
