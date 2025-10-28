/*
===================================================
createing the view of customer dim
===================================================
*/
IF OBJECT_ID('gold.dim_customers','V') is not null
drop view gold.dim_customers;
go
CREATE VIEW gold.dim_customers AS
SELECT 
ROW_NUMBER()over(order by cst_id) as customer_key,
			ci.cst_id as customer_id,
			ci.cst_key as customer_number,
			ci.cst_first_name as first_name,
			ci.cst_last_name as last_name,
			la.centry as country,
			ci.cst_material_status as maretal_status,
			ca.bdate,
			ci.cst_create_date as create_date,
			CASE WHEN ci.cst_gndr != 'n/a' then ci.cst_gndr
			else coalesce(ca.gen,'n/a')
			end as gender

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
on ci.cst_key = la.cid
/*
===================================================
createing the view of product dim
===================================================
*/
IF OBJECT_ID('gold.dim_products','V') is not null
DROP VIEW gold.dim_products
go
create view gold.dim_products  as

select 
ROW_NUMBER()OVER(ORDER BY prd.prd_start_dt, prd.prd_key) as product_key,
prd.prd_id as product_id,
prd.prd_key as product_number,
prd.cat_id as category_id,
cat.cat as category_name,
cat.subcat as subcategory,
cat.maintenance,
prd.prd_nm as product_name,
prd.prd_cost as product_cost,
prd.prd_line as product_line,
prd.prd_start_dt as start_date
from silver.crm_prd_info prd
left join silver.erp_px_cat_g1v2 as cat
on prd.cat_id = cat.id
where prd.prd_end_dt is  null
/*
===================================================
createing the view of fact sales
===================================================
*/

IF OBJECT_ID('gold.fact_sales') IS NOT NULL
DROP VIEW gold.fact_sales
GO
create view gold.fact_sales as
SELECT sls_ord_num order_number,
PR.product_key,
CR.customer_key,
SA.sls_order_dt as order_date,
SA.sls_ship_dt as shipping_date,
SA.sls_due_dt as due_date,
SA.sls_sales as sales_amount,
SA.sls_quantity as quantity,
SA.sls_price as price
FROM silver.crm_sales_details SA
LEFT JOIN gold.dim_products AS PR
ON SA.sls_prd_key = PR.product_number
LEFT JOIN gold.dim_customers AS CR
ON SA.sls_cust_id = CR.customer_id
