-----------silver layer building dwh ---------------

if OBJECT_ID('silver.crm_cust_info','U') is not null
drop table silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
cst_id int,
cst_key NVARCHAR(50),
cst_first_name NVARCHAR(50),
cst_last_name NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date date,
dwh_create_date datetime2 default getdate()
);

-----------------------------------
if OBJECT_ID('silver.crm_prd_info', 'U' ) is not null
drop table silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
prd_id int,
cat_id NVARCHAR(50),
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost int,
prd_line NVARCHAR(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date datetime2 default getdate()

);

---------------------------
if OBJECT_ID('silver.crm_sales_details', 'U' ) is not null
drop table silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
		sls_ord_num NVARCHAR(50),
		sls_prd_key NVARCHAR(50),
		sls_cust_id int,
		sls_order_dt date,
		sls_ship_dt date,
		sls_due_dt date,
		sls_sales int,
		sls_quantity int,
		sls_price int,
dwh_create_date datetime2 default getdate()

);
------------------------------------
if OBJECT_ID('silver.erp_loc_a101', 'U' ) is not null
drop table silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
		cid NVARCHAR(50),
		centry NVARCHAR(50),
dwh_create_date datetime2 default getdate()

);

-----------------------------
if OBJECT_ID('silver.erp_cust_az12', 'U' ) is not null
drop table silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
		cid NVARCHAR(50),
		bdate date,
		gen NVARCHAR(50),
dwh_create_date datetime2 default getdate()

);

-------------------------------------
if OBJECT_ID('silver.erp_px_cat_g1v2', 'U' ) is not null
drop table silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
		id NVARCHAR(50),
		cat NVARCHAR(50),
		subcat NVARCHAR(50),
		maintenance NVARCHAR(50),
dwh_create_date datetime2 default getdate()

);

