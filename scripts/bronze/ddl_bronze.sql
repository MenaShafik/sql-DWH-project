
/*
=================================
ddl script : create bronze tables
=================================
script purpose :
this script creates table in the 'bronze' schema,dropping existing tables
if this already exist.
run this script to re-define the ddl structure of 'bronze' tables
*/


use master
CREATE DATABASE DataWarehouse;
USE DataWarehouse;
GO
CREATE SCHEMA bronze;
go
CREATE SCHEMA silver;
go
CREATE SCHEMA gold;

--------------------------------
if OBJECT_ID('bronze.crm_cust_info','U') is not null
drop table bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
cst_id int,
cst_key NVARCHAR(50),
cst_first_name NVARCHAR(50),
cst_last_name NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date date
);

-----------------------------------
if OBJECT_ID('bronze.crm_prd_info', 'U' ) is not null
drop table bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
prd_id int,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost int,
prd_line NVARCHAR(50),
prd_start_dt datetime,
prd_end_dt datetime
);

---------------------------
if OBJECT_ID('bronze.crm_sales_details', 'U' ) is not null
drop table bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
		sls_ord_num NVARCHAR(50),
		sls_prd_key NVARCHAR(50),
		sls_cust_id int,
		sls_order_dt int,
		sls_ship_dt int,
		sls_due_dt int,
		sls_sales int,
		sls_quantity int,
		sls_price int
);
------------------------------------
if OBJECT_ID('bronze.erp_loc_a101', 'U' ) is not null
drop table bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
		cid NVARCHAR(50),
		centry NVARCHAR(50),
);

-----------------------------
if OBJECT_ID('bronze.erp_cust_az12', 'U' ) is not null
drop table bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
		cid NVARCHAR(50),
		bdate date,
		gen NVARCHAR(50)
);

-------------------------------------
if OBJECT_ID('bronze.erp_px_cat_g1v2', 'U' ) is not null
drop table bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
		id NVARCHAR(50),
		cat NVARCHAR(50),
		subcat NVARCHAR(50),
		maintenance NVARCHAR(50)
);
