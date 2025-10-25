
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


-------------------------
create or alter procedure bronze.load_bronze as 
begin 
DECLARE @start_time datetime,@end_time datetime,@start_bronze datetime , @end_bronze datetime
begin try;
set @start_bronze = GETDATE();
print '===================='
print 'loading bronze layer'
print '===================='
print '-------------------------------'
print '=================================='
print 'loading crm tables'
print '=================================='
set @start_time = GETDATE();
print 'truncating table : bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;
		print 'inserting table : bronze.crm_cust_info'
		bulk insert bronze.crm_cust_info
		from 'C:\Users\AFAQ\Downloads\Compressed\source_crm\cust_info.csv'
		with(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		set @end_time = GETDATE();
		PRINT('LOAD DURATION : ') + cast(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar) + ' seconds';
		print '=================================='

		-------------------------------
		set @start_time = GETDATE();
		print 'truncating table : bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info ;
		print 'inserting table : bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info 
		FROM 'C:\Users\AFAQ\Downloads\Compressed\source_crm\prd_info.csv'
		WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		set @end_time = GETDATE();
		PRINT('LOAD DURATION : ') + cast(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar) + ' seconds';
		print '=================================='

		------------------------------------
		set @start_time = GETDATE();
		print 'truncating table : bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details ;
		print 'inserting table : bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details 
		FROM 'C:\Users\AFAQ\Downloads\Compressed\source_crm\sales_details.csv'
		WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		set @end_time = GETDATE();
		PRINT('LOAD DURATION : ') + cast(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar) + ' seconds';
		print '=================================='

		
print '=================================='
print 'loading erp tables'
print '=================================='

		-------------------------------
		set @start_time = GETDATE();
		print 'truncating table : bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12
		print 'inserting table : bronze.erp_cust_az12'

		bulk insert bronze.erp_cust_az12
		from 'C:\Users\AFAQ\Downloads\Compressed\source_erp\cust_az12.csv'
		with (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		set @end_time = GETDATE();
		PRINT('LOAD DURATION : ') + cast(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar) + ' seconds';
		print '=================================='

		---------------------------------------------
		set @start_time = GETDATE();
		print 'truncating table : bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101
		print 'inserting table : bronze.erp_loc_a101'
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\AFAQ\Downloads\Compressed\source_erp\loc_a101.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = GETDATE();
		PRINT('LOAD DURATION : ') + cast(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar) + ' seconds';
		print '=================================='

		------------------------------------------
		set @start_time = GETDATE();
print 'truncating table : bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
print 'inserting table : bronze.erp_px_cat_g1v2'
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\AFAQ\Downloads\Compressed\source_erp\px_cat_g1v2.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_time = GETDATE();
		PRINT('LOAD DURATION : ') + cast(DATEDIFF(SECOND,@start_time,@end_time) as nvarchar) + ' seconds';
		print '==================================';
		set @end_bronze  = GETDATE();
		print ('LOADING BRONZE LAYER TIME IS : ') + CAST(datediff(SECOND,@start_bronze,@end_bronze) as nvarchar) + ' seconds';
end try  
begin catch

print '==================================';
print 'error occured loading bronze layer';
print 'error message' + ERROR_MESSAGE();
print 'error message' + CAST(ERROR_MESSAGE() AS NVARCHAR);
print 'error message' + CAST(ERROR_MESSAGE() AS NVARCHAR);
print '==================================';
end catch
end
