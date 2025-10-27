/*======================================================================================*/
--------loading data from crm_cust_info (bronze layer) with cleaning the data---------
/*======================================================================================*/
create or alter procedure silver.load_silver as
declare @start_time datetime,@end_time datetime,@silver_start_time datetime,@silver_end_time datetime
begin try
set @silver_start_time = GETDATE();
print('======================================================================================')
print('loading data from erp_loc_a101 (bronze layer) with cleaning the data')
print('======================================================================================')
		set @start_time = GETDATE();

print('======================================================================================')
print('=====================truncaring data into silver.crm_cust_info =======================')
print('======================================================================================')
		truncate table silver.crm_cust_info
print('======================================================================================')
print('=====================inserting data into silver.crm_cust_info =======================')
print('======================================================================================')
		insert into silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_first_name,
		cst_last_name,
		cst_gndr,
		cst_material_status,
		cst_create_date
		)
		select cst_id,
		cst_key,
		TRIM(cst_first_name) as cst_first_name,
		TRIM(cst_last_name) as cst_last_name,
		case when UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			when UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
			else 'n/a'
			end as cst_gndr,
			case when UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			when UPPER(TRIM(cst_material_status)) = 'M' then 'Married'
			else 'n/a'
			end as cst_material_status,
		cst_create_date

		from(
		select *,
		ROW_NUMBER()over(PARTITION by cst_id order by cst_create_date) as flag
		from bronze.crm_cust_info
		where cst_id is not null
		)t
		where flag = 1
		set @end_time = GETDATE() 
		print('======================================================================================')
		print('duration is : '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds')
		print('======================================================================================')

		print('	======================================================================================')
		print('--------loading data from crm_prd_info (bronze layer) with cleaning the data---------')
		print('======================================================================================')


		------------insert into silver layer---------------
		set @start_time = GETDATE()
		print('======================================================================================')
		print('=====================truncaring data into silver.crm_prd_info =======================')
		print('======================================================================================')
		truncate table silver.crm_prd_info

		print('======================================================================================')
		print('=====================inserting data into silver.crm_prd_info =======================')
		print('======================================================================================')
		insert into silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		TRIM(prd_nm) as prd_nm,
		ISNULL(prd_cost,0) prd_cost,
		case UPPER(TRIM(prd_line))
		when 'M' then 'Mountain'
			 when	'R' then 'Road'
			 when	'S' then 'Other Sales'
			 when	'T' then 'Touring'
			else	'n/a'
			END
		as prd_line,
		CAST(prd_start_dt as date) as prd_start_dt,
		CAST(LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date)
		as prd_end_dt
		FROM bronze.crm_prd_info

		set @end_time = GETDATE() 
		print('======================================================================================')
		print('duration is : '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds')
		print('======================================================================================')
/*======================================================================================*/
--------loading data from crm_sales_details (bronze layer) with cleaning the data---------
/*======================================================================================*/

-----------------------------------------
----------insert data into silver layer---------------
	
		set @start_time = GETDATE()
print('=====================================================================================')
print('=====================truncaring data into silver.crm_sales_details ==================')
print('=====================================================================================')
		truncate table silver.crm_sales_details
print('=====================================================================================')
print('=====================inserting data into silver.crm_sales_details ===================')
print('=====================================================================================')
		insert into silver.crm_sales_details (
	sls_ord_num ,
		sls_prd_key,
		sls_cust_id ,
		sls_order_dt ,
		sls_ship_dt ,
		sls_due_dt ,
		sls_sales ,
		sls_quantity ,
		sls_price 
		)
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt =0
		or LEN(sls_order_dt) != 8 then Null
		else CAST(CAST(sls_order_dt as nvarchar) as date) 
		end as sls_order_dt,
		case when sls_ship_dt =0
		or len(sls_order_dt)!= 8 then null
		else CAST(CAST(sls_ship_dt as nvarchar) as date)
		end
		as sls_ship_dt,
		case when sls_due_dt = 0
		or LEN(sls_due_dt)!=8 then null
		else CAST(CAST(sls_due_dt as nvarchar) as date)
		end
		as sls_due_dt,

		case when sls_sales is null 
		or sls_sales <=0
		or sls_sales != sls_quantity * ABS(sls_price)
		then sls_quantity * ABS(sls_price)
		else sls_sales
		end
		as sls_sales,
		sls_quantity,
		case when sls_price is null
		or sls_price <=0
		or sls_price != sls_quantity * ABS(sls_sales)
		then sls_quantity * ABS(sls_sales)
		else sls_price
		end 
		as sls_price
		from bronze.crm_sales_details
		set @end_time = GETDATE() 
		print('======================================================================================')
		print('duration is : '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds')
		print('======================================================================================')
		/*======================================================================================*/
		------loading data from erp_cust_az12 (bronze layer) with cleaning the data---------
		/*======================================================================================*/

		---------------insert into silver layer-----------------------
		set @start_time = GETDATE()
print('======================================================================================')
print('=====================truncaring data into silver.erp_cust_az12 =======================')
print('======================================================================================')
		truncate table silver.erp_cust_az12
print('======================================================================================')
print('=====================inserting data into silver.erp_cust_az12 ========================')
print('======================================================================================')
		insert into silver.erp_cust_az12(
		cid,bdate,gen
		)
		-----------------------------------------
		select 
		case when cid like '%NAS%'
		THEN SUBSTRING(cid,4,LEN(cid))
		ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE()
		THEN NULL 
		ELSE bdate
		END
		AS bdate,
		CASE WHEN UPPER(TRIM(gen)) in ('F','FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) in ('M','MALE') THEN 'Male'
			 ELSE 'n/a'
			end as gen
		from bronze.erp_cust_az12

		set @end_time = GETDATE() 
		print('======================================================================================')
		print('duration is : '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds')
		print('======================================================================================')
		/*======================================================================================*/
		------loading data from erp_loc_a101 (bronze layer) with cleaning the data---------
		/*======================================================================================*/
				set @start_time = GETDATE()
print('======================================================================================')
print('=====================truncaring data into silver.erp_loc_a101 =======================')
print('======================================================================================')
		truncate table silver.erp_loc_a101
print('======================================================================================')
print('=====================inserting data into silver.erp_loc_a101 =========================')
print('======================================================================================')
		insert into silver.erp_loc_a101(
		cid,
		centry)

		select REPLACE(cid,'-','') as cid,
		case when TRIM(centry) = 'DE' THEN 'Germany'
		WHEN TRIM(centry) IN ('USA','US','UNITED STATES') THEN 'United States'
		WHEN TRIM(centry) = '' OR centry IS NULL THEN 'n/a'
		else trim(centry)
		end
		as centry
		from bronze.erp_loc_a101

		set @end_time = GETDATE() 
		print('======================================================================================')
		print('duration is : '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds')
		print('======================================================================================')
		/*======================================================================================*/
		------loading data from erp_px_cat_g1v2 (bronze layer) with cleaning the data---------
		/*======================================================================================*/
		set @start_time = GETDATE()
print('======================================================================================')
print('=====================truncate data into silver.erp_px_cat_g1v2 =======================')
print('======================================================================================')
		truncate table silver.erp_px_cat_g1v2
print('======================================================================================')
print('=====================inserting data into silver.erp_px_cat_g1v2 =======================')
print('======================================================================================')
		insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
		select id,
		cat,subcat,maintenance
		from bronze.erp_px_cat_g1v2
		set @end_time = GETDATE() 
		print('======================================================================================')
		print('duration is : '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds')
		print('======================================================================================')
		set @silver_end_time = GETDATE() 
		print('======================================================================================')
		print('total duration is : '+ cast(datediff(second,@silver_start_time,@silver_end_time) as nvarchar) + ' seconds')
		print('======================================================================================')
end try
begin catch
print('=============================')
print('error message'+ ERROR_MESSAGE())
print('=============================')

end catch
