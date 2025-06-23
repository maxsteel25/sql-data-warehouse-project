----loading first table data into crm 

CREATE OR ALTER PROCEDURE bronze.load_bronze as 
begin
	print'Loading bronze layer crm table '
	declare @start_time Datetime ,@end_time datetime,@batch_start_time Datetime,@batch_end_time Datetime;;
	set @batch_start_time=GETDATE();
	print''
	print'loading bronze layer'
	set @start_time =getdate();
	truncate table bronze.crm_cust_info;
	bulk insert bronze.crm_cust_info
	from 'C:\Users\ivishgup\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with (
	firstrow =2,
	fieldterminator=',',
	tablock
	)
	set @end_time=GETDATE();
	print '>>load duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar) +' sec';
	--select * from bronze.crm_cust_info

	/* how to truncate table
	truncate table bronze.crm_cust_info
	making the table empty 
	*/


	--loading data into table 2
	set @start_time =getdate();
	truncate table bronze.crm_prd_info;
	bulk insert bronze.crm_prd_info 
	from 'C:\Users\ivishgup\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with (
	firstrow =2,
	fieldterminator=',',
	tablock
	)
	set @end_time=GETDATE();
	print '>>load duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar) +'sec';

	--loading data into table 3 
	set @start_time =getdate();
	truncate table bronze.crm_sales_details;
	bulk insert  bronze.crm_sales_details
	from 'C:\Users\ivishgup\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with (
	firstrow =2,
	fieldterminator=',',
	tablock
	)
	set @end_time=GETDATE();
	print '>>load duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar) +'sec';


	----loading data into table 4 bronze.erp_cust_az12
	print''
	print'Loading bronze layer erp table';

	set @start_time =getdate();
	truncate table bronze.erp_cust_az12;
	bulk insert  bronze.erp_cust_az12
	from 'C:\Users\ivishgup\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	with (
	firstrow =2,
	fieldterminator=',',
	tablock
	)
	set @end_time=GETDATE();
	print '>>load duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar) +'sec';
	----loading data into table 5 bronze.erp_loc_a101
	set @start_time =getdate();
	truncate table bronze.erp_loc_a101;
	bulk insert bronze.erp_loc_a101
	from 'C:\Users\ivishgup\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	with (
	firstrow =2,
	fieldterminator=',',
	tablock
	)
	set @end_time=GETDATE();
	print '>>load duration' + cast(datediff(second,@start_time,@end_time) as nvarchar) +'sec';
	----loading data into table 6 bronze.erp_px_cat_g1v2
	set @start_time =getdate();
	truncate table bronze.erp_px_cat_g1v2;
	bulk insert bronze.erp_px_cat_g1v2
	from 'C:\Users\ivishgup\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	with (
	firstrow =2,
	fieldterminator=',',
	tablock
	)
	set @end_time=GETDATE();
	print '>>load duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar) +'sec';
	print''
	set @batch_end_time=GETDATE()
	print '- total load duration ' + cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar) + ' sec';
end
--select * from bronze.erp_px_cat_g1v2

--checking is stored procedure exist 

/*SELECT *
FROM INFORMATION_SCHEMA.ROUTINES
WHERE ROUTINE_TYPE = 'PROCEDURE'
AND ROUTINE_NAME = 'bronze.load_bronze';*/

/*  IF EXISTS (SELECT * 
           FROM sys.objects 
           WHERE object_id = OBJECT_ID(N'bronze.load_bronze') 
           AND type = 'P')
BEGIN
    PRINT 'Stored Procedure bronze.load_bronze exists.'
END
ELSE
BEGIN
    PRINT 'Stored Procedure bronze.load_bronze does not exist.'
END
*/

