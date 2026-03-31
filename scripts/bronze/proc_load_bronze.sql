/*
=========================================================
Stored Procedure: Loading Bronze Layer 
=========================================================
Purpose:
  This store procedure loads into data bronze layer from external CSV files.
  It performs actions liek
    - Truncate the bronze tables before loading data
    - Uses **BULK INSERT** command to load data from CSV files to bronze tables

parameter: No parameters used

Usage example: EXEC bronze.load_bronze_layer;
=========================================================
*/


--Save frequently used SQL code in stored procedure in database
CREATE OR ALTER PROCEDURE bronze.load_bronze_layer AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY     --sql runs TRY block and if it fails, it runs CATCH block to handle error
		SET @batch_start_time =GETDATE();
		PRINT '============================================='
		PRINT 'Loading Bronze Layer'
		PRINT '============================================='

		PRINT '---------------------------------------------'
		PRINT 'Loading crm Tabes'
		print '---------------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>>Truncating table bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info; 

		PRINT '>>Inserting data into bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\RL Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time =GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------'

		SET @start_time = GETDATE();
		PRINT '>>Truncating table bronze.crm_prod_info'
		TRUNCATE TABLE bronze.crm_prod_info; 

		PRINT '>>Inserting data into bronze.crm_prod_info'
		BULK INSERT bronze.crm_prod_info
		FROM 'D:\RL Project\sql-data-warehouse-project\datasets\source_crm\prod_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time =GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------'

		SET @start_time = GETDATE();
		PRINT '>>Truncating table bronze.crm_sales_info'
		TRUNCATE TABLE bronze.crm_sales_info;

		PRINT '>>Inserting data into bronze.crm_sales_info'
		BULK INSERT bronze.crm_sales_info
		FROM 'D:\RL Project\sql-data-warehouse-project\datasets\source_crm\sales_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------'


		PRINT '---------------------------------------------'
		PRINT 'Loading erp Tabes'
		print '---------------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>>Truncating table bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>>Inserting data into bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\RL Project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------'

		SET @start_time =GETDATE();
		PRINT '>>Truncating table bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>>Inserting data into bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\RL Project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------'

		SET @start_time = GETDATE();
		PRINT '>>Truncating table bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>>Inserting data into bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\RL Project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time =GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------'

		SET @batch_end_time =GETDATE();
		PRINT 'Loading Bronze Load Completed'
		PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
	
	END TRY
	BEGIN CATCH  
		PRINT '===================================='  
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + Error_Message();
		PRINT '===================================='
		 -----these are PRINT error messages we used here to print if errors occured
	END CATCH
END

--- to track the ETL process load duration i.e SET, @START_TIME, @END_TIME
--- which helps to identify bottlenecks, optimize performance, monitor trends, detect issues
