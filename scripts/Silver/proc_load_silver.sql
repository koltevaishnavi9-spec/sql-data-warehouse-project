/*

Stored Procedure: Load Silver Layer (Bronze -> Silver)

Script Purpose:
This stored procedure performs the ETL (Extract, Transform, Load) process to
populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
- Truncates Silver tables.
- Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC Silver.load_silver;

*/


EXEC SILVER.load_silver
CREATE OR ALTER PROCEDURE SILVER.load_silver AS
BEGIN
   DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
    SET @batch_start_time = GETDATE();
        PRINT '=========================================================';
        PRINT 'LOADING SILVER LAYER';
        PRINT '=========================================================';

        PRINT '---------------------------------------------------------';
        PRINT 'LOADING CRM TABLES';
        PRINT '---------------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: SILVER.crm_cust_info_stage';
        TRUNCATE TABLE SILVER.crm_cust_info_stage;
        PRINT '>> Inserting Data Into: SILVER.crm_cust_info_stage';
        INSERT INTO SILVER.crm_cust_info_stage(
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date)
        SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE WHEN cst_marital_status = 'S' THEN 'SINGLE'
             WHEN cst_marital_status = 'M' THEN 'MARRIED'
             ELSE 'n/a'
        END cst_marital_status,
        CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
             WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
             ELSE 'n/a'
        END cst_gndr,
        cst_create_date
        FROM
        (SELECT *,
        ROW_NUMBER() over(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM BRONZE.crm_cust_info_stage
        WHERE cst_id IS NOT NULL) t
        WHERE flag_last = 1 
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: '
              + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR)
              + ' seconds';
        PRINT '>> ---------------------------';


        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: SILVER.crm_prd_info_stage';
        TRUNCATE TABLE SILVER.crm_prd_info_stage;
        PRINT '>> Inserting Data Into: SILVER.crm_prd_info_stage';
        INSERT INTO SILVER.crm_prd_info_stage
        (
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
            prd_nm,
            ISNULL(prd_cost,0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            prd_start_dt,
            LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt
        FROM BRONZE.crm_prd_info_stage;
         SET @end_time = GETDATE();
        PRINT '>> Load Duration: '
              + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR)
              + ' seconds';
        PRINT '>> ---------------------------';


        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.crm_sales_details_stage';
        TRUNCATE TABLE silver.crm_sales_details_stage;
        PRINT '>> Inserting Data Into: silver.crm_sales_details_stage';
        INSERT INTO silver.crm_sales_details_stage(
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price)
        SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END AS sls_order_dt,
        CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END AS sls_ship_dt,
        CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END AS sls_due_dt,
        CASE
                WHEN TRY_CAST(sls_sales AS MONEY) IS NULL
                  OR TRY_CAST(sls_sales AS MONEY) <= 0
                  OR TRY_CAST(sls_sales AS MONEY) <>
                     TRY_CAST(sls_quantity AS INT) * ABS(TRY_CAST(sls_price AS MONEY))
                THEN TRY_CAST(sls_quantity AS INT) * ABS(TRY_CAST(sls_price AS MONEY))
                ELSE TRY_CAST(sls_sales AS MONEY)
            END AS sls_sales,
        sls_quantity,
         CASE
                WHEN TRY_CAST(sls_price AS MONEY) IS NULL
                  OR TRY_CAST(sls_price AS MONEY) <= 0
                THEN TRY_CAST(sls_sales AS MONEY) /
                     NULLIF(TRY_CAST(sls_quantity AS INT), 0)
                ELSE TRY_CAST(sls_price AS MONEY)
            END AS sls_price
        FROM bronze.crm_sales_details_stage
         SET @end_time = GETDATE();
        PRINT '>> Load Duration: '
              + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR)
              + ' seconds';
        PRINT '>> ---------------------------';

        PRINT '---------------------------------------------------------';
        PRINT 'LOADING ERP TABLES';
        PRINT '---------------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table:SILVER.erp_CUST_AZ12_stage';
        TRUNCATE TABLE SILVER.erp_CUST_AZ12_stage;
        PRINT '>> Inserting Data Into: SILVER.erp_CUST_AZ12_stage';
        INSERT INTO SILVER.erp_CUST_AZ12_stage(CID, BDATE, GEN)
        SELECT
        CASE WHEN CID LIKE '%NAS' THEN SUBSTRING(CID , 4 , LEN(CID))
        ELSE CID
        END AS CID,
        CASE WHEN BDATE > GETDATE() THEN NULL
        ELSE BDATE
        END AS BDATE,
        CASE WHEN UPPER(TRIM(GEN)) IN ('F' , 'FEMALE') THEN 'Female'
              WHEN UPPER(TRIM(GEN)) IN ('M' , 'MALE') THEN 'Male'
              ELSE 'n/a'
        END AS GEN
        FROM BRONZE.erp_CUST_AZ12_stage
         SET @end_time = GETDATE();
        PRINT '>> Load Duration: '
              + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR)
              + ' seconds';
        PRINT '>> ---------------------------';


        SET @start_time = GETDATE();
        PRINT '>> Truncating Table:SILVER.erp_LOC_A101_stage';
        TRUNCATE TABLE SILVER.erp_LOC_A101_stage;
        PRINT '>> Inserting Data Into: SILVER.erp_LOC_A101_stage';
        INSERT INTO SILVER.erp_LOC_A101_stage
        (CID, CNTRY)
        SELECT 
        REPLACE(CID , '-' , '') CID,
        CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
             WHEN TRIM(CNTRY) IN ('US' , 'USA') THEN 'United States'
             WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
             ELSE TRIM(CNTRY)
        END AS CNTRY
        FROM BRONZE.erp_LOC_A101_stage
         SET @end_time = GETDATE();
        PRINT '>> Load Duration: '
              + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR)
              + ' seconds';
        PRINT '>> ---------------------------';


        SET @start_time = GETDATE();
        PRINT '>> Truncating Table:SILVER.erp_PX_CAT_G1V2_stage';
        TRUNCATE TABLE SILVER.erp_PX_CAT_G1V2_stage;
        PRINT '>> Inserting Data Into: SILVER.erp_PX_CAT_G1V2_stage';
        INSERT INTO SILVER.erp_PX_CAT_G1V2_stage
        (ID,CAT,SUBCAT,MAINTENANCE)
        SELECT
        ID,
        CAT,
        SUBCAT,
        MAINTENANCE
        FROM BRONZE.erp_PX_CAT_G1V2_stage
         SET @end_time = GETDATE();
        PRINT '>> Load Duration: '
              + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR)
              + ' seconds';
        PRINT '>> ---------------------------';
    END TRY
           BEGIN CATCH
        PRINT '=========================================================';
        PRINT 'ERROR OCCURRED DURING BRONZE LAYER';
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS VARCHAR(10));
        PRINT '=========================================================';
    END CATCH
END
