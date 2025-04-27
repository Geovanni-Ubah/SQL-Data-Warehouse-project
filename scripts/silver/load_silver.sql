
/*
This is the queries that insert data from the Bronze layer. this are all data transformations and cleansing done from data 
in the raw tables from the bronze layer.

  */

INSERT INTO silver.crm_cust_info (
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
CASE LOWER(TRIM(cst_material_status))
	WHEN 'M' THEN 'Married'
	WHEN 'S' THEN 'Single'
	ELSE 'n/a' END AS cst_marital_status,
CASE LOWER(TRIM(cst_gndr))
	WHEN 'f' THEN 'Female'
	WHEN 'm' THEN 'Male'
	ELSE 'n/a' END AS cst_gndr,
cst_create_date

FROM (
	SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_latest
	FROM bronze.crm_cust_info
	WHERE cst_id  IS NOT NULL
	) greg
WHERE flag_latest = 1;


INSERT INTO silver.crm_prd_info (
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
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') as Cat_id,
	SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
    prd_nm,          
   ISNULL(prd_cost,0) AS prd_cost,       
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
	ELSE 'n/a'  END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) 
			OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC) -1 
		AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;

 INSERT INTO silver.crm_sales_details(
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_quantity,
    sls_price,
	sls_sales
 )
  
  SELECT
  sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE) END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE) END AS sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE) END AS sls_due_dt,
    sls_quantity,
	CASE WHEN sls_price IS NULL OR  sls_price <= 0
			THEN sls_sales/NULLIF(sls_quantity,0)
			ELSE sls_price END AS sls_price,
	CASE WHEN sls_sales IS NULL OR  sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price) ELSE sls_price END AS sls_sales
FROM bronze.crm_sales_details;

INSERT INTO  silver.erp_cust_az12(cid,bdate,gen)
SELECT 
   CASE WHEN cid  LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(CID))
   ELSE cid END AS cid, --remove NAS prefix if present
   CASE WHEN bdate > GETDATE() THEN NULL
		ELSE bdate END AS bdate, --set futures birthdates to NULL
    CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN  ('M','MALE') THEN 'Male'
		 ELSE 'n/a' END AS gen --normalize gender values and handle unknown cases
FROM bronze.erp_cust_az12;

INSERT INTO silver.erp_loc_a101(cid,cntry)
SELECT
	REPLACE(cid,'-','') AS cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		 ELSE TRIM(cntry) END cntry --normalize and handle missing or blank country codes
FROM bronze.erp_loc_a101;


INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat,maintenance)
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;


