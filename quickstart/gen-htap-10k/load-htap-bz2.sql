/* sql script to create schema and load data for tpcc and chbanchmark data for htap-25 in bzip2 format */

elapsedtime on;

connect 'jdbc:splice://rodsawsaccount-kxeonb.splicemachine.io:1527/splicedb;ssl=basic;user=splice;password=admin';

/* Part 1 - create usr */ 

call SYSCS_UTIL.SYSCS_CREATE_USER('htap','htapuser');

set schema htap;

/* Part 2 - create tables, indexes, and view via ddl script - will move to oltpbench in the future */ 

CREATE TABLE CUSTOMER (
  C_W_ID INT NOT NULL,
  C_D_ID INT NOT NULL,
  C_ID INT NOT NULL,
  C_DISCOUNT DECIMAL(4,4) NOT NULL,
  C_CREDIT CHAR(2) NOT NULL,
  C_LAST VARCHAR(16) NOT NULL,
  C_FIRST VARCHAR(16) NOT NULL,
  C_CREDIT_LIM DECIMAL(12,2) NOT NULL,
  C_BALANCE DECIMAL(12,2) NOT NULL,
  C_YTD_PAYMENT FLOAT NOT NULL,
  C_PAYMENT_CNT INT NOT NULL,
  C_DELIVERY_CNT INT NOT NULL,
  C_STREET_1 VARCHAR(20) NOT NULL,
  C_STREET_2 VARCHAR(20) NOT NULL,
  C_CITY VARCHAR(20) NOT NULL,
  C_STATE CHAR(2) NOT NULL,
  C_ZIP CHAR(9) NOT NULL,
  C_NATIONKEY INT NOT NULL,
  C_PHONE CHAR(16) NOT NULL,
  C_SINCE TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  C_MIDDLE CHAR(2) NOT NULL,
  C_DATA VARCHAR(500) NOT NULL,
  PRIMARY KEY (C_W_ID,C_D_ID,C_ID)
);

CREATE TABLE DISTRICT (
  D_W_ID INT NOT NULL,
  D_ID INT NOT NULL,
  D_YTD DECIMAL(12,2) NOT NULL,
  D_TAX DECIMAL(4,4) NOT NULL,
  D_NEXT_O_ID INT NOT NULL,
  D_NAME VARCHAR(10) NOT NULL,
  D_STREET_1 VARCHAR(20) NOT NULL,
  D_STREET_2 VARCHAR(20) NOT NULL,
  D_CITY VARCHAR(20) NOT NULL,
  D_STATE CHAR(2) NOT NULL,
  D_ZIP CHAR(9) NOT NULL,
  D_NATIONKEY INT NOT NULL,
  PRIMARY KEY (D_W_ID,D_ID)
);

CREATE TABLE HISTORY (
  H_C_ID INT NOT NULL,
  H_C_D_ID INT NOT NULL,
  H_C_W_ID INT NOT NULL,
  H_D_ID INT NOT NULL,
  H_W_ID INT NOT NULL,
  H_DATE TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  H_AMOUNT DECIMAL(6,2) NOT NULL,
  H_DATA VARCHAR(24) NOT NULL
);

CREATE TABLE ITEM (
  I_ID INT NOT NULL,
  I_NAME VARCHAR(24) NOT NULL,
  I_PRICE DECIMAL(5,2) NOT NULL,
  I_DATA VARCHAR(50) NOT NULL,
  I_IM_ID INT NOT NULL,
  PRIMARY KEY (I_ID)
);

CREATE TABLE NEW_ORDER (
  NO_W_ID INT NOT NULL,
  NO_D_ID INT NOT NULL,
  NO_O_ID INT NOT NULL,
  PRIMARY KEY (NO_W_ID,NO_D_ID,NO_O_ID)
);

CREATE TABLE OORDER (
  O_W_ID INT NOT NULL,
  O_D_ID INT NOT NULL,
  O_ID INT NOT NULL,
  O_C_ID INT NOT NULL,
  O_CARRIER_ID INT DEFAULT NULL,
  O_OL_CNT DECIMAL(2,0) NOT NULL,
  O_ALL_LOCAL DECIMAL(1,0) NOT NULL,
  O_ENTRY_D TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (O_W_ID,O_D_ID,O_ID),
  UNIQUE (O_W_ID,O_D_ID,O_C_ID,O_ID)
);

CREATE TABLE ORDER_LINE (
  OL_W_ID INT NOT NULL,
  OL_D_ID INT NOT NULL,
  OL_O_ID INT NOT NULL,
  OL_NUMBER INT NOT NULL,
  OL_I_ID INT NOT NULL,
  OL_DELIVERY_D TIMESTAMP DEFAULT NULL,
  OL_AMOUNT DECIMAL(6,2) NOT NULL,
  OL_SUPPLY_W_ID INT NOT NULL,
  OL_QUANTITY DECIMAL(2,0) NOT NULL,
  OL_DIST_INFO CHAR(24) NOT NULL,
  PRIMARY KEY (OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER)
);

CREATE TABLE STOCK (
  S_W_ID INT NOT NULL,
  S_I_ID INT NOT NULL,
  S_QUANTITY DECIMAL(4,0) NOT NULL,
  S_YTD DECIMAL(8,2) NOT NULL,
  S_ORDER_CNT INT NOT NULL,
  S_REMOTE_CNT INT NOT NULL,
  S_DATA VARCHAR(50) NOT NULL,
  S_DIST_01 CHAR(24) NOT NULL,
  S_DIST_02 CHAR(24) NOT NULL,
  S_DIST_03 CHAR(24) NOT NULL,
  S_DIST_04 CHAR(24) NOT NULL,
  S_DIST_05 CHAR(24) NOT NULL,
  S_DIST_06 CHAR(24) NOT NULL,
  S_DIST_07 CHAR(24) NOT NULL,
  S_DIST_08 CHAR(24) NOT NULL,
  S_DIST_09 CHAR(24) NOT NULL,
  S_DIST_10 CHAR(24) NOT NULL,
  S_SUPPKEY INT NOT NULL, 
  PRIMARY KEY (S_W_ID,S_I_ID)
);

CREATE TABLE WAREHOUSE (
  W_ID INT NOT NULL,
  W_YTD DECIMAL(12,2) NOT NULL,
  W_TAX DECIMAL(4,4) NOT NULL,
  W_NAME VARCHAR(10) NOT NULL,
  W_STREET_1 VARCHAR(20) NOT NULL,
  W_STREET_2 VARCHAR(20) NOT NULL,
  W_CITY VARCHAR(20) NOT NULL,
  W_STATE CHAR(2) NOT NULL,
  W_ZIP CHAR(9) NOT NULL,
  W_NATIONKEY INT NOT NULL,
  PRIMARY KEY (W_ID)
);

create table region (
   r_regionkey int not null,
   r_name char(55) not null,
   r_comment char(152) not null,
   PRIMARY KEY ( r_regionkey )
);

create table nation (
   n_nationkey int not null,
   n_name char(25) not null,
   n_regionkey int not null,
   n_comment char(152) not null,
   PRIMARY KEY ( n_nationkey )
);

create table supplier (
   su_suppkey int not null,
   su_name char(25) not null,
   su_address varchar(40) not null,
   su_nationkey int not null,
   su_phone char(15) not null,
   su_acctbal numeric(12,2) not null,
   su_comment char(101) not null,
   PRIMARY KEY ( su_suppkey )
);

-- create view for Q15
CREATE view revenue0 (supplier_no, total_revenue) AS 
SELECT supplier_no, sum(cast(ol_amount as decimal(12,2))) as total_revenue 
FROM 
  order_line, 
  (SELECT s_suppkey AS supplier_no, s_i_id, s_w_id FROM stock) stocksupp 
WHERE ol_i_id = s_i_id 
AND ol_supply_w_id = s_w_id 
AND ol_delivery_d >= '2007-01-02 00:00:00.000000' 
GROUP BY supplier_no;


/* Part 3 - load data from csvs */

/* item, region, nation and supplier all in single file */ 

call SYSCS_UTIL.IMPORT_DATA (current schema,'ITEM','I_ID,I_NAME,I_PRICE,I_DATA,I_IM_ID','s3a://splice-benchmark-data/flat/HTAP/htap-10k/01k/item.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'REGION','R_REGIONKEY,R_NAME,R_COMMENT','s3a://splice-benchmark-data/flat/HTAP/htap-10k/01k/region.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'NATION','n_nationkey,n_name,n_regionkey,n_comment','s3a://splice-benchmark-data/flat/HTAP/htap-10k/01k/nation.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'SUPPLIER','su_suppkey,su_name,su_address,su_nationkey,su_phone,su_acctbal,su_comment','s3a://splice-benchmark-data/flat/HTAP/htap-10k/01k/supplier.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

/* htap-1000 - stop at any point for the number of warehouses you want to test */

call SYSCS_UTIL.IMPORT_DATA (current schema,'WAREHOUSE','W_ID,W_YTD,W_TAX,W_NAME,W_STREET_1,W_STREET_2,W_CITY,W_STATE,W_ZIP,W_NATIONKEY','s3a://splice-benchmark-data/flat/HTAP/htap-10k/01k/warehouse.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'STOCK',
   'S_W_ID,S_I_ID,S_QUANTITY,S_YTD,S_ORDER_CNT,S_REMOTE_CNT,S_DATA,S_DIST_01,S_DIST_02,S_DIST_03,S_DIST_04,S_DIST_05,S_DIST_06,S_DIST_07,S_DIST_08,S_DIST_09,S_DIST_10,S_SUPPKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/01k/stock.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'CUSTOMER','C_W_ID,C_D_ID,C_ID,C_DISCOUNT,C_CREDIT,C_LAST,C_FIRST,C_CREDIT_LIM,C_BALANCE,C_YTD_PAYMENT,C_PAYMENT_CNT,C_DELIVERY_CNT,C_STREET_1,C_STREET_2,C_CITY,C_STATE,C_ZIP,C_NATIONKEY,C_PHONE,C_SINCE,C_MIDDLE,C_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/01k/customer.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'DISTRICT',
   'D_W_ID,D_ID,D_YTD,D_TAX,D_NEXT_O_ID,D_NAME,D_STREET_1,D_STREET_2,D_CITY,D_STATE,D_ZIP,D_NATIONKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/01k/district.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'HISTORY','H_C_ID,H_C_D_ID,H_C_W_ID,H_D_ID,H_W_ID,H_DATE,H_AMOUNT,H_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/01k/history.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'NEW_ORDER','NO_W_ID,NO_D_ID,NO_O_ID','s3a://splice-benchmark-data/flat/HTAP/htap-10k/01k/new_order.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'OORDER','O_W_ID,O_D_ID,O_ID,O_C_ID,O_CARRIER_ID,O_OL_CNT,O_ALL_LOCAL,O_ENTRY_D','s3a://splice-benchmark-data/flat/HTAP/htap-10k/01k/oorder.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'ORDER_LINE',
   'OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER,OL_I_ID,OL_DELIVERY_D,OL_AMOUNT,OL_SUPPLY_W_ID,OL_QUANTITY,OL_DIST_INFO',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/01k/order_line.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

/* htap-2000 */

call SYSCS_UTIL.IMPORT_DATA (current schema,'WAREHOUSE','W_ID,W_YTD,W_TAX,W_NAME,W_STREET_1,W_STREET_2,W_CITY,W_STATE,W_ZIP,W_NATIONKEY','s3a://splice-benchmark-data/flat/HTAP/htap-10k/02k/warehouse.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'STOCK',
   'S_W_ID,S_I_ID,S_QUANTITY,S_YTD,S_ORDER_CNT,S_REMOTE_CNT,S_DATA,S_DIST_01,S_DIST_02,S_DIST_03,S_DIST_04,S_DIST_05,S_DIST_06,S_DIST_07,S_DIST_08,S_DIST_09,S_DIST_10,S_SUPPKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/02k/stock.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'CUSTOMER','C_W_ID,C_D_ID,C_ID,C_DISCOUNT,C_CREDIT,C_LAST,C_FIRST,C_CREDIT_LIM,C_BALANCE,C_YTD_PAYMENT,C_PAYMENT_CNT,C_DELIVERY_CNT,C_STREET_1,C_STREET_2,C_CITY,C_STATE,C_ZIP,C_NATIONKEY,C_PHONE,C_SINCE,C_MIDDLE,C_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/02k/customer.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'DISTRICT',
   'D_W_ID,D_ID,D_YTD,D_TAX,D_NEXT_O_ID,D_NAME,D_STREET_1,D_STREET_2,D_CITY,D_STATE,D_ZIP,D_NATIONKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/02k/district.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'HISTORY','H_C_ID,H_C_D_ID,H_C_W_ID,H_D_ID,H_W_ID,H_DATE,H_AMOUNT,H_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/02k/history.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'NEW_ORDER','NO_W_ID,NO_D_ID,NO_O_ID','s3a://splice-benchmark-data/flat/HTAP/htap-10k/02k/new_order.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'OORDER','O_W_ID,O_D_ID,O_ID,O_C_ID,O_CARRIER_ID,O_OL_CNT,O_ALL_LOCAL,O_ENTRY_D','s3a://splice-benchmark-data/flat/HTAP/htap-10k/02k/oorder.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'ORDER_LINE',
   'OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER,OL_I_ID,OL_DELIVERY_D,OL_AMOUNT,OL_SUPPLY_W_ID,OL_QUANTITY,OL_DIST_INFO',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/02k/order_line.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

/* htap-3000 */

call SYSCS_UTIL.IMPORT_DATA (current schema,'WAREHOUSE','W_ID,W_YTD,W_TAX,W_NAME,W_STREET_1,W_STREET_2,W_CITY,W_STATE,W_ZIP,W_NATIONKEY','s3a://splice-benchmark-data/flat/HTAP/htap-10k/03k/warehouse.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'STOCK',
   'S_W_ID,S_I_ID,S_QUANTITY,S_YTD,S_ORDER_CNT,S_REMOTE_CNT,S_DATA,S_DIST_01,S_DIST_02,S_DIST_03,S_DIST_04,S_DIST_05,S_DIST_06,S_DIST_07,S_DIST_08,S_DIST_09,S_DIST_10,S_SUPPKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/03k/stock.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'CUSTOMER','C_W_ID,C_D_ID,C_ID,C_DISCOUNT,C_CREDIT,C_LAST,C_FIRST,C_CREDIT_LIM,C_BALANCE,C_YTD_PAYMENT,C_PAYMENT_CNT,C_DELIVERY_CNT,C_STREET_1,C_STREET_2,C_CITY,C_STATE,C_ZIP,C_NATIONKEY,C_PHONE,C_SINCE,C_MIDDLE,C_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/03k/customer.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'DISTRICT',
   'D_W_ID,D_ID,D_YTD,D_TAX,D_NEXT_O_ID,D_NAME,D_STREET_1,D_STREET_2,D_CITY,D_STATE,D_ZIP,D_NATIONKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/03k/district.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'HISTORY','H_C_ID,H_C_D_ID,H_C_W_ID,H_D_ID,H_W_ID,H_DATE,H_AMOUNT,H_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/03k/history.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'NEW_ORDER','NO_W_ID,NO_D_ID,NO_O_ID','s3a://splice-benchmark-data/flat/HTAP/htap-10k/03k/new_order.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'OORDER','O_W_ID,O_D_ID,O_ID,O_C_ID,O_CARRIER_ID,O_OL_CNT,O_ALL_LOCAL,O_ENTRY_D','s3a://splice-benchmark-data/flat/HTAP/htap-10k/03k/oorder.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'ORDER_LINE',
   'OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER,OL_I_ID,OL_DELIVERY_D,OL_AMOUNT,OL_SUPPLY_W_ID,OL_QUANTITY,OL_DIST_INFO',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/03k/order_line.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

/* htap-4000 */

call SYSCS_UTIL.IMPORT_DATA (current schema,'WAREHOUSE','W_ID,W_YTD,W_TAX,W_NAME,W_STREET_1,W_STREET_2,W_CITY,W_STATE,W_ZIP,W_NATIONKEY','s3a://splice-benchmark-data/flat/HTAP/htap-10k/04k/warehouse.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'STOCK',
   'S_W_ID,S_I_ID,S_QUANTITY,S_YTD,S_ORDER_CNT,S_REMOTE_CNT,S_DATA,S_DIST_01,S_DIST_02,S_DIST_03,S_DIST_04,S_DIST_05,S_DIST_06,S_DIST_07,S_DIST_08,S_DIST_09,S_DIST_10,S_SUPPKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/04k/stock.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'CUSTOMER','C_W_ID,C_D_ID,C_ID,C_DISCOUNT,C_CREDIT,C_LAST,C_FIRST,C_CREDIT_LIM,C_BALANCE,C_YTD_PAYMENT,C_PAYMENT_CNT,C_DELIVERY_CNT,C_STREET_1,C_STREET_2,C_CITY,C_STATE,C_ZIP,C_NATIONKEY,C_PHONE,C_SINCE,C_MIDDLE,C_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/04k/customer.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'DISTRICT',
   'D_W_ID,D_ID,D_YTD,D_TAX,D_NEXT_O_ID,D_NAME,D_STREET_1,D_STREET_2,D_CITY,D_STATE,D_ZIP,D_NATIONKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/04k/district.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'HISTORY','H_C_ID,H_C_D_ID,H_C_W_ID,H_D_ID,H_W_ID,H_DATE,H_AMOUNT,H_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/04k/history.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'NEW_ORDER','NO_W_ID,NO_D_ID,NO_O_ID','s3a://splice-benchmark-data/flat/HTAP/htap-10k/04k/new_order.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'OORDER','O_W_ID,O_D_ID,O_ID,O_C_ID,O_CARRIER_ID,O_OL_CNT,O_ALL_LOCAL,O_ENTRY_D','s3a://splice-benchmark-data/flat/HTAP/htap-10k/04k/oorder.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'ORDER_LINE',
   'OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER,OL_I_ID,OL_DELIVERY_D,OL_AMOUNT,OL_SUPPLY_W_ID,OL_QUANTITY,OL_DIST_INFO',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/04k/order_line.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

/* htap-5000 */

call SYSCS_UTIL.IMPORT_DATA (current schema,'WAREHOUSE','W_ID,W_YTD,W_TAX,W_NAME,W_STREET_1,W_STREET_2,W_CITY,W_STATE,W_ZIP,W_NATIONKEY','s3a://splice-benchmark-data/flat/HTAP/htap-10k/05k/warehouse.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'STOCK',
   'S_W_ID,S_I_ID,S_QUANTITY,S_YTD,S_ORDER_CNT,S_REMOTE_CNT,S_DATA,S_DIST_01,S_DIST_02,S_DIST_03,S_DIST_04,S_DIST_05,S_DIST_06,S_DIST_07,S_DIST_08,S_DIST_09,S_DIST_10,S_SUPPKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/05k/stock.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'CUSTOMER','C_W_ID,C_D_ID,C_ID,C_DISCOUNT,C_CREDIT,C_LAST,C_FIRST,C_CREDIT_LIM,C_BALANCE,C_YTD_PAYMENT,C_PAYMENT_CNT,C_DELIVERY_CNT,C_STREET_1,C_STREET_2,C_CITY,C_STATE,C_ZIP,C_NATIONKEY,C_PHONE,C_SINCE,C_MIDDLE,C_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/05k/customer.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'DISTRICT',
   'D_W_ID,D_ID,D_YTD,D_TAX,D_NEXT_O_ID,D_NAME,D_STREET_1,D_STREET_2,D_CITY,D_STATE,D_ZIP,D_NATIONKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/05k/district.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'HISTORY','H_C_ID,H_C_D_ID,H_C_W_ID,H_D_ID,H_W_ID,H_DATE,H_AMOUNT,H_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/05k/history.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'NEW_ORDER','NO_W_ID,NO_D_ID,NO_O_ID','s3a://splice-benchmark-data/flat/HTAP/htap-10k/05k/new_order.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'OORDER','O_W_ID,O_D_ID,O_ID,O_C_ID,O_CARRIER_ID,O_OL_CNT,O_ALL_LOCAL,O_ENTRY_D','s3a://splice-benchmark-data/flat/HTAP/htap-10k/05k/oorder.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'ORDER_LINE',
   'OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER,OL_I_ID,OL_DELIVERY_D,OL_AMOUNT,OL_SUPPLY_W_ID,OL_QUANTITY,OL_DIST_INFO',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/05k/order_line.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

/* htap-6000 */

call SYSCS_UTIL.IMPORT_DATA (current schema,'WAREHOUSE','W_ID,W_YTD,W_TAX,W_NAME,W_STREET_1,W_STREET_2,W_CITY,W_STATE,W_ZIP,W_NATIONKEY','s3a://splice-benchmark-data/flat/HTAP/htap-10k/06k/warehouse.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'STOCK',
   'S_W_ID,S_I_ID,S_QUANTITY,S_YTD,S_ORDER_CNT,S_REMOTE_CNT,S_DATA,S_DIST_01,S_DIST_02,S_DIST_03,S_DIST_04,S_DIST_05,S_DIST_06,S_DIST_07,S_DIST_08,S_DIST_09,S_DIST_10,S_SUPPKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/06k/stock.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'CUSTOMER','C_W_ID,C_D_ID,C_ID,C_DISCOUNT,C_CREDIT,C_LAST,C_FIRST,C_CREDIT_LIM,C_BALANCE,C_YTD_PAYMENT,C_PAYMENT_CNT,C_DELIVERY_CNT,C_STREET_1,C_STREET_2,C_CITY,C_STATE,C_ZIP,C_NATIONKEY,C_PHONE,C_SINCE,C_MIDDLE,C_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/06k/customer.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'DISTRICT',
   'D_W_ID,D_ID,D_YTD,D_TAX,D_NEXT_O_ID,D_NAME,D_STREET_1,D_STREET_2,D_CITY,D_STATE,D_ZIP,D_NATIONKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/06k/district.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'HISTORY','H_C_ID,H_C_D_ID,H_C_W_ID,H_D_ID,H_W_ID,H_DATE,H_AMOUNT,H_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/06k/history.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'NEW_ORDER','NO_W_ID,NO_D_ID,NO_O_ID','s3a://splice-benchmark-data/flat/HTAP/htap-10k/06k/new_order.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'OORDER','O_W_ID,O_D_ID,O_ID,O_C_ID,O_CARRIER_ID,O_OL_CNT,O_ALL_LOCAL,O_ENTRY_D','s3a://splice-benchmark-data/flat/HTAP/htap-10k/06k/oorder.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'ORDER_LINE',
   'OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER,OL_I_ID,OL_DELIVERY_D,OL_AMOUNT,OL_SUPPLY_W_ID,OL_QUANTITY,OL_DIST_INFO',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/06k/order_line.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

/* htap-7000 */

call SYSCS_UTIL.IMPORT_DATA (current schema,'WAREHOUSE','W_ID,W_YTD,W_TAX,W_NAME,W_STREET_1,W_STREET_2,W_CITY,W_STATE,W_ZIP,W_NATIONKEY','s3a://splice-benchmark-data/flat/HTAP/htap-10k/07k/warehouse.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'STOCK',
   'S_W_ID,S_I_ID,S_QUANTITY,S_YTD,S_ORDER_CNT,S_REMOTE_CNT,S_DATA,S_DIST_01,S_DIST_02,S_DIST_03,S_DIST_04,S_DIST_05,S_DIST_06,S_DIST_07,S_DIST_08,S_DIST_09,S_DIST_10,S_SUPPKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/07k/stock.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'CUSTOMER','C_W_ID,C_D_ID,C_ID,C_DISCOUNT,C_CREDIT,C_LAST,C_FIRST,C_CREDIT_LIM,C_BALANCE,C_YTD_PAYMENT,C_PAYMENT_CNT,C_DELIVERY_CNT,C_STREET_1,C_STREET_2,C_CITY,C_STATE,C_ZIP,C_NATIONKEY,C_PHONE,C_SINCE,C_MIDDLE,C_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/07k/customer.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'DISTRICT',
   'D_W_ID,D_ID,D_YTD,D_TAX,D_NEXT_O_ID,D_NAME,D_STREET_1,D_STREET_2,D_CITY,D_STATE,D_ZIP,D_NATIONKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/07k/district.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'HISTORY','H_C_ID,H_C_D_ID,H_C_W_ID,H_D_ID,H_W_ID,H_DATE,H_AMOUNT,H_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/07k/history.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'NEW_ORDER','NO_W_ID,NO_D_ID,NO_O_ID','s3a://splice-benchmark-data/flat/HTAP/htap-10k/07k/new_order.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'OORDER','O_W_ID,O_D_ID,O_ID,O_C_ID,O_CARRIER_ID,O_OL_CNT,O_ALL_LOCAL,O_ENTRY_D','s3a://splice-benchmark-data/flat/HTAP/htap-10k/07k/oorder.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'ORDER_LINE',
   'OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER,OL_I_ID,OL_DELIVERY_D,OL_AMOUNT,OL_SUPPLY_W_ID,OL_QUANTITY,OL_DIST_INFO',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/07k/order_line.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

/* htap-8000 */

call SYSCS_UTIL.IMPORT_DATA (current schema,'WAREHOUSE','W_ID,W_YTD,W_TAX,W_NAME,W_STREET_1,W_STREET_2,W_CITY,W_STATE,W_ZIP,W_NATIONKEY','s3a://splice-benchmark-data/flat/HTAP/htap-10k/08k/warehouse.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'STOCK',
   'S_W_ID,S_I_ID,S_QUANTITY,S_YTD,S_ORDER_CNT,S_REMOTE_CNT,S_DATA,S_DIST_01,S_DIST_02,S_DIST_03,S_DIST_04,S_DIST_05,S_DIST_06,S_DIST_07,S_DIST_08,S_DIST_09,S_DIST_10,S_SUPPKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/08k/stock.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'CUSTOMER','C_W_ID,C_D_ID,C_ID,C_DISCOUNT,C_CREDIT,C_LAST,C_FIRST,C_CREDIT_LIM,C_BALANCE,C_YTD_PAYMENT,C_PAYMENT_CNT,C_DELIVERY_CNT,C_STREET_1,C_STREET_2,C_CITY,C_STATE,C_ZIP,C_NATIONKEY,C_PHONE,C_SINCE,C_MIDDLE,C_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/08k/customer.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'DISTRICT',
   'D_W_ID,D_ID,D_YTD,D_TAX,D_NEXT_O_ID,D_NAME,D_STREET_1,D_STREET_2,D_CITY,D_STATE,D_ZIP,D_NATIONKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/08k/district.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'HISTORY','H_C_ID,H_C_D_ID,H_C_W_ID,H_D_ID,H_W_ID,H_DATE,H_AMOUNT,H_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/08k/history.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'NEW_ORDER','NO_W_ID,NO_D_ID,NO_O_ID','s3a://splice-benchmark-data/flat/HTAP/htap-10k/08k/new_order.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'OORDER','O_W_ID,O_D_ID,O_ID,O_C_ID,O_CARRIER_ID,O_OL_CNT,O_ALL_LOCAL,O_ENTRY_D','s3a://splice-benchmark-data/flat/HTAP/htap-10k/08k/oorder.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'ORDER_LINE',
   'OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER,OL_I_ID,OL_DELIVERY_D,OL_AMOUNT,OL_SUPPLY_W_ID,OL_QUANTITY,OL_DIST_INFO',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/08k/order_line.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

/* htap-9000 */

call SYSCS_UTIL.IMPORT_DATA (current schema,'WAREHOUSE','W_ID,W_YTD,W_TAX,W_NAME,W_STREET_1,W_STREET_2,W_CITY,W_STATE,W_ZIP,W_NATIONKEY','s3a://splice-benchmark-data/flat/HTAP/htap-10k/09k/warehouse.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'STOCK',
   'S_W_ID,S_I_ID,S_QUANTITY,S_YTD,S_ORDER_CNT,S_REMOTE_CNT,S_DATA,S_DIST_01,S_DIST_02,S_DIST_03,S_DIST_04,S_DIST_05,S_DIST_06,S_DIST_07,S_DIST_08,S_DIST_09,S_DIST_10,S_SUPPKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/09k/stock.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'CUSTOMER','C_W_ID,C_D_ID,C_ID,C_DISCOUNT,C_CREDIT,C_LAST,C_FIRST,C_CREDIT_LIM,C_BALANCE,C_YTD_PAYMENT,C_PAYMENT_CNT,C_DELIVERY_CNT,C_STREET_1,C_STREET_2,C_CITY,C_STATE,C_ZIP,C_NATIONKEY,C_PHONE,C_SINCE,C_MIDDLE,C_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/09k/customer.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'DISTRICT',
   'D_W_ID,D_ID,D_YTD,D_TAX,D_NEXT_O_ID,D_NAME,D_STREET_1,D_STREET_2,D_CITY,D_STATE,D_ZIP,D_NATIONKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/09k/district.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'HISTORY','H_C_ID,H_C_D_ID,H_C_W_ID,H_D_ID,H_W_ID,H_DATE,H_AMOUNT,H_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/09k/history.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'NEW_ORDER','NO_W_ID,NO_D_ID,NO_O_ID','s3a://splice-benchmark-data/flat/HTAP/htap-10k/09k/new_order.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'OORDER','O_W_ID,O_D_ID,O_ID,O_C_ID,O_CARRIER_ID,O_OL_CNT,O_ALL_LOCAL,O_ENTRY_D','s3a://splice-benchmark-data/flat/HTAP/htap-10k/09k/oorder.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'ORDER_LINE',
   'OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER,OL_I_ID,OL_DELIVERY_D,OL_AMOUNT,OL_SUPPLY_W_ID,OL_QUANTITY,OL_DIST_INFO',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/09k/order_line.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

/* htap-10000 */

call SYSCS_UTIL.IMPORT_DATA (current schema,'WAREHOUSE','W_ID,W_YTD,W_TAX,W_NAME,W_STREET_1,W_STREET_2,W_CITY,W_STATE,W_ZIP,W_NATIONKEY','s3a://splice-benchmark-data/flat/HTAP/htap-10k/10k/warehouse.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'STOCK',
   'S_W_ID,S_I_ID,S_QUANTITY,S_YTD,S_ORDER_CNT,S_REMOTE_CNT,S_DATA,S_DIST_01,S_DIST_02,S_DIST_03,S_DIST_04,S_DIST_05,S_DIST_06,S_DIST_07,S_DIST_08,S_DIST_09,S_DIST_10,S_SUPPKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/10k/stock.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'CUSTOMER','C_W_ID,C_D_ID,C_ID,C_DISCOUNT,C_CREDIT,C_LAST,C_FIRST,C_CREDIT_LIM,C_BALANCE,C_YTD_PAYMENT,C_PAYMENT_CNT,C_DELIVERY_CNT,C_STREET_1,C_STREET_2,C_CITY,C_STATE,C_ZIP,C_NATIONKEY,C_PHONE,C_SINCE,C_MIDDLE,C_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/10k/customer.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'DISTRICT',
   'D_W_ID,D_ID,D_YTD,D_TAX,D_NEXT_O_ID,D_NAME,D_STREET_1,D_STREET_2,D_CITY,D_STATE,D_ZIP,D_NATIONKEY',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/10k/district.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'HISTORY','H_C_ID,H_C_D_ID,H_C_W_ID,H_D_ID,H_W_ID,H_DATE,H_AMOUNT,H_DATA','s3a://splice-benchmark-data/flat/HTAP/htap-10k/10k/history.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'NEW_ORDER','NO_W_ID,NO_D_ID,NO_O_ID','s3a://splice-benchmark-data/flat/HTAP/htap-10k/10k/new_order.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'OORDER','O_W_ID,O_D_ID,O_ID,O_C_ID,O_CARRIER_ID,O_OL_CNT,O_ALL_LOCAL,O_ENTRY_D','s3a://splice-benchmark-data/flat/HTAP/htap-10k/10k/oorder.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

call SYSCS_UTIL.IMPORT_DATA (current schema,'ORDER_LINE',
   'OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER,OL_I_ID,OL_DELIVERY_D,OL_AMOUNT,OL_SUPPLY_W_ID,OL_QUANTITY,OL_DIST_INFO',
   's3a://splice-benchmark-data/flat/HTAP/htap-10k/10k/order_line.csv.bz2', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null);

/* Part 4 - optimize database */ 

-- create indexes
CREATE INDEX IDX_CUSTOMER_NAME ON CUSTOMER (C_W_ID,C_D_ID,C_LAST,C_FIRST);

CREATE INDEX STOCK_SUPPKEY ON STOCK (S_SUPPKEY, S_I_ID, S_W_ID, S_ORDER_CNT);
CREATE INDEX STOCK_I_ID ON STOCK (S_I_ID, S_SUPPKEY, S_W_ID, S_QUANTITY);
CREATE INDEX SUPPLIER_SUPPKEY ON SUPPLIER (SU_SUPPKEY, su_nationkey);
CREATE INDEX ITEM_ID ON ITEM (I_ID, I_DATA);
CREATE INDEX OL_I_ID ON ORDER_LINE (OL_I_ID, OL_O_ID, OL_W_ID, OL_D_ID);

-- perform major compaction
call SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_SCHEMA(current schema);

-- build query statistics
ANALYZE SCHEMA current schema;
