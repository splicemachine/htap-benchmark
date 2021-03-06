/* sql script to create schema and load data for tpcc and chbanchmark data for htap-1000 */

elapsedtime on;

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

-- create indexes
CREATE INDEX CUSTOMER_IX_CUSTOMER_NAME ON CUSTOMER(C_W_ID, C_D_ID, C_LAST, C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY,
    C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE);
CREATE INDEX IDX_OORDER ON OORDER (O_W_ID, O_D_ID, O_C_ID, O_ID, O_CARRIER_ID, O_ENTRY_D);
CREATE INDEX OL_I_ID ON ORDER_LINE (OL_W_ID, OL_D_ID, OL_I_ID, OL_O_ID);

/* Part 3 - load data from csvs */

call SYSCS_UTIL.BULK_IMPORT_HFILE (current schema, 'WAREHOUSE',  null, '###SOURCE###/warehouse.csv',  null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false);
call SYSCS_UTIL.BULK_IMPORT_HFILE (current schema, 'STOCK',      null, '###SOURCE###/stock.csv',      null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false);
call SYSCS_UTIL.BULK_IMPORT_HFILE (current schema, 'CUSTOMER',   null, '###SOURCE###/customer.csv',   null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false);
call SYSCS_UTIL.BULK_IMPORT_HFILE (current schema, 'DISTRICT',   null, '###SOURCE###/district.csv',   null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false);
call SYSCS_UTIL.BULK_IMPORT_HFILE (current schema, 'HISTORY',    null, '###SOURCE###/history.csv',    null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false);
call SYSCS_UTIL.BULK_IMPORT_HFILE (current schema, 'ITEM',       null, '###SOURCE###/item.csv',       null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false);
call SYSCS_UTIL.BULK_IMPORT_HFILE (current schema, 'NEW_ORDER',  null, '###SOURCE###/new_order.csv',  null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false);
call SYSCS_UTIL.BULK_IMPORT_HFILE (current schema, 'OORDER',     null, '###SOURCE###/oorder.csv',     null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false);
call SYSCS_UTIL.BULK_IMPORT_HFILE (current schema, 'ORDER_LINE', null, '###SOURCE###/order_line.csv', null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false);
call SYSCS_UTIL.BULK_IMPORT_HFILE (current schema, 'REGION',     null, '###SOURCE###/region.csv',     null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false);
call SYSCS_UTIL.BULK_IMPORT_HFILE (current schema, 'NATION',     null, '###SOURCE###/nation.csv',     null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false);
call SYSCS_UTIL.BULK_IMPORT_HFILE (current schema, 'SUPPLIER',   null, '###SOURCE###/supplier.csv',   null, '"', 'yyyy-MM-dd HH:mm:ss', null, null, 5, '/tmp', true, null, '/tmp/HFILE', false);

/* Part 4 - optimize database */ 

-- perform major compaction
-- call SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_SCHEMA('htap');

-- build query statistics
ANALYZE SCHEMA htap;
