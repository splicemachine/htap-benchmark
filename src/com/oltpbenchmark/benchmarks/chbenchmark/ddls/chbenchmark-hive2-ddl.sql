/*

Hive2 does not currently support constraints on fields (e.g. not null and primary key)

comments inside sql statements causing problems for Hive so removed ... 

*/

DROP TABLE IF EXISTS REGION;
DROP TABLE IF EXISTS NATION;
DROP TABLE IF EXISTS SUPPLIER;

CREATE TABLE REGION (
   R_REGIONKEY INT,  
   R_NAME CHAR(55), 
   R_COMMENT CHAR(152) 
);

CREATE TABLE NATION (
   N_NATIONKEY INT, 
   N_NAME CHAR(25), 
   N_REGIONKEY INT, 
   N_COMMENT CHAR(152) 
);

CREATE TABLE SUPPLIER (
   SU_SUPPKEY INT, 
   SU_NAME CHAR(25), 
   SU_ADDRESS VARCHAR(40), 
   SU_NATIONKEY INT, 
   SU_PHONE CHAR(15), 
   SU_ACCTBAL DECIMAL(12,2), 
   SU_COMMENT CHAR(101) 
);

DROP VIEW IF EXISTS REVENUE0;

CREATE VIEW REVENUE0 (SUPPLIER_NO, TOTAL_REVENUE) AS 
SELECT SUPPLIER_NO, SUM(OL_AMOUNT) AS TOTAL_REVENUE 
FROM ORDER_LINE, (SELECT CAST(PMOD((S_W_ID * S_I_ID),10000) AS BIGINT) AS SUPPLIER_NO, S_I_ID, S_W_ID FROM STOCK) STOCKSUPP 
WHERE OL_I_ID = S_I_ID 
AND OL_SUPPLY_W_ID = S_W_ID 
AND OL_DELIVERY_D >= '2007-01-02 00:00:00.000000' 
GROUP BY SUPPLIER_NO;