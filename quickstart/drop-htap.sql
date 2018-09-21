/* sql script to drop schema tpcc and chbanchmark data */

elapsedtime on;

connect 'jdbc:splice://localhost:1527/splicedb;user=splice;password=admin';

/* Part 1 - drop view and tables - will move to oltpbench in the future */ 

set schema htap;

-- temporary table used for processing STOCK data file
DROP VIEW revenue0;

DROP TABLE region;
DROP TABLE nation;
DROP TABLE supplier;

DROP TABLE CUSTOMER;
DROP TABLE DISTRICT;
DROP TABLE HISTORY;
DROP TABLE ITEM;
DROP TABLE NEW_ORDER;
DROP TABLE OORDER;
DROP TABLE ORDER_LINE;
DROP TABLE STOCK;
DROP TABLE WAREHOUSE;

/* Part 2 - drop usr */ 

call SYSCS_UTIL.SYSCS_DROP_USER('htap');
