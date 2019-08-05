/* sql script to drop schema tpcc and chbanchmark data */

elapsedtime on;

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
DROP SCHEMA htap RESTRICT;

call SYSCS_UTIL.VACUUM();

/* Part 2 - drop usr */ 

call SYSCS_UTIL.SYSCS_DROP_USER('htap');

call SYSCS_UTIL.SYSCS_UPDATE_ALL_SYSTEM_PROCEDURES();
call SYSCS_UTIL.SYSCS_EMPTY_STATEMENT_CACHE();

