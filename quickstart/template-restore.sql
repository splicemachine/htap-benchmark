/* sql script to restore schema from a backup */

elapsedtime on;

call SYSCS_UTIL.SYSCS_CREATE_USER('htap','htapuser');
call SYSCS_UTIL.SYSCS_UPDATE_SCHEMA_OWNER('htap', 'htap');
call SYSCS_UTIL.SYSCS_RESTORE_SCHEMA('htap', 'htap', '###SOURCE###', ###BACKUPID###, false);

-- create view for Q15
set schema htap;
CREATE view revenue0 (supplier_no, total_revenue) AS 
SELECT supplier_no, sum(cast(ol_amount as decimal(12,2))) as total_revenue 
FROM 
  order_line, 
  (SELECT s_suppkey AS supplier_no, s_i_id, s_w_id FROM stock) stocksupp 
WHERE ol_i_id = s_i_id 
AND ol_supply_w_id = s_w_id 
AND ol_delivery_d >= '2007-01-02 00:00:00.000000' 
GROUP BY supplier_no;

