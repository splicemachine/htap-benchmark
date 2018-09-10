DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS supplier;

create table region (
   r_regionkey int not null,
   r_name char(55) not null,
   r_comment char(152) not null,
   PRIMARY KEY ( r_regionkey )
);

create table nation (
   n_nationkey int not null,
   n_name char(25) not null,
--   n_regionkey int not null references region(r_regionkey) ON DELETE CASCADE, -- needs foreign keys defn'd
   n_regionkey int not null,
   n_comment char(152) not null,
   PRIMARY KEY ( n_nationkey )
);

create table supplier (
   su_suppkey int not null,
   su_name char(25) not null,
   su_address varchar(40) not null,
--   su_nationkey int not null references nation(n_nationkey)  ON DELETE CASCADE, -- needs foreign keys defn'd
   su_nationkey int not null,
   su_phone char(15) not null,
   su_acctbal numeric(12,2) not null,
   su_comment char(101) not null,
   PRIMARY KEY ( su_suppkey )
);

-- DROP VIEW revenue0; -- Splice doesn't support IF EXISTS on drop view
CREATE view revenue0 (supplier_no, total_revenue) AS 
SELECT supplier_no, sum(cast(ol_amount as decimal(12,2))) as total_revenue 
FROM 
	order_line, 
	(SELECT s_suppkey AS supplier_no, s_i_id, s_w_id FROM stock) stocksupp 
WHERE ol_i_id = s_i_id 
AND ol_supply_w_id = s_w_id 
AND ol_delivery_d >= '2007-01-02 00:00:00.000000' 
GROUP BY 
supplier_no;
