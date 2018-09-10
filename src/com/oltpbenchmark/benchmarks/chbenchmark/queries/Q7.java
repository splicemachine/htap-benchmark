/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;

public class Q7 extends GenericQuery {
	
    public final SQLStmt query_stmt = new SQLStmt(
              "SELECT su_nationkey AS supp_nation, "
            +        "c_nationkey, "
            +        "l_year, "
            +        "sum(cast(ol_amount as decimal(12,2))) AS revenue "
            + "FROM supplier, "
            +      "stock, "
            +      "order_line, "
            +      "(select o_w_id, o_d_id, o_id, o_c_id, year(o_entry_d) as l_year from oorder) oorderyear, "
            +      "customer, "
            +      "nation n1, "
            +      "nation n2 "
            + "WHERE ol_supply_w_id = s_w_id "
            +   "AND ol_i_id = s_i_id "
            +   "AND s_suppkey = su_suppkey "
            +   "AND ol_w_id = o_w_id "
            +   "AND ol_d_id = o_d_id "
            +   "AND ol_o_id = o_id "
            +   "AND c_id = o_c_id "
            +   "AND c_w_id = o_w_id "
            +   "AND c_d_id = o_d_id "
            +   "AND su_nationkey = n1.n_nationkey "
            +   "AND c_nationkey = n2.n_nationkey "
            +   "AND ((n1.n_name = 'Germany' "
            +         "AND n2.n_name = 'Cambodia') "
            +        "OR (n1.n_name = 'Cambodia' "
            +            "AND n2.n_name = 'Germany')) "
            + "GROUP BY su_nationkey, "
            +          "c_nationkey, "
            +          "l_year "
            + "ORDER BY su_nationkey, "
            +          "c_nationkey, "
            +          "l_year"
        );
	
		protected SQLStmt get_query() {
	    return query_stmt;
	}
}
