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

public class Q11 extends GenericQuery {
	
    public final SQLStmt query_stmt = new SQLStmt(
              "SELECT s_i_id, "
            +        "sum(cast(s_order_cnt as decimal(12,0))) AS ordercount "
            + "FROM stock, "
            +      "supplier, "
            +      "nation "
            + "WHERE s_suppkey = su_suppkey "
            +   "AND su_nationkey = n_nationkey "
            +   "AND n_name = 'Germany' "
            + "GROUP BY s_i_id HAVING sum(cast(s_order_cnt as decimal(12,0))) > "
            +   "(SELECT sum(cast(s_order_cnt as decimal(12,0))) * .005 "
            +    "FROM stock, "
            +         "supplier, "
            +         "nation "
            +    "WHERE s_suppkey = su_suppkey "
            +      "AND su_nationkey = n_nationkey "
            +      "AND n_name = 'Germany') "
            + "ORDER BY ordercount DESC"
        );
	
		protected SQLStmt get_query() {
	    return query_stmt;
	}
}
