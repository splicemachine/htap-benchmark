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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

public class Q15 extends GenericQuery {
	
    private final SQLStmt query_stmt = new SQLStmt (
              "SELECT su_suppkey, "
            +        "su_name, "
            +        "su_address, "
            +        "su_phone, "
            +        "total_revenue "
            + "FROM supplier, revenue0 "
            + "WHERE su_suppkey = supplier_no "
            +     "AND total_revenue = (select max(total_revenue) from revenue0) "
            + "ORDER BY su_suppkey"
        );
		
    protected SQLStmt get_query() {
	    return query_stmt;
	}

}
