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

package com.oltpbenchmark.benchmarks.chbenchmark;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.benchmarks.chbenchmark.pojo.Nation;
import com.oltpbenchmark.benchmarks.chbenchmark.pojo.Region;
import com.oltpbenchmark.benchmarks.chbenchmark.pojo.Supplier;
import com.oltpbenchmark.util.RandomGenerator;

public class CHBenCHmarkLoader extends Loader<CHBenCHmark> {
	private static final Logger LOG = Logger.getLogger(CHBenCHmarkLoader.class);
	
	private final static int configCommitCount = 1000; // commit every n records
	private static final RandomGenerator ran = new RandomGenerator(0);
	private static PreparedStatement regionPrepStmt;
	private static PreparedStatement nationPrepStmt;
	private static PreparedStatement supplierPrepStmt;
	
	private static Date now;
	private static long lastTimeMS;
	private static Connection conn;
	
	//create possible keys for n_nationkey ([a-zA-Z0-9])
	private static final int[] nationkeys = new int[62];
	static {
	    for (char i = 0; i < 10; i++) {
	        nationkeys[i] = (char)('0') + i;
	    }
	    for (char i = 0; i < 26; i++) {
	        nationkeys[i + 10] = (char)('A') + i;
	    }
	    for (char i = 0; i < 26; i++) {
            nationkeys[i + 36] = (char)('a') + i;
        }
	}
	
	public CHBenCHmarkLoader(CHBenCHmark benchmark, Connection c) {
		super(benchmark, c);
		conn =c;
	}
	
	@Override
	public List<LoaderThread> createLoaderThreads() throws SQLException {
	    // TODO Auto-generated method stub
	    return null;
	}

	public void load() throws SQLException {
		if (startId == 1) {
			try {
				regionPrepStmt = conn.prepareStatement("INSERT INTO region "
						+ " (r_regionkey, r_name, r_comment) "
						+ "VALUES (?, ?, ?)");
				
				nationPrepStmt = conn.prepareStatement("INSERT INTO nation "
						+ " (n_nationkey, n_name, n_regionkey, n_comment) "
						+ "VALUES (?, ?, ?, ?)");
				
				supplierPrepStmt = conn.prepareStatement("INSERT INTO supplier "
						+ " (su_suppkey, su_name, su_address, su_nationkey, su_phone, su_acctbal, su_comment) "
						+ "VALUES (?, ?, ?, ?, ?, ?, ?)");

			} catch (SQLException se) {
				LOG.debug(se.getMessage());
				conn.rollback();

			} catch (Exception e) {
				e.printStackTrace(System.err);
				conn.rollback();

			} // end try
			
			loadHelper();
			conn.commit();
		}
	}
	
   static void truncateTable(String strTable) throws SQLException {

        LOG.debug("Truncating '" + strTable + "' ...");
        try {
            conn.createStatement().execute("DELETE FROM " + strTable);
            conn.commit();
        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            conn.rollback();
        }
   }
	
	protected int loadRegions() throws SQLException {
		
		int k = 0;
		int t = 0;
		BufferedReader br = null;
        PrintWriter regionPrntWr = null;
		
        if (fileLocation != null) {
			try { 
				regionPrntWr = new PrintWriter(fileLocation + "/" + "region.csv");
			} catch (FileNotFoundException fnfe) { 
				LOG.error(fnfe); 
				System.exit(1); 
			} 
        }

		try {
		    
		    truncateTable("region");
		    truncateTable("nation");
		    truncateTable("supplier");

			now = new java.util.Date();
			LOG.debug("\nStart Region Load @ " + now
					+ " ...");

			Region region = new Region();
			
			File file = new File("src", "com/oltpbenchmark/benchmarks/chbenchmark/region_gen.tbl");
			br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while (line != null) {
				StringTokenizer st = new StringTokenizer(line, "|");
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				region.r_regionkey = Integer.parseInt(st.nextToken());
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				region.r_name = st.nextToken();
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				region.r_comment = st.nextToken();
				if (st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }

				k++;

				if (regionPrntWr == null) {
					regionPrepStmt.setLong(1, region.r_regionkey);
					regionPrepStmt.setString(2, region.r_name);
					regionPrepStmt.setString(3, region.r_comment);
					regionPrepStmt.addBatch();
				} else {
					regionPrntWr.printf("%d,", region.r_regionkey);
					regionPrntWr.printf("%s,", region.r_name);
					regionPrntWr.printf("%s", region.r_comment);
					regionPrntWr.println();
				}

				long tmpTime = new java.util.Date().getTime();
				String etStr = "  Elasped Time(ms): "
						+ ((tmpTime - lastTimeMS) / 1000.000)
						+ "                    ";
				LOG.debug(etStr.substring(0, 30)
						+ "  Writing record " + k + " of " + t);
				lastTimeMS = tmpTime;
				if (regionPrntWr == null) {
					regionPrepStmt.executeBatch();
					regionPrepStmt.clearBatch();
					conn.commit();
				} else {
					regionPrntWr.flush();
				}
				line = br.readLine();
			}

			long tmpTime = new java.util.Date().getTime();
			String etStr = "  Elasped Time(ms): "
					+ ((tmpTime - lastTimeMS) / 1000.000)
					+ "                    ";
			LOG.debug(etStr.substring(0, 30) + "  Writing record " + k
					+ " of " + t);
			lastTimeMS = tmpTime;

			if (regionPrntWr == null) {
				// nothing to do since batch already executed and committed
			} else {
				regionPrntWr.close();
			}
			now = new java.util.Date();
			LOG.debug("End Region Load @  " + now);

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			conn.rollback();
		
		} catch (FileNotFoundException e) {
		    e.printStackTrace(System.err);
		}  catch (Exception e) {
            e.printStackTrace(System.err);
            conn.rollback();
		} finally {
		    if (br != null){
		        try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace(System.err);
                }
		    }
		}

		return (k);

	} // end loadRegions()
	
	protected int loadNations() throws SQLException {
		
		int k = 0;
		int t = 0;
		BufferedReader br = null;
        PrintWriter nationPrntWr = null;
		
        if (fileLocation != null) {
			try { 
				nationPrntWr = new PrintWriter(fileLocation + "/" + "nation.csv");
			} catch (FileNotFoundException fnfe) { 
				LOG.error(fnfe); 
				System.exit(1); 
			} 
        }
		
		try {

			now = new java.util.Date();
			LOG.debug("\nStart Nation Load @ " + now
					+ " ...");

			Nation nation = new Nation();
			
			File file = new File("src", "com/oltpbenchmark/benchmarks/chbenchmark/nation_gen.tbl");
			br = new BufferedReader(new FileReader(file));
			String line = br.readLine();
			while (line != null) {
				StringTokenizer st = new StringTokenizer(line, "|");
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				nation.n_nationkey = Integer.parseInt(st.nextToken());
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				nation.n_name = st.nextToken();
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				nation.n_regionkey = Integer.parseInt(st.nextToken());
				if (!st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }
				nation.n_comment = st.nextToken();
				if (st.hasMoreTokens()) { LOG.error("invalid input file: " + file.getAbsolutePath()); }

				k++;

				if (nationPrntWr == null) {
					nationPrepStmt.setLong(1, nation.n_nationkey);
					nationPrepStmt.setString(2, nation.n_name);
					nationPrepStmt.setLong(3, nation.n_regionkey);
					nationPrepStmt.setString(4, nation.n_comment);
					nationPrepStmt.addBatch();
				} else {
					nationPrntWr.printf("%d,", nation.n_nationkey);
					nationPrntWr.printf("%s,", nation.n_name);
					nationPrntWr.printf("%d,", nation.n_regionkey);
					nationPrntWr.printf("%s", nation.n_comment);
					nationPrntWr.println();
				}

				long tmpTime = new java.util.Date().getTime();
				String etStr = "  Elasped Time(ms): "
						+ ((tmpTime - lastTimeMS) / 1000.000)
						+ "                    ";
				LOG.debug(etStr.substring(0, 30)
						+ "  Writing record " + k + " of " + t);
				lastTimeMS = tmpTime;
				if (nationPrntWr == null) {
					nationPrepStmt.executeBatch();
					nationPrepStmt.clearBatch();
					conn.commit();
				} else {
					nationPrntWr.flush();
				}
				line = br.readLine();
			}

			long tmpTime = new java.util.Date().getTime();
			String etStr = "  Elasped Time(ms): "
					+ ((tmpTime - lastTimeMS) / 1000.000)
					+ "                    ";
			LOG.debug(etStr.substring(0, 30) + "  Writing record " + k
					+ " of " + t);
			lastTimeMS = tmpTime;

			if (nationPrntWr == null) {
				// nothing to do since batch already executed and committed
			} else {
				nationPrntWr.close();
			}
			now = new java.util.Date();
			LOG.debug("End Region Load @  " + now);

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			conn.rollback();
		} catch (FileNotFoundException e) {
            e.printStackTrace(System.err);
        }  catch (Exception e) {
            e.printStackTrace(System.err);
            conn.rollback();
        } finally {
            if (br != null){
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace(System.err);
                }
            }
        }

		return (k);

	} // end loadNations()
	
	protected int loadSuppliers() throws SQLException {
		
		int k = 0;
		int t = 0;
        PrintWriter supplierPrntWr = null;
		
        if (fileLocation != null) {
			try { 
				supplierPrntWr = new PrintWriter(fileLocation + "/" + "supplier.csv");
			} catch (FileNotFoundException fnfe) { 
				LOG.error(fnfe); 
				System.exit(1); 
			} 
        }
		
		try {

			now = new java.util.Date();
			LOG.debug("\nStart Supplier Load @ " + now
					+ " ...");

			Supplier supplier = new Supplier();
			
			for (int index = 1; index <= 10000; index++) {
				supplier.su_suppkey = index;
				supplier.su_name = ran.astring(25, 25);
				supplier.su_address = ran.astring(20, 40);
				supplier.su_nationkey = nationkeys[ran.number(0, 61)];
				supplier.su_phone = ran.nstring(15, 15);
				supplier.su_acctbal = (float) ran.fixedPoint(2, 10000., 1000000000.);
				supplier.su_comment = ran.astring(51, 101);

				k++;
				
				if (supplierPrntWr == null) {
					supplierPrepStmt.setLong(1, supplier.su_suppkey);
					supplierPrepStmt.setString(2, supplier.su_name);
					supplierPrepStmt.setString(3, supplier.su_address);
					supplierPrepStmt.setLong(4, supplier.su_nationkey);
					supplierPrepStmt.setString(5, supplier.su_phone);
					supplierPrepStmt.setDouble(6, supplier.su_acctbal);
					supplierPrepStmt.setString(7, supplier.su_comment);
					supplierPrepStmt.addBatch();
				} else {
					supplierPrntWr.printf("%d,", supplier.su_suppkey);
					supplierPrntWr.printf("%s,", supplier.su_name);
					supplierPrntWr.printf("%s,", supplier.su_address);
					supplierPrntWr.printf("%d,", supplier.su_nationkey);
					supplierPrntWr.printf("%s,", supplier.su_phone);
					supplierPrntWr.printf("%.2f,", supplier.su_acctbal);
					supplierPrntWr.printf("%s", supplier.su_comment);
					supplierPrntWr.println();
				}

				if ((k % configCommitCount) == 0) {
					long tmpTime = new java.util.Date().getTime();
					String etStr = "  Elasped Time(ms): "
							+ ((tmpTime - lastTimeMS) / 1000.000)
							+ "                    ";
					LOG.debug(etStr.substring(0, 30)
							+ "  Writing record " + k + " of " + t);
					lastTimeMS = tmpTime;
					if (supplierPrntWr == null) {
						supplierPrepStmt.executeBatch();
						supplierPrepStmt.clearBatch();
						conn.commit();
					} else {
						supplierPrntWr.flush();
					}
				}
			}

			long tmpTime = new java.util.Date().getTime();
			String etStr = "  Elasped Time(ms): "
					+ ((tmpTime - lastTimeMS) / 1000.000)
					+ "                    ";
			LOG.debug(etStr.substring(0, 30) + "  Writing record " + k
					+ " of " + t);
			lastTimeMS = tmpTime;

			if (supplierPrntWr == null) {
				supplierPrepStmt.executeBatch();
				conn.commit();
			} else {
				supplierPrntWr.flush();
				supplierPrntWr.close();
			}
			now = new java.util.Date();
			LOG.debug("End Region Load @  " + now);

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			conn.rollback();
		} catch (Exception e) {
			e.printStackTrace(System.err);
			conn.rollback();
		}

		return (k);

	} // end loadSuppliers()

	protected long loadHelper() {
		long totalRows = 0;
		try {
			totalRows += loadRegions();
			totalRows += loadNations();
			totalRows += loadSuppliers();
		}
		catch (SQLException e) {
			LOG.debug(e.getMessage());
		}
		return totalRows;
	}	
	
}
