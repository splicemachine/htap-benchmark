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


package com.oltpbenchmark.benchmarks.tpcc;

/*
 * Copyright (C) 2004-2006, Denis Lussier
 *
 * LoadData - Load Sample Data directly into database tables or create CSV files for
 *            each table that can then be bulk loaded (again & again & again ...)  :-)
 *
 *    Two optional parameter sets for the command line:
 *
 *                 numWarehouses=9999
 *
 *                 fileLocation=c:/temp/csv/
 *
 *    "numWarehouses" defaults to "1" and when "fileLocation" is omitted the generated
 *    data is loaded into the database tables directly.
 *
 */

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.io.*;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.benchmarks.tpcc.pojo.*;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

/**
 * TPC-C Benchmark Loader
 */
public class TPCCLoader extends Loader<TPCCBenchmark> {
    private static final Logger LOG = Logger.getLogger(TPCCLoader.class);

	// create file name for current csv
	private String fileName(String table, int warehouseId) {
		return String.format("%s/%s-%06d.csv", fileLocation, table, warehouseId);
	}
	
	//create possible keys for nationkey ([a-zA-Z0-9])
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

    public TPCCLoader(TPCCBenchmark benchmark, Connection c) {
        super(benchmark, c);
        numWarehouses = (int)Math.round(TPCCConfig.configWhseCount * this.scaleFactor);
        if (numWarehouses <= 0) {
            //where would be fun in that?
            numWarehouses = 1;
        }
        LOG.debug("startid: " + this.startId + " filelocation: " + this.fileLocation);
    }

    private int numWarehouses = 0;
    private static final int FIRST_UNPROCESSED_O_ID = 2101;

    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        List<LoaderThread> threads = new ArrayList<LoaderThread>();
        final CountDownLatch itemLatch = new CountDownLatch(1);
        // rdb: only create items if this is the initial load - incremental loads to not need additional items 
        final boolean doItemLoad = (startId == 1);

        // ITEM
        // This will be invoked first and executed in a single thread.
        threads.add(new LoaderThread() {
        	@Override
        	public void load(Connection conn) throws SQLException {
        		if (doItemLoad) {
                    if (LOG.isDebugEnabled()) LOG.debug("Starting to load ITEM");

        			loadItems(conn, TPCCConfig.configItemCount);
        		}
        		itemLatch.countDown();
        	}
        });

        // WAREHOUSES
        // We use a separate thread per warehouse. Each thread will load
        // all of the tables that depend on that warehouse. They all have
        // to wait until the ITEM table is loaded first though.
        for (int w = startId; w < numWarehouses + startId; w++) {
            final int w_id = w;
            LoaderThread t = new LoaderThread() {
                @Override
                public void load(Connection conn) throws SQLException {
                    // Make sure that we load the ITEM table first
                    try {
                        itemLatch.await();
                    } catch (InterruptedException ex) {
                        ex.printStackTrace(System.err);
                        throw new RuntimeException(ex);
                    }

                    if (LOG.isDebugEnabled()) LOG.debug("Starting to load WAREHOUSE " + w_id);

                    // WAREHOUSE
                    loadWarehouse(conn, w_id);

                    // STOCK
                    loadStock(conn, w_id, TPCCConfig.configItemCount);

                    // DISTRICT
                    loadDistricts(conn, w_id, TPCCConfig.configDistPerWhse);

                    // CUSTOMER
                    loadCustomers(conn, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);

                    // ORDERS
                    loadOrders(conn, w_id, TPCCConfig.configDistPerWhse, TPCCConfig.configCustPerDist);
                }
            };
            threads.add(t);
        } // FOR
        return (threads);
    }

    private PreparedStatement getInsertStatement(Connection conn, String tableName) throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog(tableName);
        assert(catalog_tbl != null);
        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        LOG.trace("Insert statement for " + tableName + ": " + sql);
        PreparedStatement stmt = conn.prepareStatement(sql);
        return stmt;
    }

    protected void transRollback(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException se) {
            LOG.debug(se.getMessage());
        }
    }

    protected void transCommit(Connection conn) {
        try {
            conn.commit();
        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            transRollback(conn);
        }
    }

    protected int loadItems(Connection conn, int itemKount) {
        int k = 0;
        int randPct = 0;
        int len = 0;
        int startORIGINAL = 0;
        boolean fail = false;
        PrintWriter itemPrntWr = null;
        
        LOG.info("Loading ITEM");

        if (fileLocation != null) {
			try { 
				itemPrntWr = new PrintWriter(fileLocation + "/" + "item.csv");
			} catch (FileNotFoundException fnfe) { 
				LOG.error(fnfe); 
				System.exit(1); 
			} 
        }

        try {
            PreparedStatement itemPrepStmt = itemPrntWr == null ? getInsertStatement(conn, TPCCConstants.TABLENAME_ITEM) : null;

            Item item = new Item();
            int batchSize = 0;
            for (int i = 1; i <= itemKount; i++) {

                item.i_id = i;
                item.i_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(14, 24,
                        benchmark.rng()));
                item.i_price = (double) (TPCCUtil.randomNumber(100, 10000, benchmark.rng()) / 100.0);

                // i_data
                randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
                len = TPCCUtil.randomNumber(26, 50, benchmark.rng());
                if (randPct > 10) {
                    // 90% of time i_data isa random string of length [26 .. 50]
                    item.i_data = TPCCUtil.randomStr(len);
                } else {
                    // 10% of time i_data has "ORIGINAL" crammed somewhere in
                    // middle
                    startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), benchmark.rng());
                    item.i_data = TPCCUtil.randomStr(startORIGINAL - 1)
                            + "ORIGINAL"
                            + TPCCUtil.randomStr(len - startORIGINAL - 9);
                }

                item.i_im_id = TPCCUtil.randomNumber(1, 10000, benchmark.rng());

                k++;

            	int idx = 1;
                if (itemPrntWr == null) {
                	itemPrepStmt.setLong(idx++, item.i_id);
                	itemPrepStmt.setString(idx++, item.i_name);
                	itemPrepStmt.setDouble(idx++, item.i_price);
                	itemPrepStmt.setString(idx++, item.i_data);
                	itemPrepStmt.setLong(idx++, item.i_im_id);
                	itemPrepStmt.addBatch();
                } else {
                	itemPrntWr.printf("%d,", item.i_id);
                	itemPrntWr.printf("%s,", item.i_name);
                	itemPrntWr.printf("%.2f,", item.i_price);
                	itemPrntWr.printf("%s,", item.i_data);
                	itemPrntWr.printf("%d", item.i_im_id);
                	itemPrntWr.println();
                }
                batchSize++;

                if (batchSize == TPCCConfig.configCommitCount) {
                	if (itemPrntWr == null) {
                        itemPrepStmt.executeBatch();
                        itemPrepStmt.clearBatch();
                        transCommit(conn);                		
                	} else {
                		itemPrntWr.flush();
                	}
                    batchSize = 0;
                }
            } // end for


            if (batchSize > 0) {
            	if (itemPrntWr == null) {
            		itemPrepStmt.executeBatch();
                    transCommit(conn);
            	} else {
            		itemPrntWr.flush();
            		itemPrntWr.close();
            	}
            }            

        } catch (BatchUpdateException ex) {
            SQLException next = ex.getNextException();
            LOG.error("Failed to load data for TPC-C", ex);
            if (next != null) LOG.error(ex.getClass().getSimpleName() + " Cause => " + next.getMessage());
            fail = true;
        } catch (SQLException ex) {
            SQLException next = ex.getNextException();
            LOG.error("Failed to load data for TPC-C", ex);
            if (next != null) LOG.error(ex.getClass().getSimpleName() + " Cause => " + next.getMessage());
            fail = true;
        } catch (Exception ex) {
            LOG.error("Failed to load data for TPC-C", ex);
            fail = true;
        } finally {
            if (fail) {
                LOG.debug("Rolling back changes from last batch");
                transRollback(conn);
            }
        }

        return (k);

    } // end loadItem()

    protected int loadWarehouse(Connection conn, int w_id) {
        PrintWriter whsePrntWr = null;
        
        if (fileLocation != null) {
			try { 
				whsePrntWr = new PrintWriter(fileName("warehouse", w_id));
			} catch (FileNotFoundException fnfe) { 
				LOG.error(fnfe); 
				System.exit(1); 
			} 
        }

        try {
            PreparedStatement whsePrepStmt = whsePrntWr == null ? getInsertStatement(conn, TPCCConstants.TABLENAME_WAREHOUSE) : null;
            Warehouse warehouse = new Warehouse();

            warehouse.w_id = w_id;
            warehouse.w_ytd = 300000;

            // random within [0.0000 .. 0.2000]
            warehouse.w_tax = (double) ((TPCCUtil.randomNumber(0, 2000, benchmark.rng())) / 10000.0);
			warehouse.w_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, benchmark.rng()));
			warehouse.w_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
			warehouse.w_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
			warehouse.w_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
			warehouse.w_state = TPCCUtil.randomStr(3).toUpperCase();
			warehouse.w_zip = "123456789";
			warehouse.w_nationkey = nationkeys[TPCCUtil.randomNumber(0, 61, benchmark.rng())];

			int idx = 1;
			if (whsePrntWr == null) {
				whsePrepStmt.setLong(idx++, warehouse.w_id);
				whsePrepStmt.setDouble(idx++, warehouse.w_ytd);
				whsePrepStmt.setDouble(idx++, warehouse.w_tax);
				whsePrepStmt.setString(idx++, warehouse.w_name);
				whsePrepStmt.setString(idx++, warehouse.w_street_1);
				whsePrepStmt.setString(idx++, warehouse.w_street_2);
				whsePrepStmt.setString(idx++, warehouse.w_city);
				whsePrepStmt.setString(idx++, warehouse.w_state);
				whsePrepStmt.setString(idx++, warehouse.w_zip);
				whsePrepStmt.setInt(idx++, warehouse.w_nationkey);
				whsePrepStmt.execute();				
				transCommit(conn);
			} else {
				whsePrntWr.printf("%d,", warehouse.w_id);
				whsePrntWr.printf("%.2f,", warehouse.w_ytd);
				whsePrntWr.printf("%.4f,", warehouse.w_tax);
				whsePrntWr.printf("%s,", warehouse.w_name);
				whsePrntWr.printf("%s,", warehouse.w_street_1);
				whsePrntWr.printf("%s,", warehouse.w_street_2);
				whsePrntWr.printf("%s,", warehouse.w_city);
				whsePrntWr.printf("%s,", warehouse.w_state);
				whsePrntWr.printf("%s,", warehouse.w_zip);
				whsePrntWr.printf("%d", warehouse.w_nationkey);
				whsePrntWr.println();
				whsePrntWr.flush();
				whsePrntWr.close();
			}

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			transRollback(conn);
		} catch (Exception e) {
			e.printStackTrace(System.err);
			transRollback(conn);
		}

		return (1);

	} // end loadWhse()

	protected int loadStock(Connection conn, int w_id, int numItems) {

		int k = 0;
		int randPct = 0;
		int len = 0;
		int startORIGINAL = 0;
		
        PrintWriter stckPrntWr = null;
        
        if (fileLocation != null) {
			try { 
				stckPrntWr = new PrintWriter(fileName("stock", w_id));
			} catch (FileNotFoundException fnfe) { 
				LOG.error(fnfe); 
				System.exit(1); 
			} 
        }

		try {
		    PreparedStatement stckPrepStmt = stckPrntWr == null ? getInsertStatement(conn, TPCCConstants.TABLENAME_STOCK) : null;

			Stock stock = new Stock();
			for (int i = 1; i <= numItems; i++) {
				stock.s_i_id = i;
				stock.s_w_id = w_id;
				stock.s_quantity = TPCCUtil.randomNumber(10, 100, benchmark.rng());
				stock.s_ytd = 0;
				stock.s_order_cnt = 0;
				stock.s_remote_cnt = 0;

				// s_data
				randPct = TPCCUtil.randomNumber(1, 100, benchmark.rng());
				len = TPCCUtil.randomNumber(26, 50, benchmark.rng());
				if (randPct > 10) {
					// 90% of time i_data isa random string of length [26 ..
					// 50]
					stock.s_data = TPCCUtil.randomStr(len);
				} else {
					// 10% of time i_data has "ORIGINAL" crammed somewhere
					// in middle
					startORIGINAL = TPCCUtil
							.randomNumber(2, (len - 8), benchmark.rng());
					stock.s_data = TPCCUtil.randomStr(startORIGINAL - 1)
							+ "ORIGINAL"
							+ TPCCUtil.randomStr(len - startORIGINAL - 9);
				}
				
				// supplier key
				stock.s_suppkey = (stock.s_w_id * stock.s_i_id) % 10000; // MOD((S_W_ID * S_I_ID), 10000)

				k++;
				int idx = 1;
				if (stckPrntWr == null) {
					stckPrepStmt.setLong(idx++, stock.s_w_id);
					stckPrepStmt.setLong(idx++, stock.s_i_id);
					stckPrepStmt.setLong(idx++, stock.s_quantity);
					stckPrepStmt.setDouble(idx++, stock.s_ytd);
					stckPrepStmt.setLong(idx++, stock.s_order_cnt);
					stckPrepStmt.setLong(idx++, stock.s_remote_cnt);
					stckPrepStmt.setString(idx++, stock.s_data);
					stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
					stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
					stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
					stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
					stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
					stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
					stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
					stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
					stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
					stckPrepStmt.setString(idx++, TPCCUtil.randomStr(24));
					stckPrepStmt.setLong(idx++, stock.s_suppkey);
					stckPrepStmt.addBatch();					
				} else {
					stckPrntWr.printf("%d,", stock.s_w_id);
					stckPrntWr.printf("%d,", stock.s_i_id);
					stckPrntWr.printf("%d,", stock.s_quantity);
					stckPrntWr.printf("%.2f,", stock.s_ytd);
					stckPrntWr.printf("%d,", stock.s_order_cnt);
					stckPrntWr.printf("%d,", stock.s_remote_cnt);
					stckPrntWr.printf("%s,", stock.s_data);
					stckPrntWr.printf("%s,", TPCCUtil.randomStr(24));
					stckPrntWr.printf("%s,", TPCCUtil.randomStr(24));
					stckPrntWr.printf("%s,", TPCCUtil.randomStr(24));
					stckPrntWr.printf("%s,", TPCCUtil.randomStr(24));
					stckPrntWr.printf("%s,", TPCCUtil.randomStr(24));
					stckPrntWr.printf("%s,", TPCCUtil.randomStr(24));
					stckPrntWr.printf("%s,", TPCCUtil.randomStr(24));
					stckPrntWr.printf("%s,", TPCCUtil.randomStr(24));
					stckPrntWr.printf("%s,", TPCCUtil.randomStr(24));
					stckPrntWr.printf("%s,", TPCCUtil.randomStr(24));
					stckPrntWr.printf("%d", stock.s_suppkey);
					stckPrntWr.println();					
				}
				
				if ((k % TPCCConfig.configCommitCount) == 0) {
					if (stckPrntWr == null) {
						stckPrepStmt.executeBatch();
						stckPrepStmt.clearBatch();
						transCommit(conn);
					} else {
						stckPrntWr.flush();
					}
				}
			} // end for [i]

			if (stckPrntWr == null) {
				stckPrepStmt.executeBatch();
				transCommit(conn);
			} else {
				stckPrntWr.flush();
				stckPrntWr.close();
			}

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			transRollback(conn);

		} catch (Exception e) {
			e.printStackTrace(System.err);
			transRollback(conn);
		}

		return (k);

	} // end loadStock()

	protected int loadDistricts(Connection conn, int w_id, int distWhseKount) {

		int k = 0;

        PrintWriter distPrntWr = null;
        
        if (fileLocation != null) {
			try { 
				distPrntWr = new PrintWriter(fileName("district", w_id));
			} catch (FileNotFoundException fnfe) { 
				LOG.error(fnfe); 
				System.exit(1); 
			} 
        }

		try {

			PreparedStatement distPrepStmt = distPrntWr == null ? getInsertStatement(conn, TPCCConstants.TABLENAME_DISTRICT) : null;
			District district = new District();

			for (int d = 1; d <= distWhseKount; d++) {
				district.d_id = d;
				district.d_w_id = w_id;
				district.d_ytd = 30000;

				// random within [0.0000 .. 0.2000]
				district.d_tax = (float) ((TPCCUtil.randomNumber(0, 2000, benchmark.rng())) / 10000.0);

				district.d_next_o_id = TPCCConfig.configCustPerDist + 1;
				district.d_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, benchmark.rng()));
				district.d_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
				district.d_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
				district.d_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
				district.d_state = TPCCUtil.randomStr(3).toUpperCase();
				district.d_zip = "123456789";
				district.d_nationkey = nationkeys[TPCCUtil.randomNumber(0, 61, benchmark.rng())];

				k++;
				int idx = 1;
				if (distPrntWr == null) {
					distPrepStmt.setLong(idx++, district.d_w_id);
					distPrepStmt.setLong(idx++, district.d_id);
					distPrepStmt.setDouble(idx++, district.d_ytd);
					distPrepStmt.setDouble(idx++, district.d_tax);
					distPrepStmt.setLong(idx++, district.d_next_o_id);
					distPrepStmt.setString(idx++, district.d_name);
					distPrepStmt.setString(idx++, district.d_street_1);
					distPrepStmt.setString(idx++, district.d_street_2);
					distPrepStmt.setString(idx++, district.d_city);
					distPrepStmt.setString(idx++, district.d_state);
					distPrepStmt.setString(idx++, district.d_zip);
					distPrepStmt.setInt(idx++, district.d_nationkey);
					distPrepStmt.executeUpdate();
				} else {
					distPrntWr.printf("%d,", district.d_w_id);
					distPrntWr.printf("%d,", district.d_id);
					distPrntWr.printf("%.2f,", district.d_ytd);
					distPrntWr.printf("%.4f,", district.d_tax);
					distPrntWr.printf("%d,", district.d_next_o_id);
					distPrntWr.printf("%s,", district.d_name);
					distPrntWr.printf("%s,", district.d_street_1);
					distPrntWr.printf("%s,", district.d_street_2);
					distPrntWr.printf("%s,", district.d_city);
					distPrntWr.printf("%s,", district.d_state);
					distPrntWr.printf("%s,", district.d_zip);
					distPrntWr.printf("%d", district.d_nationkey);
					distPrntWr.println();					
				}
			} // end for [d]

			if (distPrntWr == null) {
				transCommit(conn);
			} else {
				distPrntWr.flush();
				distPrntWr.close();
			}
		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			transRollback(conn);
		} catch (Exception e) {
			e.printStackTrace(System.err);
			transRollback(conn);
		}

		return (k);

	} // end loadDist()

	protected int loadCustomers(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

		int k = 0;

        PrintWriter custPrntWr = null;
        PrintWriter histPrntWr = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        
        if (fileLocation != null) {
			try { 
				custPrntWr = new PrintWriter(fileName("customer", w_id));
				histPrntWr = new PrintWriter(fileName("history", w_id));				
			} catch (FileNotFoundException fnfe) { 
				LOG.error(fnfe); 
				System.exit(1); 
			} 
        }

		Customer customer = new Customer();
		History history = new History();

		try {
		    PreparedStatement custPrepStmt = custPrntWr == null ? getInsertStatement(conn, TPCCConstants.TABLENAME_CUSTOMER) : null;
		    PreparedStatement histPrepStmt = histPrntWr == null ? getInsertStatement(conn, TPCCConstants.TABLENAME_HISTORY) : null;

			for (int d = 1; d <= districtsPerWarehouse; d++) {
				for (int c = 1; c <= customersPerDistrict; c++) {
					Timestamp sysdate = this.benchmark.getTimestamp(System.currentTimeMillis());

					customer.c_id = c;
					customer.c_d_id = d;
					customer.c_w_id = w_id;

					// discount is random between [0.0000 ... 0.5000]
					customer.c_discount = (float) (TPCCUtil.randomNumber(1, 5000, benchmark.rng()) / 10000.0);

					if (TPCCUtil.randomNumber(1, 100, benchmark.rng()) <= 10) {
						customer.c_credit = "BC"; // 10% Bad Credit
					} else {
						customer.c_credit = "GC"; // 90% Good Credit
					}
					if (c <= 1000) {
						customer.c_last = TPCCUtil.getLastName(c - 1);
					} else {
						customer.c_last = TPCCUtil.getNonUniformRandomLastNameForLoad(benchmark.rng());
					}
					customer.c_first = TPCCUtil.randomStr(TPCCUtil.randomNumber(8, 16, benchmark.rng()));
					customer.c_credit_lim = 50000;

					customer.c_balance = -10;
					customer.c_ytd_payment = 10;
					customer.c_payment_cnt = 1;
					customer.c_delivery_cnt = 0;

					customer.c_street_1 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
					customer.c_street_2 = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
					customer.c_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, benchmark.rng()));
					customer.c_state = TPCCUtil.randomStr(3).toUpperCase();
					// TPC-C 4.3.2.7: 4 random digits + "11111"
					customer.c_zip = TPCCUtil.randomNStr(4) + "11111";
					customer.c_nationkey = nationkeys[TPCCUtil.randomNumber(0, 61, benchmark.rng())];
					customer.c_phone = TPCCUtil.randomNStr(16);
					customer.c_since = sysdate;
					customer.c_middle = "OE";
					customer.c_data = TPCCUtil.randomStr(TPCCUtil
							.randomNumber(300, 500, benchmark.rng()));

					history.h_c_id = c;
					history.h_c_d_id = d;
					history.h_c_w_id = w_id;
					history.h_d_id = d;
					history.h_w_id = w_id;
					history.h_date = sysdate;
					history.h_amount = 10;
					history.h_data = TPCCUtil.randomStr(TPCCUtil
							.randomNumber(10, 24, benchmark.rng()));

					k = k + 2;
					int idx = 1;
					if (custPrntWr == null) {
						custPrepStmt.setLong(idx++, customer.c_w_id);
						custPrepStmt.setLong(idx++, customer.c_d_id);
						custPrepStmt.setLong(idx++, customer.c_id);
						custPrepStmt.setDouble(idx++, customer.c_discount);
						custPrepStmt.setString(idx++, customer.c_credit);
						custPrepStmt.setString(idx++, customer.c_last);
						custPrepStmt.setString(idx++, customer.c_first);
						custPrepStmt.setDouble(idx++, customer.c_credit_lim);
						custPrepStmt.setDouble(idx++, customer.c_balance);
						custPrepStmt.setDouble(idx++, customer.c_ytd_payment);
						custPrepStmt.setLong(idx++, customer.c_payment_cnt);
						custPrepStmt.setLong(idx++, customer.c_delivery_cnt);
						custPrepStmt.setString(idx++, customer.c_street_1);
						custPrepStmt.setString(idx++, customer.c_street_2);
						custPrepStmt.setString(idx++, customer.c_city);
						custPrepStmt.setString(idx++, customer.c_state);
						custPrepStmt.setString(idx++, customer.c_zip);
						custPrepStmt.setInt(idx++, customer.c_nationkey);
						custPrepStmt.setString(idx++, customer.c_phone);
						custPrepStmt.setTimestamp(idx++, customer.c_since);
						custPrepStmt.setString(idx++, customer.c_middle);
						custPrepStmt.setString(idx++, customer.c_data);
						custPrepStmt.addBatch();
					} else {
						custPrntWr.printf("%d,", customer.c_w_id);
						custPrntWr.printf("%d,", customer.c_d_id);
						custPrntWr.printf("%d,", customer.c_id);
						custPrntWr.printf("%.4f,", customer.c_discount);
						custPrntWr.printf("%s,", customer.c_credit);
						custPrntWr.printf("%s,", customer.c_last);
						custPrntWr.printf("%s,", customer.c_first);
						custPrntWr.printf("%.2f,", customer.c_credit_lim);
						custPrntWr.printf("%.2f,", customer.c_balance);
						custPrntWr.printf("%.2f,", customer.c_ytd_payment);
						custPrntWr.printf("%d,", customer.c_payment_cnt);
						custPrntWr.printf("%d,", customer.c_delivery_cnt);
						custPrntWr.printf("%s,", customer.c_street_1);
						custPrntWr.printf("%s,", customer.c_street_2);
						custPrntWr.printf("%s,", customer.c_city);
						custPrntWr.printf("%s,", customer.c_state);
						custPrntWr.printf("%s,", customer.c_zip);
						custPrntWr.printf("%d,", customer.c_nationkey);
						custPrntWr.printf("%s,", customer.c_phone);
						custPrntWr.printf("%s,", dateFormat.format(customer.c_since.getTime()));
						custPrntWr.printf("%s,", customer.c_middle);
						custPrntWr.printf("%s", customer.c_data);
						custPrntWr.println();
					}

					idx = 1;
					if (histPrntWr == null) {
						histPrepStmt.setInt(idx++, history.h_c_id);
						histPrepStmt.setInt(idx++, history.h_c_d_id);
						histPrepStmt.setInt(idx++, history.h_c_w_id);
						histPrepStmt.setInt(idx++, history.h_d_id);
						histPrepStmt.setInt(idx++, history.h_w_id);
						histPrepStmt.setTimestamp(idx++, history.h_date);
						histPrepStmt.setDouble(idx++, history.h_amount);
						histPrepStmt.setString(idx++, history.h_data);
						histPrepStmt.addBatch();
					} else {
						histPrntWr.printf("%d,", history.h_c_id);
						histPrntWr.printf("%d,", history.h_c_d_id);
						histPrntWr.printf("%d,", history.h_c_w_id);
						histPrntWr.printf("%d,", history.h_d_id);
						histPrntWr.printf("%d,", history.h_w_id);
						histPrntWr.printf("%s,", dateFormat.format(history.h_date.getTime()));
						histPrntWr.printf("%.2f,", history.h_amount);
						histPrntWr.printf("%s", history.h_data);
						histPrntWr.println();
					}

					if ((k % TPCCConfig.configCommitCount) == 0) {
						if (custPrntWr == null) {
							custPrepStmt.executeBatch();
							histPrepStmt.executeBatch();
							custPrepStmt.clearBatch();
							custPrepStmt.clearBatch();
							transCommit(conn);
						} else {
							custPrntWr.flush();
							histPrntWr.flush();
						}
					}
				} // end for [c]
			} // end for [d]

			if (custPrntWr == null) {
				custPrepStmt.executeBatch();
				histPrepStmt.executeBatch();
				custPrepStmt.clearBatch();
				histPrepStmt.clearBatch();
				transCommit(conn);
			} else {
				custPrntWr.flush();
				histPrntWr.flush();
				custPrntWr.close();
				histPrntWr.close();
			}

		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			transRollback(conn);
		} catch (Exception e) {
			e.printStackTrace(System.err);
			transRollback(conn);
		}

		return (k);

	} // end loadCust()

	protected int loadOrders(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict) {

		int k = 0;
		int t = 0;
		
        PrintWriter ordrPrntWr = null;
        PrintWriter nworPrntWr = null;
        PrintWriter orlnPrntWr = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        
        if (fileLocation != null) {
			try { 
				ordrPrntWr = new PrintWriter(fileName("oorder", w_id));
				nworPrntWr = new PrintWriter(fileName("new_order", w_id));
				orlnPrntWr = new PrintWriter(fileName("order_line", w_id));
			} catch (FileNotFoundException fnfe) { 
				LOG.error(fnfe); 
				System.exit(1); 
			} 
        }

		try {
		    PreparedStatement ordrPrepStmt = ordrPrntWr == null ? getInsertStatement(conn, TPCCConstants.TABLENAME_OPENORDER) : null;
		    PreparedStatement nworPrepStmt = nworPrntWr == null ? getInsertStatement(conn, TPCCConstants.TABLENAME_NEWORDER) : null;
		    PreparedStatement orlnPrepStmt = orlnPrntWr == null ? getInsertStatement(conn, TPCCConstants.TABLENAME_ORDERLINE) : null;

			Oorder oorder = new Oorder();
			NewOrder new_order = new NewOrder();
			OrderLine order_line = new OrderLine();

			for (int d = 1; d <= districtsPerWarehouse; d++) {
				// TPC-C 4.3.3.1: o_c_id must be a permutation of [1, 3000]
				int[] c_ids = new int[customersPerDistrict];
				for (int i = 0; i < customersPerDistrict; ++i) {
					c_ids[i] = i + 1;
				}
				// Collections.shuffle exists, but there is no
				// Arrays.shuffle
				for (int i = 0; i < c_ids.length - 1; ++i) {
					int remaining = c_ids.length - i - 1;
					int swapIndex = benchmark.rng().nextInt(remaining) + i + 1;
					assert i < swapIndex;
					int temp = c_ids[swapIndex];
					c_ids[swapIndex] = c_ids[i];
					c_ids[i] = temp;
				}

				int newOrderBatch = 0;
				for (int c = 1; c <= customersPerDistrict; c++) {

					oorder.o_id = c;
					oorder.o_w_id = w_id;
					oorder.o_d_id = d;
					oorder.o_c_id = c_ids[c - 1];
					// o_carrier_id is set *only* for orders with ids < 2101
					// [4.3.3.1]
					if (oorder.o_id < FIRST_UNPROCESSED_O_ID) {
						oorder.o_carrier_id = TPCCUtil.randomNumber(1, 10, benchmark.rng());
					} else {
						oorder.o_carrier_id = null;
					}
					oorder.o_ol_cnt = TPCCUtil.randomNumber(5, 15, benchmark.rng());
					oorder.o_all_local = 1;
					oorder.o_entry_d = this.benchmark.getTimestamp(System.currentTimeMillis());

					k++;
					int idx = 1;
					if (ordrPrntWr == null) {
						ordrPrepStmt.setInt(idx++, oorder.o_w_id);
			            ordrPrepStmt.setInt(idx++, oorder.o_d_id);
			            ordrPrepStmt.setInt(idx++, oorder.o_id);
			            ordrPrepStmt.setInt(idx++, oorder.o_c_id);
			            if (oorder.o_carrier_id != null) {
			                ordrPrepStmt.setInt(idx++, oorder.o_carrier_id);
			            } else {
			                ordrPrepStmt.setNull(idx++, Types.INTEGER);
			            }
			            ordrPrepStmt.setInt(idx++, oorder.o_ol_cnt);
			            ordrPrepStmt.setInt(idx++, oorder.o_all_local);
			            ordrPrepStmt.setTimestamp(idx++, oorder.o_entry_d);
			            ordrPrepStmt.addBatch();						
					} else {
						ordrPrntWr.printf("%d,", oorder.o_w_id);
			            ordrPrntWr.printf("%d,", oorder.o_d_id);
			            ordrPrntWr.printf("%d,", oorder.o_id);
			            ordrPrntWr.printf("%d,", oorder.o_c_id);
			            if (oorder.o_carrier_id != null) {
			                ordrPrntWr.printf("%d,", oorder.o_carrier_id);
			            } else {
			                ordrPrntWr.printf("null,");
			            }
			            ordrPrntWr.printf("%d,", oorder.o_ol_cnt);
			            ordrPrntWr.printf("%d,", oorder.o_all_local);
						ordrPrntWr.printf("%s", dateFormat.format(oorder.o_entry_d.getTime()));
			            ordrPrntWr.println();
					}

					// 900 rows in the NEW-ORDER table corresponding to the last
					// 900 rows in the ORDER table for that district (i.e.,
					// with NO_O_ID between 2,101 and 3,000)
					if (c >= FIRST_UNPROCESSED_O_ID) {
						new_order.no_w_id = w_id;
						new_order.no_d_id = d;
						new_order.no_o_id = c;

						k++;
						idx = 1;
						if (nworPrntWr == null) {
					        nworPrepStmt.setInt(idx++, new_order.no_w_id);
				            nworPrepStmt.setInt(idx++, new_order.no_d_id);
					        nworPrepStmt.setInt(idx++, new_order.no_o_id);
				            nworPrepStmt.addBatch();							
						} else {
					        nworPrntWr.printf("%d,", new_order.no_w_id);
				            nworPrntWr.printf("%d,", new_order.no_d_id);
					        nworPrntWr.printf("%d", new_order.no_o_id);
				            nworPrntWr.println();
						}
						newOrderBatch++;
					} // end new order

					for (int l = 1; l <= oorder.o_ol_cnt; l++) {
						order_line.ol_w_id = w_id;
						order_line.ol_d_id = d;
						order_line.ol_o_id = c;
						order_line.ol_number = l; // ol_number
						order_line.ol_i_id = TPCCUtil.randomNumber(1,
						        TPCCConfig.configItemCount, benchmark.rng());
						if (order_line.ol_o_id < FIRST_UNPROCESSED_O_ID) {
							order_line.ol_delivery_d = oorder.o_entry_d;
							order_line.ol_amount = 0;
						} else {
							order_line.ol_delivery_d = null;
							// random within [0.01 .. 9,999.99]
							order_line.ol_amount = (float) (TPCCUtil.randomNumber(1, 999999, benchmark.rng()) / 100.0);
						}
						order_line.ol_supply_w_id = order_line.ol_w_id;
						order_line.ol_quantity = 5;
						order_line.ol_dist_info = TPCCUtil.randomStr(24);

						k++;
						idx = 1;
						if (orlnPrntWr == null) {
							orlnPrepStmt.setInt(idx++, order_line.ol_w_id);
				            orlnPrepStmt.setInt(idx++, order_line.ol_d_id);
				            orlnPrepStmt.setInt(idx++, order_line.ol_o_id);
				            orlnPrepStmt.setInt(idx++, order_line.ol_number);
				            orlnPrepStmt.setLong(idx++, order_line.ol_i_id);
				            if (order_line.ol_delivery_d != null) {
				                orlnPrepStmt.setTimestamp(idx++, order_line.ol_delivery_d);
				            } else {
//				                orlnPrepStmt.setNull(idx++, 0);
				                orlnPrepStmt.setTimestamp(idx++, null);
				            }
				            orlnPrepStmt.setDouble(idx++, order_line.ol_amount);
				            orlnPrepStmt.setLong(idx++, order_line.ol_supply_w_id);
				            orlnPrepStmt.setDouble(idx++, order_line.ol_quantity);
				            orlnPrepStmt.setString(idx++, order_line.ol_dist_info);
				            orlnPrepStmt.addBatch();
						} else {
							orlnPrntWr.printf("%d,", order_line.ol_w_id);
				            orlnPrntWr.printf("%d,", order_line.ol_d_id);
				            orlnPrntWr.printf("%d,", order_line.ol_o_id);
				            orlnPrntWr.printf("%d,", order_line.ol_number);
				            orlnPrntWr.printf("%d,", order_line.ol_i_id);
				            if (order_line.ol_delivery_d != null) {
				                orlnPrntWr.printf("%s,", dateFormat.format(order_line.ol_delivery_d.getTime()));
				            } else {
				                orlnPrntWr.printf("null,");
				            }
				            orlnPrntWr.printf("%.2f,", order_line.ol_amount);
				            orlnPrntWr.printf("%d,", order_line.ol_supply_w_id);
				            orlnPrntWr.printf("%d,", order_line.ol_quantity);
				            orlnPrntWr.printf("%s", order_line.ol_dist_info);
				            orlnPrntWr.println();
						}

						if ((k % TPCCConfig.configCommitCount) == 0) {
							if (ordrPrntWr == null) {
								ordrPrepStmt.executeBatch();
								if (newOrderBatch > 0) {
								    nworPrepStmt.executeBatch();
								    newOrderBatch = 0;
								}
								orlnPrepStmt.executeBatch();

								ordrPrepStmt.clearBatch();
								nworPrepStmt.clearBatch();
								orlnPrepStmt.clearBatch();
								transCommit(conn);
							} else {
								ordrPrntWr.flush();
								if (newOrderBatch > 0) {
								    nworPrntWr.flush();
								    newOrderBatch = 0;
								}
								orlnPrntWr.flush();
							}
						}

					} // end for [l]

				} // end for [c]

			} // end for [d]


			if (LOG.isDebugEnabled())  LOG.debug("  Writing final records " + k + " of " + t);
			if (ordrPrntWr == null) {
			    ordrPrepStmt.executeBatch();
			    nworPrepStmt.executeBatch();
			    orlnPrepStmt.executeBatch();
				transCommit(conn);
			} else {
			    ordrPrntWr.flush();
			    nworPrntWr.flush();
			    orlnPrntWr.flush();
			    ordrPrntWr.close();
			    nworPrntWr.close();
			    orlnPrntWr.close();
			}

        } catch (SQLException se) {
            LOG.debug(se.getMessage());
            se.printStackTrace(System.err);
            transRollback(conn);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            transRollback(conn);
        }

        return (k);

    } // end loadOrder()

} // end LoadData Class
