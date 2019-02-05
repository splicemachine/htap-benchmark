package com.oltpbenchmark.benchmarks.chbenchmark.queries;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class INSERT extends GenericQuery {

    private final SQLStmt getMaxOrderIdSQL = new SQLStmt(
            "SELECT MAX(NO_O_ID) AS VALUE FROM " + TPCCConstants.TABLENAME_NEWORDER + " --splice-properties splits=12");

    private final SQLStmt  stmtInsertNewOrderSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER +
                    " (NO_O_ID, NO_D_ID, NO_W_ID) " +
                    " VALUES ( ?, ?, ?)");

    public final SQLStmt  stmtInsertOOrderSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER +
                    " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)" +
                    " VALUES (?, ?, ?, ?, ?, ?, ?)");

    public final SQLStmt  stmtInsertOrderLineSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE +
                    " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) " +
                    " VALUES (?,?,?,?,?,?,?,?,?)");

    protected SQLStmt get_query() {
        return null;
    }

    private static int orderId = 0;

    @Override
    public ResultSet run(Connection conn, Worker w, int timeout) throws SQLException {
        int numWarehouses = (int)w.getWorkloadConfiguration().getScaleFactor();

        int o_id;
        synchronized (INSERT.class) {
            if (orderId == 0) {
                PreparedStatement p = getPreparedStatement(conn, getMaxOrderIdSQL);
                ResultSet rs = p.executeQuery();
                if (!rs.next()) {
                    throw new RuntimeException("Can't query " + TPCCConstants.TABLENAME_NEWORDER);
                }
                orderId = rs.getInt("VALUE");
                rs.close();
                rs = null;
            }
            o_id = ++orderId;
        }

        int districtsPerWarehouse = TPCCConfig.configDistPerWhse;
        Random gen = ThreadLocalRandom.current();

        PreparedStatement stmtInsertNewOrder = getPreparedStatement(conn, stmtInsertNewOrderSQL);
        PreparedStatement stmtInsertOOrder =this.getPreparedStatement(conn, stmtInsertOOrderSQL);
        PreparedStatement stmtInsertOrderLine =this.getPreparedStatement(conn, stmtInsertOrderLineSQL);

        int count = 0;
        for (int w_id = 1; w_id <= numWarehouses; ++w_id) {
            for (int d_id = 1; d_id <= districtsPerWarehouse; ++d_id) {
                int c_id = TPCCUtil.getCustomerID(gen);
                int o_ol_cnt = TPCCUtil.randomNumber(5, 15, gen);
                int o_all_local = 1;

                for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                    int ol_supply_w_id = w_id;
                    int ol_i_id = TPCCUtil.getItemID(gen);
                    int ol_quantity = TPCCUtil.randomNumber(1, 10, gen);
                    float ol_amount = (float) (TPCCUtil.randomNumber(1, 999999, gen) / 100.0);
                    String ol_dist_info = TPCCUtil.randomStr(24);

                    if (TPCCUtil.randomNumber(1, 100, gen) <= 1) {
                        ol_supply_w_id = TPCCUtil.randomNumber(1, numWarehouses, gen);
                        o_all_local = 0;
                    }

                    stmtInsertOrderLine.setInt(1, o_id);
                    stmtInsertOrderLine.setInt(2, d_id);
                    stmtInsertOrderLine.setInt(3, w_id);
                    stmtInsertOrderLine.setInt(4, ol_number);
                    stmtInsertOrderLine.setInt(5, ol_i_id);
                    stmtInsertOrderLine.setInt(6, ol_supply_w_id);
                    stmtInsertOrderLine.setInt(7, ol_quantity);
                    stmtInsertOrderLine.setDouble(8, ol_amount);
                    stmtInsertOrderLine.setString(9, ol_dist_info);
                    stmtInsertOrderLine.addBatch();
                    ++count;

                    if (count >= TPCCConfig.configCommitCount) {
                        stmtInsertOOrder.executeBatch();
                        stmtInsertNewOrder.executeBatch();
                        stmtInsertOrderLine.executeBatch();
                        conn.commit();

                        stmtInsertOOrder.clearBatch();
                        stmtInsertNewOrder.clearBatch();
                        stmtInsertOrderLine.clearBatch();
                        count = 0;
                    }
                }

                stmtInsertOOrder.setInt(1, o_id);
                stmtInsertOOrder.setInt(2, d_id);
                stmtInsertOOrder.setInt(3, w_id);
                stmtInsertOOrder.setInt(4, c_id);
                stmtInsertOOrder.setTimestamp(5, new java.sql.Timestamp(System.currentTimeMillis()));
                stmtInsertOOrder.setInt(6, o_ol_cnt);
                stmtInsertOOrder.setInt(7, o_all_local);
                stmtInsertOOrder.addBatch();
                ++count;

                stmtInsertNewOrder.setInt(1, o_id);
                stmtInsertNewOrder.setInt(2, d_id);
                stmtInsertNewOrder.setInt(3, w_id);
                stmtInsertNewOrder.addBatch();
                ++count;

            }
        }

        stmtInsertOOrder.executeBatch();
        stmtInsertNewOrder.executeBatch();
        stmtInsertOrderLine.executeBatch();
        conn.commit();

        return null;
    }
}
