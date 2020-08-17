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

package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.sql.*;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;

public class Delivery extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(Delivery.class);
    private static final int DELIVERY_KEYING_TIME = 2;
    private static final int DELIVERY_THINK_TIME  = 5;

	public SQLStmt delivGetOrderIdSQL = new SQLStmt(
	        "SELECT NO_O_ID FROM " + TPCCConstants.TABLENAME_NEWORDER + 
	        " WHERE NO_D_ID = ? " +
	        "   AND NO_W_ID = ? " +
	        " ORDER BY NO_O_ID ASC " +
	        " LIMIT 1");
	
	public SQLStmt delivDeleteNewOrderSQL = new SQLStmt(
	        "DELETE FROM " + TPCCConstants.TABLENAME_NEWORDER +
			" WHERE NO_O_ID = ? " +
            "   AND NO_D_ID = ?" +
			"   AND NO_W_ID = ?");
	
	public SQLStmt delivGetCustIdSQL = new SQLStmt(
	        "SELECT O_C_ID FROM " + TPCCConstants.TABLENAME_OPENORDER + 
	        " WHERE O_ID = ? " +
            "   AND O_D_ID = ? " +
	        "   AND O_W_ID = ?");
	
	public SQLStmt delivUpdateCarrierIdSQL = new SQLStmt(
	        "UPDATE " + TPCCConstants.TABLENAME_OPENORDER + 
	        "   SET O_CARRIER_ID = ? " +
			" WHERE O_ID = ? " +
	        "   AND O_D_ID = ?" +
			"   AND O_W_ID = ?");
	
	public SQLStmt delivUpdateDeliveryDateSQL = new SQLStmt(
	        "UPDATE " + TPCCConstants.TABLENAME_ORDERLINE +
	        "   SET OL_DELIVERY_D = ? " +
			" WHERE OL_O_ID = ? " +
			"   AND OL_D_ID = ? " +
			"   AND OL_W_ID = ? ");
	
	public SQLStmt delivSumOrderAmountSQL = new SQLStmt(
	        "SELECT SUM(OL_AMOUNT) AS OL_TOTAL " +
			"  FROM " + TPCCConstants.TABLENAME_ORDERLINE + 
			" WHERE OL_O_ID = ? " +
			"   AND OL_D_ID = ? " +
			"   AND OL_W_ID = ?");
	
	public SQLStmt delivUpdateCustBalDelivCntSQL = new SQLStmt(
	        "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER +
	        "   SET C_BALANCE = C_BALANCE + ?," +
			"       C_DELIVERY_CNT = C_DELIVERY_CNT + 1 " +
			" WHERE C_W_ID = ? " +
			"   AND C_D_ID = ? " +
			"   AND C_ID = ? ");


	// Delivery Txn
	private PreparedStatement delivGetOrderId = null;
	private PreparedStatement delivDeleteNewOrder = null;
	private PreparedStatement delivGetCustId = null;
	private PreparedStatement delivUpdateCarrierId = null;
	private PreparedStatement delivUpdateDeliveryDate = null;
	private PreparedStatement delivSumOrderAmount = null;
	private PreparedStatement delivUpdateCustBalDelivCnt = null;

    public int getKeyingTime() {
        return DELIVERY_KEYING_TIME;
    }

    public int getThinkTime() {
        return DELIVERY_THINK_TIME;
    }

    public ResultSet run(Connection conn, Random gen,
			int w_id, int numWarehouses,
			int terminalDistrictLowerID, int terminalDistrictUpperID,
			TPCCWorker w) throws SQLException {

        int o_carrier_id = TPCCUtil.randomNumber(1, 10, gen);

        delivGetOrderId = this.getPreparedStatement(conn, delivGetOrderIdSQL);
        delivDeleteNewOrder = this.getPreparedStatement(conn, delivDeleteNewOrderSQL);
        delivGetCustId = this.getPreparedStatement(conn, delivGetCustIdSQL);
        delivUpdateCarrierId = this.getPreparedStatement(conn, delivUpdateCarrierIdSQL);
        delivUpdateDeliveryDate = this.getPreparedStatement(conn, delivUpdateDeliveryDateSQL);
        delivSumOrderAmount = this.getPreparedStatement(conn, delivSumOrderAmountSQL);
        delivUpdateCustBalDelivCnt = this.getPreparedStatement(conn, delivUpdateCustBalDelivCntSQL);

        boolean done = false;
        while (!done) {
            try {
                deliveryTransaction(w_id, terminalDistrictLowerID, terminalDistrictUpperID, o_carrier_id, conn);
            }
            catch (SQLException ex) {
                SQLException ex2 = ex;
                if (ex2 instanceof BatchUpdateException) {
                    ex2 = ex2.getNextException();
                }
                if (ex2.getSQLState().equals("SE014")) {
                    // Retry write conflicts
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(ex2.getMessage());
                    }
                    conn.rollback();
                    continue;
                }
                throw ex;
            }
            done = true;
        }
        return null;
    }


    private void deliveryTransaction(int w_id, int d_lo_id, int d_hi_id, int o_carrier_id, Connection conn) throws SQLException {

		int d_id, c_id;
        float ol_total = 0;
        int[] orderIDs;

        orderIDs = new int[d_hi_id - d_lo_id + 1];
        for (d_id = d_lo_id; d_id <= d_hi_id; d_id++) {
            delivGetOrderId.setInt(1, d_id);
            delivGetOrderId.setInt(2, w_id);
            ResultSet rs = delivGetOrderId.executeQuery();
            if (!rs.next()) {
                // This district has no new orders
                // This can happen but should be rare
                LOG.warn(String.format("District has no new orders [W_ID=%d, D_ID=%d]", w_id, d_id));
                continue;
            }

            int no_o_id = rs.getInt("NO_O_ID");
            orderIDs[d_id - d_lo_id] = no_o_id;
            rs.close();
            rs = null;

            delivDeleteNewOrder.setInt(1, no_o_id);
            delivDeleteNewOrder.setInt(2, d_id);
            delivDeleteNewOrder.setInt(3, w_id);
            int result = delivDeleteNewOrder.executeUpdate();
            if (result != 1) {
                // This code used to run in a loop in an attempt to make this work
                // with MySQL's default weird consistency level. We just always run
                // this as SERIALIZABLE instead. I don't *think* that fixing this one
                // error makes this work with MySQL's default consistency. 
                // Careful auditing would be required.
                String msg = String.format("NewOrder delete failed. Not running with SERIALIZABLE isolation? " +
                                           "[w_id=%d, d_id=%d, no_o_id=%d]", w_id, d_id, no_o_id);
                throw new UserAbortException(msg);
            }


            delivGetCustId.setInt(1, no_o_id);
            delivGetCustId.setInt(2, d_id);
            delivGetCustId.setInt(3, w_id);
            rs = delivGetCustId.executeQuery();

            if (!rs.next()) {
                String msg = String.format("Failed to retrieve ORDER record [W_ID=%d, D_ID=%d, O_ID=%d]",
                                           w_id, d_id, no_o_id);
                LOG.warn(msg);
                throw new RuntimeException(msg);
            }
            c_id = rs.getInt("O_C_ID");
            rs.close();

            delivUpdateCarrierId.setInt(1, o_carrier_id);
            delivUpdateCarrierId.setInt(2, no_o_id);
            delivUpdateCarrierId.setInt(3, d_id);
            delivUpdateCarrierId.setInt(4, w_id);
            result = delivUpdateCarrierId.executeUpdate();

            if (result != 1) {
                String msg = String.format("Failed to update ORDER record [W_ID=%d, D_ID=%d, O_ID=%d]",
                                           w_id, d_id, no_o_id);
                LOG.warn(msg);
                throw new RuntimeException(msg);
            }

            delivUpdateDeliveryDate.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            delivUpdateDeliveryDate.setInt(2, no_o_id);
            delivUpdateDeliveryDate.setInt(3, d_id);
            delivUpdateDeliveryDate.setInt(4, w_id);
            result = delivUpdateDeliveryDate.executeUpdate();

            if (result == 0){
                String msg = String.format("Failed to update ORDER_LINE records [W_ID=%d, D_ID=%d, O_ID=%d]",
                                           w_id, d_id, no_o_id);
                LOG.warn(msg);
                throw new RuntimeException(msg);
            }


            delivSumOrderAmount.setInt(1, no_o_id);
            delivSumOrderAmount.setInt(2, d_id);
            delivSumOrderAmount.setInt(3, w_id);
            rs = delivSumOrderAmount.executeQuery();

            if (!rs.next()) {
                String msg = String.format("Failed to retrieve ORDER_LINE records [W_ID=%d, D_ID=%d, O_ID=%d]",
                                           w_id, d_id, no_o_id);
                LOG.warn(msg);
                throw new RuntimeException(msg);
            }
            ol_total = rs.getFloat("OL_TOTAL");
            rs.close();

            int idx = 1; // HACK: So that we can debug this query
            delivUpdateCustBalDelivCnt.setDouble(idx++, ol_total);
            delivUpdateCustBalDelivCnt.setInt(idx++, w_id);
            delivUpdateCustBalDelivCnt.setInt(idx++, d_id);
            delivUpdateCustBalDelivCnt.setInt(idx++, c_id);
            result = delivUpdateCustBalDelivCnt.executeUpdate();

            if (result == 0) {
                String msg = String.format("Failed to update CUSTOMER record [W_ID=%d, D_ID=%d, C_ID=%d]",
                                           w_id, d_id, c_id);
                LOG.warn(msg);
                throw new RuntimeException(msg);
            }
        }

        conn.commit();
         
        if (LOG.isTraceEnabled()) {
            StringBuilder terminalMessage = new StringBuilder();
            terminalMessage
                    .append("\n+---------------------------- DELIVERY ---------------------------+\n");
            terminalMessage.append(" Date: ");
            terminalMessage.append(TPCCUtil.getCurrentTime());
            terminalMessage.append("\n\n Warehouse: ");
            terminalMessage.append(w_id);
            terminalMessage.append("\n Carrier:   ");
            terminalMessage.append(o_carrier_id);
            terminalMessage.append("\n\n Delivered Orders\n");
            for (int i = d_lo_id; i <= d_hi_id; i++) {
                if (orderIDs[i - d_lo_id] >= 0) {
                    terminalMessage.append("  District ");
                    terminalMessage.append(i < 10 ? " " : "");
                    terminalMessage.append(i);
                    terminalMessage.append(": Order number ");
                    terminalMessage.append(orderIDs[i - d_lo_id]);
                    terminalMessage.append(" was delivered.\n");
                }
            } // FOR
            terminalMessage.append("+-----------------------------------------------------------------+\n\n");
            LOG.trace(terminalMessage.toString());
        }
    }

}
