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

import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.chbenchmark.queries.GenericQuery;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.types.TransactionStatus;

public class CHBenCHmarkWorker extends Worker<CHBenCHmark> {
    public CHBenCHmarkWorker(CHBenCHmark benchmarkModule, int id) {
        super(benchmarkModule, id);
    }

    @Override
    protected TransactionStatus executeWork(TransactionType nextTransaction) throws UserAbortException, SQLException {
        try {
            // The timeout mechanism is used in lieu of unimplemented statement cancellation
            int timeout = (int)((wrkld.getWorkloadState().getTestDeadline() - System.nanoTime())/1000000000l) + 1;
            if (timeout <= 0) return TransactionStatus.USER_ABORTED;

            GenericQuery proc = (GenericQuery) this.getProcedure(nextTransaction.getProcedureClass());
            proc.run(conn, this, timeout);
        } catch (ClassCastException e) {
            System.err.println("We have been invoked with an INVALID transactionType?!");
            throw new RuntimeException("Bad transaction type = "+ nextTransaction);
        }
        catch (SQLException ex) {
            State state = wrkld.getWorkloadState().getGlobalState();
            if (state == State.DONE || state == State.EXIT) {
                // Exceptions in these states are likely timeouts which we ignore
                return TransactionStatus.USER_ABORTED;
            }
            throw ex;
        }

        conn.commit();
        return (TransactionStatus.SUCCESS);

    }
}
