package com.jd.userprofile.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class QueryGroupBy extends VoltProcedure {

	public final SQLStmt getTop = new SQLStmt(
		"SELECT TOP 10 " +
	    "DISTINCT user_log_acct " +
	    "FROM USERPROFILE; "
	);
	
	public VoltTable[] run() throws VoltAbortException {
        // Add a SQL statement to the current execution queue
        voltQueueSQL(getTop);

        // Run all queued queries.
        // Passing true parameter since this is the last voltExecuteSQL for this procedure.
        return voltExecuteSQL(true);
    }
}
