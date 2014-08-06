package com.jd.userprofile.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class QueryDetails extends VoltProcedure {

	public final SQLStmt getGroupBy = new SQLStmt(
	    "select top 10 sku_id,sum(SALE_QTTY) from USERPROFILE group by sku_id;"
	);
	
	public VoltTable[] run() throws VoltAbortException {
        // Add a SQL statement to the current execution queue
        voltQueueSQL(getGroupBy);

        // Run all queued queries.
        // Passing true parameter since this is the last voltExecuteSQL for this procedure.
        return voltExecuteSQL(true);
    }
}
