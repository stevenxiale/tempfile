package com.jd.userprofile.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class QueryOrderBy extends VoltProcedure {

	public final SQLStmt getOrderBy = new SQLStmt(
		"select top 10 * from  USERPROFILE order by ord_tm desc;"
	);
	
	public VoltTable[] run() throws VoltAbortException {
        // Add a SQL statement to the current execution queue
        voltQueueSQL(getOrderBy);

        // Run all queued queries.
        // Passing true parameter since this is the last voltExecuteSQL for this procedure.
        return voltExecuteSQL(true);
    }
}
