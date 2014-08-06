package com.jd.orderpat.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class QueryOrderBy extends VoltProcedure {

	public final SQLStmt getOrderBy = new SQLStmt(
		"select  * from  orders_par order by ord_tm desc limit 10;"
	);
	
	public VoltTable[] run() throws VoltAbortException {
        // Add a SQL statement to the current execution queue
        voltQueueSQL(getOrderBy);

        // Run all queued queries.
        // Passing true parameter since this is the last voltExecuteSQL for this procedure.
        return voltExecuteSQL(true);
    }
}
