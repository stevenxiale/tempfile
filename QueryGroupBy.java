package com.jd.orderpat.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class QueryGroupBy extends VoltProcedure {

	public final SQLStmt getTop = new SQLStmt(
		"select sku_id,sum(SALE_QTTY) from orders_par group by sku_id limit 10;"
	);
	
	public VoltTable[] run() throws VoltAbortException {
        // Add a SQL statement to the current execution queue
        voltQueueSQL(getTop);

        // Run all queued queries.
        // Passing true parameter since this is the last voltExecuteSQL for this procedure.
        return voltExecuteSQL(true);
    }
}
