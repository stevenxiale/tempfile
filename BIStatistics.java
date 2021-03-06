package com.jd.orderpat.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class BIStatistics extends VoltProcedure {

	public final SQLStmt getStatistics = new SQLStmt(
		"select item_first_cate_name,sum(SALE_QTTY) as amount1 from  orders_par  group by item_first_cate_name limit 10;"
	);
	
	public VoltTable[] run() throws VoltAbortException {
        // Add a SQL statement to the current execution queue
        voltQueueSQL(getStatistics);

        // Run all queued queries.
        // Passing true parameter since this is the last voltExecuteSQL for this procedure.
        return voltExecuteSQL(true);
    }
}
