package com.jd.orderpat.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class BIFilter extends VoltProcedure {

	public final SQLStmt getFilter = new SQLStmt(
		"select a.user_log_acct,a.sku_id,a.item_first_cate_cd from orders_par a where a.user_log_acct='paper1973' limit 10;"
	);
	
	public VoltTable[] run() throws VoltAbortException {
        // Add a SQL statement to the current execution queue
        voltQueueSQL(getFilter);

        // Run all queued queries.
        // Passing true parameter since this is the last voltExecuteSQL for this procedure.
        return voltExecuteSQL(true);
    }
}
