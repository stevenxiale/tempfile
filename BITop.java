package com.jd.orderpat.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class BITop extends VoltProcedure {

	public final SQLStmt getTopN = new SQLStmt(
		"select user_log_acct,item_first_cate_cd,sum(SALE_QTTY) SALE_QTTY  from  orders_par  group by user_log_acct,item_first_cate_cd order by SALE_QTTY desc limit 10;"
	);
	
	public VoltTable[] run() throws VoltAbortException {
        // Add a SQL statement to the current execution queue
        voltQueueSQL(getTopN);

        // Run all queued queries.
        // Passing true parameter since this is the last voltExecuteSQL for this procedure.
        return voltExecuteSQL(true);
    }
}
