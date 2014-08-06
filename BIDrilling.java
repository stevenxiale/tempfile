package com.jd.userprofile.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class BIDrilling extends VoltProcedure {

	public final SQLStmt getDrilling = new SQLStmt(
		"SELECT  USER_LOG_ACCT,ITEM_FIRST_CATE_CD,a.ITEM_AMOUNT FROM (SELECT USER_LOG_ACCT,ITEM_FIRST_CATE_CD,SUM(SALE_QTTY) SALE_QTTY,SUM(ITEM_AMOUNT) ITEM_AMOUNT FROM USERPROFILE GROUP BY USER_LOG_ACCT, ITEM_FIRST_CATE_CD) a where a.ITEM_AMOUNT > 1000;"
	);
	
	public VoltTable[] run() throws VoltAbortException {
        // Add a SQL statement to the current execution queue
        voltQueueSQL(getDrilling);

        // Run all queued queries.
        // Passing true parameter since this is the last voltExecuteSQL for this procedure.
        return voltExecuteSQL(true);
    }
}
