// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.statement.ddl.SchemaQueryStatement;

public class XARecoverTransactionStatement extends SchemaQueryStatement {

	public XARecoverTransactionStatement() {
		super(false, "", buildEmptyResultSet());
	}
	
	
	public static IntermediateResultSet buildEmptyResultSet() {
		ColumnSet cs = new ColumnSet();
		cs.addColumn("formatID", 21, "bigint", java.sql.Types.BIGINT);
		cs.addColumn("gtrid_length",21, "bigint", java.sql.Types.BIGINT);
		cs.addColumn("bqual_length",21, "bigint", java.sql.Types.BIGINT);
		cs.addColumn("data", 128, "varchar", java.sql.Types.VARCHAR);

		List<ResultRow> rows = new ArrayList<ResultRow>();
		return new IntermediateResultSet(cs, rows);
	}


}
