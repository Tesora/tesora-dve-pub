// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.connectionmanager.ConnectionInfo;
import com.tesora.dve.server.connectionmanager.PerHostConnectionManager;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.transform.execution.DDLQueryExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;

public class ShowProcesslistStatement extends SessionStatement {

	//TODO
	// - State isn't getting set for stuff that doesn't go thru SingleStatement
	
	/**
	 * @param pc
	 * @param full
	 */
	public ShowProcesslistStatement(boolean full) {
		super();
	}

	@Override
	public boolean isPassthrough() {
		return false;
	}

	@Override
	public void plan(SchemaContext pc, ExecutionSequence es) throws PEException {
		ColumnSet cs = new ColumnSet();
		cs.addColumn("Id", 11, "bigint", java.sql.Types.BIGINT);
		cs.addColumn("User", 48, "varchar", java.sql.Types.VARCHAR);
		cs.addColumn("Host", 192, "varchar", java.sql.Types.VARCHAR);
		cs.addNullableColumn("db", 192, "varchar", java.sql.Types.VARCHAR);
		cs.addColumn("Command", 48, "varchar", java.sql.Types.VARCHAR);
		cs.addColumn("Time", 7, "integer", java.sql.Types.INTEGER);
		cs.addNullableColumn("State", 90, "varchar", java.sql.Types.VARCHAR);
		cs.addNullableColumn("Info", 300, "varchar", java.sql.Types.VARCHAR);

		List<ResultRow> rows = new ArrayList<ResultRow>();
		for ( ConnectionInfo ci : PerHostConnectionManager.INSTANCE.getConnectionInfoList() ) {
			ResultRow rr = new ResultRow();
			rr.addResultColumn(ci.getConnectionId());
			rr.addResultColumn(ci.getUser());
			rr.addResultColumn(ci.getHost());
			rr.addResultColumn(ci.getDb());
			rr.addResultColumn(ci.getCommand());
			rr.addResultColumn(ci.getTimeinState());
			rr.addResultColumn(ci.getState());
			rr.addResultColumn(ci.getInfo());
			rows.add(rr);
		}
		
		IntermediateResultSet rs = new IntermediateResultSet(cs, rows);
		
		es.append(new DDLQueryExecutionStep("show processlist", rs));
	}

	@Override
	public StatementType getStatementType() {
		return StatementType.SHOW_PROCESSLIST;
	}

}
