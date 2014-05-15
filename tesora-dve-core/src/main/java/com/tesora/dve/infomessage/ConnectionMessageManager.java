// OS_STATUS: public
package com.tesora.dve.infomessage;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultRow;

/**
 * Handles some mgmt around info and warning messages
 * 
 */
public class ConnectionMessageManager {

	private final List<InformationMessage> messages;
	
	public ConnectionMessageManager() {
		messages = new ArrayList<InformationMessage>(1);
	}
	
	public short getNumberOfMessages() {
		if (messages.size() > Short.MAX_VALUE)
			return Short.MAX_VALUE;
		return (short)messages.size();
	}
	
	public void addWarning(String message) {
		messages.add(new InformationMessage(Level.WARNING, message));
	}
	
	public void clear() {
		messages.clear();
	}
	
	// eventually could do errors, infos, but not quite yet
	public IntermediateResultSet buildShow(Level atLeastLevel) {
		List<ResultRow> rows = new ArrayList<ResultRow>();
		accumulateResults(atLeastLevel,rows);
		if (rows.isEmpty()) {
			rows.add(new ResultRow().addResultColumn("Note")
					.addResultColumn(new Integer(9999))
					.addResultColumn("SHOW " + atLeastLevel.getSQLName() + " is not supported"));
		}
		
		return new IntermediateResultSet(buildColumnSet(), rows);
	}

	private ColumnSet buildColumnSet() {
		ColumnSet cs = new ColumnSet();
		cs.addColumn("Level", 7, "VARCHAR", Types.VARCHAR);
		cs.addColumn("Code", 4, "INT", Types.INTEGER);
		cs.addColumn("Message",512,"VARCHAR", Types.VARCHAR);
		return cs;
	}
	
	private void accumulateResults(Level atLeastLevel, List<ResultRow> rows) {
		for(InformationMessage im : messages) {
			if (im.atLeast(atLeastLevel)) {
				rows.add(new ResultRow().addResultColumn(im.getLevel().getResultSetName())
						.addResultColumn(im.getCode())
						.addResultColumn(im.getMessage()));
			}
		}
	}
	
}
