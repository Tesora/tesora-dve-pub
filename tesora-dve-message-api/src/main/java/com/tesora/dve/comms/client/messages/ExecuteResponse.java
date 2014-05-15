// OS_STATUS: public
package com.tesora.dve.comms.client.messages;

import com.tesora.dve.resultset.ColumnSet;

public class ExecuteResponse extends ResponseMessage {

	private static final long serialVersionUID = 1L;

	private transient String workerAddress;

	protected ColumnSet columns;
	protected long rowsAffected;
	protected long lastInsertedID;
	protected boolean hasResults;
	protected boolean inTransaction;
	protected short warnings;

	public ExecuteResponse(boolean hasResults, long rowCount, ColumnSet columns, long lastInsertedId, boolean inTransaction, short warnings) {
		setHasResults(hasResults);
		setRowsAffected(rowCount);
		setColumns(columns);
		setLastInsertedId(lastInsertedId);
		setInTransaction(inTransaction);
		setWarnings(warnings);
	}

	public ExecuteResponse(boolean hasResults, long rowCount, ColumnSet columns) {
		setHasResults(hasResults);
		setRowsAffected(rowCount);
		setColumns(columns);
	}

	public ExecuteResponse() {
	}

	public long getRowsAffected() {
		return this.rowsAffected;
	}

	public ColumnSet getColumns() {
		return (ColumnSet) this.columns;
	}

	public void setColumns(ColumnSet columns) {
		this.columns = columns;
	}

	public void setRowsAffected(long rowsAffected) {
		this.rowsAffected = rowsAffected;
	}

	public boolean hasResults() {
		return this.hasResults;
	}

	public void setHasResults(boolean hasResults) {
		this.hasResults = hasResults;
	}
	
	public long getLastInsertedId() {
		return this.lastInsertedID;
	}

	public void setLastInsertedId(long lastInsertedID) {
		this.lastInsertedID = lastInsertedID;
	}

	public boolean isInTransaction() {
		return this.inTransaction;
	}
	
	public void setInTransaction(boolean inTransaction) {
		this.inTransaction = inTransaction;
	}

	public void setWarnings(short w) {
		this.warnings = w;
	}
	
	public short getNumberOfWarnings() {
		return this.warnings;
	}
	
	@Override
	public MessageType getMessageType() {
		return MessageType.EXECUTE_RESPONSE;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

	public String from() {
		return workerAddress;
	}

	public ExecuteResponse from(String workerAddress) {
		this.workerAddress = workerAddress;
		return this;
	}
}
