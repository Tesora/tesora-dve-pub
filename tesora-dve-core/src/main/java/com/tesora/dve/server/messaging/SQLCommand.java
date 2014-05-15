// OS_STATUS: public
package com.tesora.dve.server.messaging;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.worker.Worker;

// we need to support both regular statements and parameterized statements;
// this class just wraps that all up
public class SQLCommand implements Serializable {
	
	static AtomicLong nextId = new AtomicLong();
	long thisId = nextId.incrementAndGet();

	private static final long serialVersionUID = 1L;
	
	public static final SQLCommand EMPTY = new SQLCommand("");
	
	static final Pattern HAS_LIMIT_CLAUSE = Pattern.compile(".*\\blimit\\s+\\d+\\b.*", Pattern.CASE_INSENSITIVE);
	static final Pattern IS_SELECT_STATEMENT = Pattern.compile("\\s*select\\b.*", Pattern.CASE_INSENSITIVE);

	private GenericSQLCommand sql;
	private List<Object> parameters;
	private ProjectionInfo projection;
	private long referenceTime = 0;
	private int width = -1;
	private String insertPrefix = null;
	private String insertOptions = null;
	
	public SQLCommand(GenericSQLCommand command) {
		this(command, null);
	}

	public SQLCommand(GenericSQLCommand command, List<Object> params) {
		sql = command;
		parameters = params;
	}
	
	public SQLCommand(String regularStatement) {
		this(new GenericSQLCommand(regularStatement));
	}
	
	public SQLCommand withProjection(ProjectionInfo projection) {
		this.projection = projection;
		return this;
	}
	
	public SQLCommand withReferenceTime(long refTime) {
		this.referenceTime = refTime;
		return this;
	}
		
	public String getRawSQL() {
		return sql.getUnresolved();
	}

	public SQLCommand append(String toAppend) {
		if (toAppend == null) return this;
		return new SQLCommand(sql.modify(toAppend),parameters).withReferenceTime(referenceTime);
	}
	
	public SQLCommand stripForUpdate() {
		if (!Boolean.TRUE.equals(sql.isForUpdate()))
			return this;
		return new SQLCommand(sql.stripForUpdate(),parameters).withReferenceTime(referenceTime);
	}
	
	public String getSQL() {
		if (sql.hasLateResolution())
			throw new IllegalStateException("Per site resolution required but no worker available");
		return sql.getUnresolved();
	}
	
	public byte[] getSQLAsBytes() {
		if (sql.hasLateResolution())
			throw new IllegalStateException("Per site resolution required but no worker available");
		return sql.getUnresolvedAsBytes();
	}

	public String getSQL(Worker w) {
		return sql.resolve(w);
	}
	
	public byte[] getSQLAsBytes(Worker w) {
		return sql.resolveAsBytes(w);
	}

	public boolean isEmpty() {
		return sql == null || sql.getUnresolved().isEmpty();
	}
	
	public boolean isPreparedStatement() {
		return parameters != null;
	}
	
	public List<Object> getParameters() {
		return parameters;
	}
	
	public void setParameters(ResultRow row) {
		setParameters(Collections.singletonList(row));
	}
	
	public void setParameters(List<ResultRow> rows) {
		parameters = new ArrayList<Object>();
		for(ResultRow row : rows) {
			List<ResultColumn> columns = row.getRow();
			if (columns.size() == 0)
				throw new IllegalStateException("ResultRow has no columns");
			for (int i = 0; i < columns.size(); ++i)
				parameters.add(columns.get(i).getColumnValue());
		}
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + "("+thisId+"): " + " \"" + sql.getUnresolved() + "\": " + parameters;
	}

	public ProjectionInfo getProjection() {
		return projection;
	}

	public long getReferenceTime() {
		return referenceTime;
	}

	public boolean hasReferenceTime() {
		return referenceTime != 0;
	}
	
	public boolean isSelectStatement() {
		if (sql.isSelect() == null)
			return IS_SELECT_STATEMENT.matcher(sql.getUnresolved()).matches();
		else
			return sql.isSelect().booleanValue();
	}
	
	public boolean hasLimitClause() {
		if (sql.isLimit() == null)
			return HAS_LIMIT_CLAUSE.matcher(sql.getUnresolved()).matches();
		else
			return sql.isLimit().booleanValue();
	}
	
	public boolean isForUpdateStatement() {
		return (sql.isForUpdate() == null ? false : sql.isForUpdate().booleanValue());
	}

	public SQLCommand getResolvedCommand(Worker worker) {
		SQLCommand newCommand = new SQLCommand(new GenericSQLCommand(getSQLAsBytes(worker)));
		newCommand.parameters = parameters;
		newCommand.projection = projection;
		newCommand.referenceTime = referenceTime;
		return newCommand;
	}
	
	public int getWidth() {
		return width;
	}
	
	// used for insert redist ONLY
	public void setWidth(int v) {
		width = v;
	}

	public void setInsertPrefix(String insertPrefix) {
		this.insertPrefix = insertPrefix;
	}
	
	public String getInsertPrefix() {
		return insertPrefix;
	}

	public void setInsertOptions(String insertOptions) {
		this.insertOptions = insertOptions;
	}
	
	public String getInsertOptions() {
		return insertOptions;
	}
	
	public StatementType getStatementType() {
		return sql.getStatementType();
	}

	public String getDisplayForLog() {
		String stringToDisplay = getRawSQL();
		int lenToDisplay = Math.min(stringToDisplay.length(), 1024);
		if (lenToDisplay < stringToDisplay.length()) 
			stringToDisplay = stringToDisplay.substring(0, lenToDisplay) + "...";
		return stringToDisplay;
	}
}
