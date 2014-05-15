// OS_STATUS: public
package com.tesora.dve.sql.schema.cache;

import java.util.List;

import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;

public class CachedPreparedStatement implements CachedPlan {

	private final PlanCacheKey key;
	private final ExecutionPlan thePlan;
	private final List<TableKey> tables;
	private final GenericSQLCommand logFormat;
	
	public CachedPreparedStatement(PlanCacheKey pck, ExecutionPlan ep, List<TableKey> tabs, GenericSQLCommand logFormat) {
		this.key = pck;
		this.thePlan = ep;
		this.tables = tabs;
		this.logFormat = logFormat;
		thePlan.setOwningCache(this);
	}
	
	@Override
	public PlanCacheKey getKey() {
		return key;
	}

	@Override
	public boolean invalidate(SchemaCacheKey<?> unloaded) {
		for(TableKey tk : tables) {
			if (tk.getCacheKey().equals(unloaded))
				return true;
		}
		return false;
	}

	public ExecutionPlan rebuildPlan(SchemaContext sc, List<?> params) throws PEException {
		if (thePlan.getValueManager().getNumberOfParameters() != params.size()) {
			throw new PEException("Invalid prep. stmt. execute: require " + thePlan.getValueManager().getNumberOfParameters() + " parameters but have " + params.size());
		}
		thePlan.getValueManager().resetForNewPStmtExec(sc, params);
		if (InvokeParser.isSqlLoggingEnabled()) {
			GenericSQLCommand resolved = logFormat.resolve(sc, false, "  ");
			InvokeParser.logSql(sc, resolved.getUnresolved());
		}
		return thePlan;
	}

	public int getNumberOfParameters() {
		return thePlan.getValueManager().getNumberOfParameters();
	}
	
}
