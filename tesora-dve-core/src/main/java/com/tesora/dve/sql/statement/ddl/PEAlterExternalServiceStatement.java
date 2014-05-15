// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEExternalService;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.ExternalServiceExecutionStep;
import com.tesora.dve.sql.util.Pair;

public class PEAlterExternalServiceStatement extends
		PEAlterStatement<PEExternalService> {
	List<Pair<Name,LiteralExpression>> newOptions = null;
	
	// this constructor takes a target with the new options already set
	public PEAlterExternalServiceStatement(PEExternalService target) {
		super(target, true);
	}

	public PEAlterExternalServiceStatement(PEExternalService target, List<Pair<Name,LiteralExpression>> newOptions) {
		super(target, true);
		
		this.newOptions = newOptions;
	}

	@Override
	protected PEExternalService modify(SchemaContext pc, PEExternalService backing)
			throws PEException {
		backing.parseOptions(pc, newOptions);
		return backing;
	}
	
	@Override
	protected ExecutionStep buildStep(SchemaContext pc) throws PEException {
		return new ExternalServiceExecutionStep(getDatabase(pc),
				getStorageGroup(pc), getRoot(), getAction(), getSQLCommand(pc),
				getDeleteObjects(pc), getCatalogObjects(pc), getInvalidationRecord(pc));
	}
	
	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		return CacheInvalidationRecord.GLOBAL;
	}

}
