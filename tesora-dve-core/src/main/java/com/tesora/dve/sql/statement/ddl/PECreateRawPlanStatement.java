// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import java.util.List;

import com.tesora.dve.common.catalog.RawPlan;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.PERawPlan;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.schema.validate.ValidateResult;

public class PECreateRawPlanStatement extends
		PECreateStatement<PERawPlan, RawPlan> {

	public PECreateRawPlanStatement(PERawPlan thePlan,
			Boolean ine) {
		super(thePlan, true, ine, "RAW PLAN", false);
	}

	@Override
	public void normalize(SchemaContext pc) {
		List<ValidateResult> results = getCreated().get().validate(pc,false);
		// make sure we fail on errors
		for(ValidateResult vr : results) {
			if (vr.isError())
				throw new SchemaException(Pass.NORMALIZE,vr.getMessage(pc));
			else
				pc.getConnection().getMessageManager().addWarning(vr.getMessage(pc));
		}
	}
	
	
	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		return new CacheInvalidationRecord(getCreated().get().getCacheKey(), InvalidationScope.LOCAL);
	}

	
}
