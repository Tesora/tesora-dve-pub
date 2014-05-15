// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.PERawPlan;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.schema.validate.ValidateResult;

public class PEAlterRawPlanStatement extends PEAlterStatement<PERawPlan> {

	private Boolean alterEnable;
	private String alterBody;
	private String newComment;
	
	public PEAlterRawPlanStatement(PERawPlan target, String comment, Boolean enable, String body) {
		super(target, true);
		alterEnable = enable;
		alterBody = body;
		newComment = comment;
	}

	@Override
	public void normalize(SchemaContext sc) {
		if (alterEnable != null && Boolean.FALSE.equals(alterEnable))
			return;
		List<ValidateResult> results = getTarget().get().validate(sc,false);
		// make sure we fail on errors
		for(ValidateResult vr : results) {
			if (vr.isError())
				throw new SchemaException(Pass.NORMALIZE,vr.getMessage(sc));
		}
	}

	
	@Override
	protected PERawPlan modify(SchemaContext sc, PERawPlan backing) throws PEException {
		if (alterEnable != null)
			backing.setEnabled(alterEnable.booleanValue());
		if (alterBody != null)
			backing.setPlan(alterBody);
		if (newComment != null)
			backing.setComment(newComment);
		return backing;
	}

	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		return new CacheInvalidationRecord(getTarget().getCacheKey(),InvalidationScope.LOCAL);
	}

}
