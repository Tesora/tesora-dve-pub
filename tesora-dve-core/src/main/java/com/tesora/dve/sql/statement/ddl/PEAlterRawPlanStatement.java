package com.tesora.dve.sql.statement.ddl;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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
