// OS_STATUS: public
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
