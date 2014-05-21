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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PESiteInstance;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.util.Pair;

public class PEAlterSiteInstanceStatement extends
		PEAlterStatement<PESiteInstance> {

	List<Pair<Name,LiteralExpression>> newOptions = null;

	public PEAlterSiteInstanceStatement(PESiteInstance target,
			List<Pair<Name,LiteralExpression>> newOptions) {
		super(target, true);
		
		this.newOptions = newOptions;
	}
	

	@Override
	protected PESiteInstance modify(SchemaContext pc, PESiteInstance backing) throws PEException {
		PECreateSiteInstanceStatement.parseOptions(pc, backing, newOptions, false);
		return backing;
	}


	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext pc) {
		return CacheInvalidationRecord.GLOBAL;
	}

}
