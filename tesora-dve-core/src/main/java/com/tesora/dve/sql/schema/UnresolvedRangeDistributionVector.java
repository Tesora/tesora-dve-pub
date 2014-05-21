// OS_STATUS: public
package com.tesora.dve.sql.schema;

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

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;

public class UnresolvedRangeDistributionVector extends DistributionVector {

	private final Name rangeName;
	
	public UnresolvedRangeDistributionVector(SchemaContext pc, Name rangeName,
			List<PEColumn> cols) {
		super(pc, cols, Model.RANGE);
		this.rangeName = rangeName;
	}

	public RangeDistributionVector resolve(SchemaContext pc, PEPersistentGroup group) {
		RangeDistribution rd = pc.findRange(rangeName, group.getName());
		if (rd == null)
			throw new SchemaException(Pass.SECOND, "No such range on group " + group.getName() + ": " + rangeName);
		return new RangeDistributionVector(pc,getColumns(pc),false,new VectorRange(pc,rd));
	}
	
}
