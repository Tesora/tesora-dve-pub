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

import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.schema.cache.IConstantExpression;

public class DeferredTColumnDatum extends TColumnDatumBase {

	// by doing a constant expression we can handle both autoincrements and 
	// trigger columns
	private final IConstantExpression deferred;
	
	protected DeferredTColumnDatum(PEColumn pec, IConstantExpression expr) {
		super(pec);
		this.deferred = expr;
	}

	@Override
	public Object getValue() {
		throw new SchemaException(Pass.PLANNER, "Illegal call to deferred column datum getValue - not bound yet");
	}

	@Override
	public Comparable<?> getValueForComparison() {
		throw new SchemaException(Pass.PLANNER, "Illegal call to deferred column datum getValueForComparison - not bound yet");
	}

	@Override
	public int hashCode() {
		throw new SchemaException(Pass.PLANNER, "Illegal call to deferred column datum hashCode - not bound yet");
	}

	@Override
	public TColumnDatumBase bind(ConnectionValues cv) {
		Object value = deferred.getValue(cv);
		return new TColumnDatum(column,value);
	}

		
}
