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

import com.tesora.dve.common.catalog.PersistentColumn;
import com.tesora.dve.db.LateBoundConstants;
import com.tesora.dve.distribution.IColumnDatum;

public abstract class TColumnDatumBase implements IColumnDatum {

	private static final long serialVersionUID = 1L;
	protected final PEColumn column;
	protected String comparatorClassName;

	protected TColumnDatumBase(PEColumn pec) {
		this.column = pec;
	}
	
	
	@Override
	public String getComparatorClassName() {
		return comparatorClassName;
	}
	
	@Override
	public void setComparatorClassName(String comparatorClassName) {
		this.comparatorClassName = comparatorClassName;
	}

	@Override
	public String getNativeType() {
		return this.column.getType().getBaseType().getTypeName();
	}

	@Override
	public PersistentColumn getColumn() {
		return column;
	}

	@Override
	public abstract int hashCode();

	public abstract TColumnDatumBase bind(LateBoundConstants constants);
	
}
