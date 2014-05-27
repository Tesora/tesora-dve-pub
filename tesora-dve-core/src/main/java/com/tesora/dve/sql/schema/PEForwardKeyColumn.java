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

import com.tesora.dve.common.catalog.KeyColumn;
import com.tesora.dve.exceptions.PEException;

public class PEForwardKeyColumn extends PEKeyColumnBase {

	protected UnqualifiedName forwardName;
	
	public PEForwardKeyColumn(PEKey key, UnqualifiedName forwardName, Integer length, long card) {
		super(key,length,card);
		this.forwardName = forwardName;
	}
	
	public PEKeyColumn resolve(PEColumn actual) {
		return new PEKeyColumn(actual,getLength(),getCardinality());
	}
	
	@Override
	public Name getName() {
		return forwardName;
	}
	
	
	@Override
	protected KeyColumn lookup(SchemaContext sc) throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected KeyColumn createEmptyNew(SchemaContext sc) throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void populateNew(SchemaContext sc, KeyColumn p)
			throws PEException {
		// TODO Auto-generated method stub

	}

	@Override
	protected Persistable<PEKeyColumn, KeyColumn> load(SchemaContext sc,
			KeyColumn p) throws PEException {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public boolean isForwardKeyColumn() {
		return true;
	}

}
