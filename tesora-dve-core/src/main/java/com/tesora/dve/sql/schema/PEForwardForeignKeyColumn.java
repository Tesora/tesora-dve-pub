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

public class PEForwardForeignKeyColumn extends PEForwardKeyColumn {

	private UnqualifiedName columnName;
	private boolean isForward;
	
	public PEForwardForeignKeyColumn(PEKey pek, UnqualifiedName c, PEColumn targCol) {
		super(pek, c, null, -1L);
		columnName = targCol.getName().getUnqualified();
		isForward = false;
	}

	public PEForwardForeignKeyColumn(PEKey pek, UnqualifiedName c, UnqualifiedName missingColumn) {
		super(pek, c,null, -1L);
		columnName = missingColumn;
		isForward = true;
	}
	
	public PEKeyColumn resolve(PEColumn actual) {
		return new PEForeignKeyColumn(actual,columnName,isForward);
	}

}
