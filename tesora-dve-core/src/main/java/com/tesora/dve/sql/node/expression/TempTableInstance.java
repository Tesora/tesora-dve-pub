package com.tesora.dve.sql.node.expression;

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

import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.transform.CopyContext;

public class TempTableInstance extends TableInstance {

	public TempTableInstance(SchemaContext sc, TempTable tt) {
		this(sc,tt,null);
	}
	
	public TempTableInstance(SchemaContext sc, TempTable tt, UnqualifiedName alias) {
		super(tt,tt.getName(sc),alias,sc.getNextTable(),true);
	}
	
	private TempTableInstance(TempTableInstance tti) {
		super(tti.schemaTable,null,tti.alias,tti.node,true);
	}
	
	@Override
	public Name getSpecifiedAs(SchemaContext sc) {
		// always specified as itself, since it is by definition unique
		return schemaTable.getName(sc);
	}

	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		if (cc == null) return new TempTableInstance(this);
		TableInstance out = cc.getTableInstance(this);
		if (out != null) return out;
		out = new TempTableInstance(this);
		return cc.put(this, out);
	}

}
