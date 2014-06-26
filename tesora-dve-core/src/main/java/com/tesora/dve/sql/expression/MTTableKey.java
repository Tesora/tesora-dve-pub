package com.tesora.dve.sql.expression;

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

import com.tesora.dve.sql.node.expression.MTTableInstance;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.mt.TableScope;

public class MTTableKey extends TableKey {

	protected TableScope scope;
	
	public MTTableKey(Table<?> tab, TableScope ts, long n) {
		super(tab, n);
		scope = ts;
	}
	
	MTTableKey(MTTableInstance ti) {
		super(ti);
		scope = ti.getTableScope();
	}

	@Override
	public TableInstance toInstance() {
		if (instance == null)
			instance = new MTTableInstance(getTable(),scope,null,null,node,true);
		return instance;
	}

	public TableScope getScope() {
		return scope;
	}
	
	@Override
	public void reload(SchemaContext sc) {
		super.reload(sc);
		scope = (TableScope) sc.getSource().find(sc, scope.getCacheKey());
	}
	
	@Override
	public Integer getAutoIncTrackerID() {
		return getScope().getAutoIncrementID();
	}

	@Override
	public SchemaCacheKey<?> getCacheKey() {
		return scope.getCacheKey();
	}	
	
	@Override
	public TableKey makeFrozen() {
		return new MTTableKey(getTable(),scope,getNode());
	}
}
