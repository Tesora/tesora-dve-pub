// OS_STATUS: public
package com.tesora.dve.sql.expression;

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
	
	public MTTableKey(MTTableInstance ti) {
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
}
