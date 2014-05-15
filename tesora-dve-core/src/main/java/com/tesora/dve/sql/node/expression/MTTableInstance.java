// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

import com.tesora.dve.sql.expression.MTTableKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.mt.TableScope;
import com.tesora.dve.sql.transform.CopyContext;

public class MTTableInstance extends TableInstance {

	protected TableScope ts;

	public MTTableInstance(Table<?> tab, TableScope ts, Name origName, UnqualifiedName tableAlias, long n, boolean checknull) {
		super(tab, origName, tableAlias, n, checknull);
		this.ts = ts;
	}

	public MTTableInstance(Table<?> tab, TableScope ts, Name origName, UnqualifiedName tableAlias, boolean checknull) {
		this(tab, ts,origName, tableAlias, 0, checknull);
	}

	public MTTableInstance(Table<?> tab, TableScope ts, boolean checknull) {
		this(tab, ts, null, null, checknull);
	}
	
	public TableScope getTableScope() {
		return ts;
	}

	@Override
	protected LanguageNode copySelf(CopyContext cc) {
		if (cc == null) return new MTTableInstance(schemaTable, ts, specifiedAs, alias, node, false);
		TableInstance out = cc.getTableInstance(this);
		if (out != null) return out;
		out = new MTTableInstance(schemaTable, ts, specifiedAs, alias, node, false);
		return cc.put(this, out);
	}

	@Override
	public TableKey getTableKey() {
		return new MTTableKey(this);
	}

	@Override
	public TableInstance adapt(Name tableName, UnqualifiedName alias, long node, boolean checknull) {
		return new MTTableInstance(schemaTable, ts, tableName, alias, node, checknull);
	}
	
	public MTTableInstance postFlipAdapt(PETable actual, boolean checknull) {
		return new MTTableInstance(actual, ts, specifiedAs, alias, node, checknull);
	}
	
	@Override
	public void reload(SchemaContext sc) {
		super.reload(sc);
		ts = (TableScope) sc.getSource().find(sc, ts.getCacheKey());
	}
	
	@Override
	public boolean isMT() {
		return true;
	}
}
