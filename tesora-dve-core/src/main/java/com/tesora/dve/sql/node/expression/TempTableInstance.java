// OS_STATUS: public
package com.tesora.dve.sql.node.expression;

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
