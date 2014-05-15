// OS_STATUS: public
package com.tesora.dve.sql.node.test;


import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.DerivedAttribute;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.util.ListSet;

class Tables extends DerivedAttribute<ListSet<TableKey>> {

	private final boolean crossNested;
	
	public Tables(boolean cn) {
		super();
		crossNested = cn;
	}
	
	@Override
	public boolean isApplicableSubject(LanguageNode ln) {
		return (ln instanceof DMLStatement);
	}

	@Override
	public ListSet<TableKey> computeValue(SchemaContext sc, LanguageNode ln) {
		DMLStatement dmls = (DMLStatement) ln;
		if (crossNested)
			return dmls.getDerivedInfo().getAllTableKeys();
		return dmls.getDerivedInfo().getLocalTableKeys();
	}

}
