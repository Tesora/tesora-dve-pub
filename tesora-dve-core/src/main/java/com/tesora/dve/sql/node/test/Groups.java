// OS_STATUS: public
package com.tesora.dve.sql.node.test;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.DerivedAttribute;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.util.ListSet;

class Groups extends DerivedAttribute<ListSet<PEStorageGroup>> {

	@Override
	public boolean isApplicableSubject(LanguageNode ln) {
		return (ln instanceof DMLStatement);
	}

	@Override
	public ListSet<PEStorageGroup> computeValue(SchemaContext sc, LanguageNode ln) {
		ListSet<TableKey> tables = EngineConstant.TABLES_INC_NESTED.getValue(ln,sc);
		ListSet<PEStorageGroup> out = new ListSet<PEStorageGroup>();
		for(TableKey tk : tables) {
			out.add(tk.getAbstractTable().getStorageGroup(sc));
		}
		return out;
	}

}
