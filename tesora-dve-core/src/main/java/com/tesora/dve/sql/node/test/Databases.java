// OS_STATUS: public
package com.tesora.dve.sql.node.test;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.DerivedAttribute;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.util.ListSet;

public class Databases extends DerivedAttribute<ListSet<Database<?>>> {

	@Override
	public boolean isApplicableSubject(LanguageNode ln) {
		return (ln instanceof DMLStatement);
	}

	@Override
	public ListSet<Database<?>> computeValue(SchemaContext sc, LanguageNode ln) {
		ListSet<TableKey> tabs = EngineConstant.TABLES_INC_NESTED.getValue(ln,sc);
		ListSet<Database<?>> dbs = new ListSet<Database<?>>();
		for(TableKey tk : tabs) {
			dbs.add(tk.getTable().getDatabase(sc));
		}
		return dbs;
	}

}
