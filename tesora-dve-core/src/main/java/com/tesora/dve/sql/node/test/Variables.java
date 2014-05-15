// OS_STATUS: public
package com.tesora.dve.sql.node.test;

import com.tesora.dve.sql.node.DerivedAttribute;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.util.ListSet;

class Variables extends DerivedAttribute<ListSet<VariableInstance>> {

	@Override
	public boolean isApplicableSubject(LanguageNode ln) {
		return (ln instanceof DMLStatement);
	}

	@Override
	public ListSet<VariableInstance> computeValue(SchemaContext sc, LanguageNode ln) {
		DMLStatement dmls = (DMLStatement) ln;
		return dmls.getDerivedInfo().getAllVariables();
	}

}
