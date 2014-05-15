// OS_STATUS: public
package com.tesora.dve.sql.node.test;


import com.tesora.dve.sql.node.DerivedAttribute;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.util.ListSet;

public class NestedQueries extends DerivedAttribute<ListSet<ProjectingStatement>> {

	@Override
	public boolean isApplicableSubject(LanguageNode ln) {
		return (ln instanceof DMLStatement);
	}

	@Override
	public ListSet<ProjectingStatement> computeValue(SchemaContext sc, LanguageNode ln) {
		DMLStatement dmls = (DMLStatement) ln;
		return dmls.getDerivedInfo().getAllNestedQueries();
	}

}
