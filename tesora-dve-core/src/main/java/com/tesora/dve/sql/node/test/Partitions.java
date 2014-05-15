// OS_STATUS: public
package com.tesora.dve.sql.node.test;

import com.tesora.dve.sql.jg.CollapsedJoinGraph;
import com.tesora.dve.sql.jg.UncollapsedJoinGraph;
import com.tesora.dve.sql.node.DerivedAttribute;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.InsertIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.MultiTableDMLStatement;

class Partitions extends DerivedAttribute<CollapsedJoinGraph> {

	@Override
	public boolean isApplicableSubject(LanguageNode ln) {
		return EngineConstant.FROMCLAUSE.has(ln) || EngineConstant.WHERECLAUSE.has(ln);
	}

	@Override
	public CollapsedJoinGraph computeValue(SchemaContext sc, LanguageNode ln) {
		MultiTableDMLStatement mdml = null;
		if (ln instanceof MultiTableDMLStatement)
			mdml = (MultiTableDMLStatement) ln;
		else if (ln instanceof InsertIntoSelectStatement) {
			InsertIntoSelectStatement iiss = (InsertIntoSelectStatement) ln;
			mdml = iiss.getSource();
		}
			
		UncollapsedJoinGraph ujg = new UncollapsedJoinGraph(sc,mdml);
		CollapsedJoinGraph cjg = new CollapsedJoinGraph(sc,ujg);
		
		return cjg;
	}

}
