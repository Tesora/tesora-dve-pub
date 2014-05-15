// OS_STATUS: public
package com.tesora.dve.sql.node.test;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.DerivedAttribute;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.util.ListSet;

class DistributionVectors extends DerivedAttribute<ListSet<DistributionVector>> {

	@Override
	public boolean isApplicableSubject(LanguageNode ln) {
		return (ln instanceof DMLStatement);
	}

	@Override
	public ListSet<DistributionVector> computeValue(SchemaContext sc, LanguageNode ln) {
		ListSet<TableKey> tables = EngineConstant.TABLES_INC_NESTED.getValue(ln,sc);		
		ListSet<DistributionVector> dvs = new ListSet<DistributionVector>();
		for(TableKey tk : tables)
			dvs.add(tk.getAbstractTable().getDistributionVector(sc));
		return dvs;
	}

}
