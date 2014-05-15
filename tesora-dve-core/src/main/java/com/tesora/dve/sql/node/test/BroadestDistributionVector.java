// OS_STATUS: public
package com.tesora.dve.sql.node.test;

import com.tesora.dve.sql.node.DerivedAttribute;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.util.ListSet;

class BroadestDistributionVector extends DerivedAttribute<DistributionVector> {

	@Override
	public boolean isApplicableSubject(LanguageNode ln) {
		return (ln instanceof DMLStatement);
	}

	@Override
	public DistributionVector computeValue(SchemaContext sc, LanguageNode ln) {
		ListSet<DistributionVector> dvs = EngineConstant.DISTRIBUTION_VECTORS.getValue(ln,sc);
		return DistributionVector.findMaximal(dvs);
	}

}
