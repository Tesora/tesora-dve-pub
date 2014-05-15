// OS_STATUS: public
package com.tesora.dve.sql.schema;

import java.util.List;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;

public class UnresolvedRangeDistributionVector extends DistributionVector {

	private final Name rangeName;
	
	public UnresolvedRangeDistributionVector(SchemaContext pc, Name rangeName,
			List<PEColumn> cols) {
		super(pc, cols, Model.RANGE);
		this.rangeName = rangeName;
	}

	public RangeDistributionVector resolve(SchemaContext pc, PEPersistentGroup group) {
		RangeDistribution rd = pc.findRange(rangeName, group.getName());
		if (rd == null)
			throw new SchemaException(Pass.SECOND, "No such range on group " + group.getName() + ": " + rangeName);
		return new RangeDistributionVector(pc,getColumns(pc),false,new VectorRange(pc,rd));
	}
	
}
