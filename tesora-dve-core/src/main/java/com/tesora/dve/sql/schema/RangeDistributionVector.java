// OS_STATUS: public
package com.tesora.dve.sql.schema;

import java.util.List;
import java.util.Set;

import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.distribution.RangeTableRelationship;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;

public class RangeDistributionVector extends DistributionVector {

	private VectorRange range;

	public RangeDistributionVector(SchemaContext pc, List<PEColumn> cols, boolean skipDVCheck, VectorRange range) {
		super(pc, cols, Model.RANGE, skipDVCheck);
		if (range == null) 
			throw new SchemaException(Pass.SECOND, "Missing range distribution for selected distribution by range");
		if (!skipDVCheck)
			range.getDistribution(pc).validate(pc,cols);
		this.range = range;
	}	

	public RangeDistributionVector(SchemaContext pc, PEAbstractTable<?> t, UserTable ut) {
		super(pc,t,ut,Model.RANGE);
		RangeTableRelationship rel = pc.getCatalog().findRangeTableRelationship(ut);
		this.range = VectorRange.load(rel, pc,t);
	}

	@Override
	public void setTable(SchemaContext sc, PEAbstractTable<?> t) {
		super.setTable(sc, t);
		range.setTable(sc,t);
	}

	@Override
	public VectorRange getRangeDistribution() {
		return range;
	}

	@Override
	public RangeDistribution getDistributedWhollyOnTenantColumn(SchemaContext sc) {
		if (columns.size() == 1 && columns.get(0).isTenantColumn() && range != null)
			return range.getDistribution(sc);
		return null;
	}

	@Override
	public String describe(SchemaContext sc) {
		return super.describe(sc) + " using range " + range.getName(); 
	}

	@Override
	protected void persistForTable(PEAbstractTable<?> pet, UserTable targ, SchemaContext sc) throws PEException {
		getRangeDistribution().persistTree(sc);
	}
	
	@Override
	public DistributionVector adapt(final SchemaContext sc, final PEAbstractTable<?> ontoTable) {
		RangeDistributionVector rdv = new RangeDistributionVector(sc, adaptColumns(sc,ontoTable), true, range.adapt(sc,ontoTable));
		rdv.setTable(sc, ontoTable);
		return rdv;
	}
	
	@Override
	public boolean collectDifferences(SchemaContext sc, List<String> messages,
			Persistable<DistributionVector, DistributionModel> oth, boolean first,
			@SuppressWarnings("rawtypes") Set<Persistable> visited) {
		
		if (visited.contains(this) && visited.contains(oth)) {
			return false;
		}
		visited.add(this);
		visited.add(oth);

		if (super.collectDifferences(sc, messages, oth, first, visited))
			return true;
		DistributionVector other = oth.get();
		if (maybeBuildDiffMessage(sc, messages, "range distribution", getRangeDistribution(), other.getRangeDistribution(), first, visited))
			return true;
		return false;
	}
}
	
