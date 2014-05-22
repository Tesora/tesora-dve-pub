package com.tesora.dve.sql.schema;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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
	
