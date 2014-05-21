// OS_STATUS: public
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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.util.ListSet;

public class ContainerDistributionVector extends DistributionVector {

	private SchemaEdge<PEContainer> container;
	
	@SuppressWarnings("unchecked")
	public ContainerDistributionVector(SchemaContext pc, PEContainer container, boolean skipDVCheck) {
		super(pc,Collections.EMPTY_LIST,DistributionVector.Model.CONTAINER,skipDVCheck);
		this.container = StructuralUtils.buildEdge(pc,container, false);
	}

	@SuppressWarnings("unchecked")
	public ContainerDistributionVector(SchemaContext pc, PEAbstractTable<?> t, UserTable ut) {
		super(pc,t,ut,Model.CONTAINER);
		this.container = StructuralUtils.buildEdge(pc,PEContainer.load(ut.getContainer(),pc),true);
	}
	
	@SuppressWarnings("unchecked")
	private ContainerDistributionVector(SchemaContext pc, SchemaEdge<PEContainer> cont) {
		super(pc,Collections.EMPTY_LIST,DistributionVector.Model.CONTAINER,true);
		this.container = cont;
	}
	
	@Override
	public PEContainer getContainer(SchemaContext pc) {
		return this.container.get(pc);
	}
	
	@Override
	public RangeDistribution getDistributedWhollyOnTenantColumn(SchemaContext sc) {
		// a container is distributed wholly on the tenant column if the backing container is range distributed
		return getContainer(sc).getRange(sc);
	}

	@Override
	protected void persistForTable(PEAbstractTable<?> pet, UserTable targ, SchemaContext sc) throws PEException {
		PEContainer cont = getContainer(sc);
		targ.setContainer(cont.persistTree(sc));
		if (cont.getContainerDistributionModel() == DistributionVector.Model.RANGE) {
			VectorRange vr = new VectorRange(sc, cont.getRange(sc));
			vr.setTable(sc, pet);
			vr.persistTree(sc);
		}
	}

	public Model getContainerDistributionModel(SchemaContext sc) {
		return getContainer(sc).getContainerDistributionModel();
	}

	@Override
	public boolean usesColumns(SchemaContext sc) {
		return getContainerDistributionModel(sc).getUsesColumns();
	}

	@Override
	public ListSet<PEColumn> getColumns(SchemaContext sc) {
		return getColumns(sc,false);
	}

	public ListSet<PEColumn> getColumns(SchemaContext sc, boolean init) {
		if (getContainerDistributionModel(sc).getUsesColumns()) {
			ListSet<PEColumn> out = new ListSet<PEColumn>();
			// tenant column may not have been injected yet
			PEColumn tenantColumn = getTable().getTenantColumn(sc,init);
			if (tenantColumn != null)
				out.add(tenantColumn);
			return out;
		}
		return super.getColumns(sc);
	}
	
	@Override
	protected boolean comparableJoinForDistribution(SchemaContext pc, DistributionVector other, Map<PEColumn, PEColumn> joinedColumns, boolean schemaOnly) {
		// different containers - never comparable
		ContainerDistributionVector ocdv = (ContainerDistributionVector) other;
		if (!ocdv.getContainer(pc).equals(getContainer(pc)))
			return false;
		if (schemaOnly || pc.getPolicyContext().getCurrentTenant() != null) {
			// schema only test (during create table no tenant is needed) or
			// the container is set to a particular tenant or the global tenant.
			return true;
		} else {
			// no container context set, fallback to the default
			return super.comparableJoinForDistribution(pc,other,joinedColumns, schemaOnly);
		}
	}
	
	@Override
	public boolean hasJoinRequiredColumns(SchemaContext pc, ListSet<PEColumn> joinCols) {
		// if this is a container context - the join works regardless of the columns
		if (pc.getPolicyContext().getCurrentTenant() != null)
			return true;
		else
			return super.hasJoinRequiredColumns(pc, joinCols);
	}
	
	@Override
	public DistributionVector adapt(final SchemaContext sc, final PEAbstractTable<?> ontoTable) {
		ContainerDistributionVector cdv = new ContainerDistributionVector(sc,container);
		cdv.setTable(sc, ontoTable);
		return cdv;
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
		ContainerDistributionVector other = (ContainerDistributionVector) oth.get();
		if (maybeBuildDiffMessage(sc,messages, "container", getContainer(sc), other.getContainer(sc), first, visited))
			return true;
		return false;
	}
}
