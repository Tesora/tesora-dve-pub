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


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.Container;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentContainer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;

public class PEContainer extends Persistable<PEContainer, Container> implements PersistentContainer {

	private SchemaEdge<PETable> baseTable;
	private SchemaEdge<PEPersistentGroup> defaultStorageGroup;
	// the model on which the container is distributed
	private Model model;
	// and the range if applicable
	private SchemaEdge<RangeDistribution> range;
	
	@SuppressWarnings("unchecked")
	public PEContainer(SchemaContext pc, Name n, PEPersistentGroup defStorage,
			Model model, RangeDistribution anyRange) {
		super(getContainerKey(n));
		setName(n);
		this.defaultStorageGroup = StructuralUtils.buildEdge(pc,defStorage, false);
		this.baseTable = null;
		this.model = model;
		if (anyRange != null)
			range = StructuralUtils.buildEdge(pc,anyRange, false);
		else
			range = null;
		setPersistent(pc, null, null);
	}

	@SuppressWarnings("unchecked")
	public PEContainer(SchemaContext pc, Container container) {
		super(getContainerKey(container));
		pc.startLoading(this, container);
		setPersistent(pc, container, container.getId());
		UnqualifiedName dbn = new UnqualifiedName(container.getName());
		setName(dbn);
		if (container.getStorageGroup() != null)
			defaultStorageGroup = StructuralUtils.buildEdge(pc,
					PEPersistentGroup.load(container.getStorageGroup(), pc),
					true);
		if (container.getBaseTable() != null) 
			baseTable = StructuralUtils.buildEdge(pc,PETable.load(container.getBaseTable(), pc),true);
		this.model = Model.getModelFromPersistent(container.getDistributionModel().getName());
		if (container.getRange() != null)
			this.range = StructuralUtils.buildEdge(pc,RangeDistribution.load(container.getRange(), pc),true);
		pc.finishedLoading(this, container);
	}

	public static PEContainer load(Container container, SchemaContext pc) {
		PEContainer peContainer = (PEContainer) pc.getLoaded(container, getContainerKey(container));
		if (peContainer == null)
			peContainer = new PEContainer(pc, container);
		return peContainer;
	}

	public PEPersistentGroup getDefaultStorage(SchemaContext pc) {
		return defaultStorageGroup.get(pc);
	}

	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return Container.class;
	}

	@Override
	protected int getID(Container p) {
		return p.getId();
	}

	@Override
	protected Container lookup(SchemaContext sc) throws PEException {
		return sc.getCatalog().findContainer(getName().getUnqualified().get());
	}

	@Override
	protected Container createEmptyNew(SchemaContext sc) throws PEException {
		Map<String, DistributionModel> persistentModels = sc.getCatalog().getDistributionModelMap();

		Container container = new Container(name.getUnqualified().get(),
				defaultStorageGroup.get(sc).getPersistent(sc), persistentModels.get(model.getPersistentName()),
				(range == null ? null : range.get(sc).getPersistent(sc)));
		sc.getSaveContext().add(this, container);
		return container;
	}

	@Override
	protected void populateNew(SchemaContext sc, Container p)
			throws PEException {
		p.setName(p.getName());
		p.setStorageGroup(defaultStorageGroup.get(sc).getPersistent(sc));
		p.setBaseTable((baseTable != null) ? baseTable.get(sc).persistTree(sc) : null);
	}

	@Override
	protected Persistable<PEContainer, Container> load(SchemaContext sc,
			Container p) throws PEException {
		return PEContainer.load(p, sc);
	}

	@Override
	public Persistable<PEContainer, Container> reload(SchemaContext sc) throws PEException {
		Container pcont = sc.getCatalog().findContainer(getName().getUnqualified().get());
		return PEContainer.load(pcont, sc);
	}
	
	@Override
	protected void updateExisting(SchemaContext sc, Container p) throws PEException {
		PETable pet = getBaseTable(sc);
		if (p.getBaseTable() == null && pet != null)
			p.setBaseTable(pet.persistTree(sc));
		else if (p.getBaseTable() != null && pet == null)
			p.setBaseTable(null);
	}

	
	
	@Override
	protected String getDiffTag() {
		return "Container";
	}

	@Override
	public boolean collectDifferences(SchemaContext sc, List<String> messages, Persistable<PEContainer, Container> oth, boolean first, @SuppressWarnings("rawtypes") Set<Persistable> visited) {
		PEContainer other = oth.get();

		if (visited.contains(this) && visited.contains(other)) {
			return false;
		}
		visited.add(this);
		visited.add(other);

		if (maybeBuildDiffMessage(sc, messages, "name", getName(), other.getName(), first, visited))
			return true;
		if (defaultStorageGroup.get(sc).collectDifferences(sc, messages, other.getDefaultStorage(sc), first, visited))
			return true;
		if (maybeBuildDiffMessage(sc, messages, "model", getContainerDistributionModel(), other.getContainerDistributionModel(), first, visited))
			return true;
		if (getContainerDistributionModel() == DistributionVector.Model.RANGE &&
				getRange(sc).collectDifferences(sc, messages, other.getRange(sc), first, visited))
			return true;
		PETable mybase = getBaseTable(sc);
		PETable yourbase = other.getBaseTable(sc);
		if (mybase == null && yourbase == null) {
			// we're the same
		} else if (mybase != null && yourbase != null) {
			if (maybeBuildDiffMessage(sc, messages, "base table", mybase.getName(),yourbase.getName(), first, visited)) 
				return true;
		} else if (mybase != null) {
			messages.add("Missing base table");
			if (first)
				return true;
		} else {
			messages.add("Extra base table");
			if (first)
				return true;
		}
		return false;
	}

	public boolean hasBaseTable() {
		return (baseTable != null);
	}
	
	public PETable getBaseTable(SchemaContext sc) {
		if (baseTable == null) return null;
		return baseTable.get(sc);
	}

	public List<PEColumn> getDiscriminantColumns(SchemaContext sc) {
		TreeMap<Integer, PEColumn> cols = new TreeMap<Integer,PEColumn>();
		for(PEColumn pec : baseTable.get(sc).getColumns(sc)) {
			if (pec.isPartOfContainerDistributionVector())
				cols.put(pec.getContainerDistributionValuePosition(),pec);
		}
		return new ArrayList<PEColumn>(cols.values());
	}
	
	@SuppressWarnings("unchecked")
	public void setBaseTable(SchemaContext sc, PETable baseTable) {
		this.baseTable = StructuralUtils.buildEdge(sc,baseTable, false);
	}
	
	public Model getContainerDistributionModel() {
		return model;
	}	
	
	public RangeDistribution getRange(SchemaContext sc) {
		if (range == null) return null;
		return range.get(sc);
	}
	
	public static SchemaCacheKey<?> getContainerKey(Name n) {
		return new ContainerCacheKey(n.getUnqualified().getUnquotedName().get());
	}
	
	public static SchemaCacheKey<?> getContainerKey(Container c) {
		return new ContainerCacheKey(c.getName());
	}
	
	public static class ContainerCacheKey extends SchemaCacheKey<PEContainer> {

		private static final long serialVersionUID = 1L;
		private String name;
		
		public ContainerCacheKey(String n) {
			super();
			name = n;
		}
		
		@Override
		public int hashCode() {
			return initHash(PEContainer.class,name.hashCode());
		}
		
		@Override
		public String toString() {
			return "PEContainer:" + name;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof ContainerCacheKey) {
				ContainerCacheKey occk = (ContainerCacheKey) o;
				return name.equals(occk.name);
			}
			return false;
		}

		@Override
		public PEContainer load(SchemaContext sc) {
			Container cont = sc.getCatalog().findContainer(name);
			if (cont == null) return null;
			return PEContainer.load(cont, sc);
		}
		
	}

	@Override
	public DistributionModel getDistributionModel() {
		return model.getSingleton();
	}

}
