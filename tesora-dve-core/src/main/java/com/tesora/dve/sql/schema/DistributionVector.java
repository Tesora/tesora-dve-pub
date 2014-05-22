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


import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.distribution.ColumnDatum;
import com.tesora.dve.distribution.ContainerDistributionModel;
import com.tesora.dve.distribution.IColumnDatum;
import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.distribution.KeyTemplate;
import com.tesora.dve.distribution.KeyValue;
import com.tesora.dve.distribution.RandomDistributionModel;
import com.tesora.dve.distribution.RangeDistributionModel;
import com.tesora.dve.distribution.RangeLimit;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.schema.types.DBEnumType;
import com.tesora.dve.sql.transform.MatchableKey;
import com.tesora.dve.sql.transform.constraints.PlanningConstraint;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

public class DistributionVector extends Persistable<DistributionVector, DistributionModel> implements MatchableKey {

	public enum Model {
		BROADCAST("BROADCAST",BroadcastDistributionModel.MODEL_NAME, "BroadcastDistributionModel",BroadcastDistributionModel.SINGLETON,false,1), 
		RANDOM("RANDOM",RandomDistributionModel.MODEL_NAME, "RandomDistributionModel", RandomDistributionModel.SINGLETON,false,2), 
		STATIC("STATIC",StaticDistributionModel.MODEL_NAME, "StaticDistributionModel", StaticDistributionModel.SINGLETON,true,5), 
		RANGE("RANGE",RangeDistributionModel.MODEL_NAME, "RangeDistributionModel", RangeDistributionModel.SINGLETON,true,3),
		CONTAINER("CONTAINER",ContainerDistributionModel.MODEL_NAME,"ContainerDistributionModel",ContainerDistributionModel.SINGLETON,false,4);
		private final String sqlName;
		private final String persistentName;
		private final boolean hasColumns;
		private final int priority;
		private final String codeName;
		private final DistributionModel singleton;
		private Model(String name, String persistName, String className, DistributionModel single, boolean usesColumns, int p) {
			sqlName = name;
			persistentName = persistName;
			hasColumns = usesColumns;
			priority = p;
			codeName = className;
			singleton = single;
		}
		
		public String getSQL() { return sqlName; }
		public String getPersistentName() { return persistentName; }
		public String getCodeName() { return codeName; }
		public boolean getUsesColumns() { return hasColumns; }
		public int getPriority() { return priority; }
		public DistributionModel getSingleton() { return singleton; }
		
		public static Model getModelFromSQL(String in) {
			for(Model m : Model.values()) {
				if (in.equals(m.getSQL())) {
					return m;
				} else if (in.toUpperCase(Locale.ENGLISH).equals(m.getSQL())) {
					return m;
				}
			}
			return null;
		}
		
		public static Model getModelFromPersistent(String in) {
			for(Model m : Model.values()) {
				if (in.equals(m.getPersistentName()))
					return m;
			}
			return null;
		}		

		public static Model orderByPriority(Model a, Model b) {
			return (priorityComparator.compare(a,b) >= 0 ? a : b);
		}				
		
		public static final Comparator<Model> priorityComparator = new Comparator<Model>() {

			@Override
			public int compare(Model o1, Model o2) {
				if (o1.getPriority() > o2.getPriority())
					return 1;
				else if (o1.getPriority() < o2.getPriority())
					return -1;
				else
					return 0;
			}
			
		};
	}
	
	public static DistributionVector buildDistributionVector(
			final SchemaContext sc, final Model model,
			final List<PEColumn> components, final Name rangeOrContainer) {
		if (model == DistributionVector.Model.RANGE) {
			if (rangeOrContainer == null)
				throw new SchemaException(Pass.SECOND, "Range distribution requires named range");
			return new UnresolvedRangeDistributionVector(sc, rangeOrContainer, components);
		} else if (model == DistributionVector.Model.CONTAINER) {
			if (rangeOrContainer == null)
				throw new SchemaException(Pass.SECOND, "Container distribution requires named container");
			PEContainer cont = sc.findContainer(rangeOrContainer);
			if (cont == null)
				throw new SchemaException(Pass.SECOND, "No such container: '" + rangeOrContainer.getSQL() + "'");
			return new ContainerDistributionVector(sc, cont, false);
		}

		return new DistributionVector(sc, components, model, false);
	}

	protected ListSet<PEColumn> columns;
	protected Model model;
	protected PEAbstractTable<?> ofTable;
	
	public DistributionVector(SchemaContext pc, List<PEColumn> cols, Model model) {
		this(pc, cols, model, false);
	}
	
	// set skipDVCheck to true for cases when we are sure we don't need the checking, i.e. empty static dist vector
	public DistributionVector(SchemaContext pc, List<PEColumn> cols, Model model, boolean skipDVCheck) {
		super(null);
		this.columns = new ListSet<PEColumn>();
		if (!skipDVCheck && !model.getUsesColumns() && cols != null && !cols.isEmpty()) {
			throw new SchemaException(Pass.SECOND, "Distribution columns specified for a model that does not use them.");
		} else if (!skipDVCheck && model.getUsesColumns() && (cols == null || cols.isEmpty())) {
			throw new SchemaException(Pass.SECOND, "Missing distribution columns");
		}	
		if (cols != null) {
			this.columns.addAll(cols);
			int position = 1;
			for(PEColumn c : cols) {
				c.setDistributionValuePosition(position);
				position++;
				if (c.getType() instanceof DBEnumType) {
					throw new SchemaException(Pass.SECOND, "Invalid distribution column type: " + c.getType().getTypeName());
				}
			}
		}
		this.model = model;
		setPersistent(pc,null,null);
		this.ofTable = null;
	}
	
	public static DistributionVector load(SchemaContext pc, PEAbstractTable<?> t, UserTable ut) {
		Model m = Model.getModelFromPersistent(ut.getDistributionModel().getName());
		if (m == Model.RANGE)
			return new RangeDistributionVector(pc, t, ut);
		else if (m == Model.CONTAINER)
			return new ContainerDistributionVector(pc, t, ut);
		else
			return new DistributionVector(pc, t, ut, m);
	}
	
	protected DistributionVector(SchemaContext pc, PEAbstractTable<?> t, UserTable ut, Model m) {
		super(null);
		this.columns = new ListSet<PEColumn>();
		this.model = m;
		TreeMap<Integer, PEColumn> buf = new TreeMap<Integer, PEColumn>();
		for(PEColumn c : t.getColumns(pc,true)) {
			if (c.isPartOfDistributionVector())
				buf.put(new Integer(c.getDistributionValuePosition()), c);
		}
		this.columns.addAll(buf.values());
		this.ofTable = t;
		setPersistent(pc,ut.getDistributionModel(), ut.getDistributionModel().getId());
	}
	
	@Override
	public ListSet<PEColumn> getColumns(SchemaContext sc) { 
		return columns; 
	}
	
	public ListSet<PEColumn> getColumns(SchemaContext sc, boolean init) {
		return columns;
	}
	
	// for alter 
	public void setColumns(ListSet<PEColumn> cols) {
		this.columns.clear();
		this.columns.addAll(cols);
		int position = 1;
		for(PEColumn c : cols) {
			c.setDistributionValuePosition(position);
			position++;
			if (c.getType() instanceof DBEnumType) {
				throw new SchemaException(Pass.SECOND, "Invalid distribution column type: " + c.getType().getTypeName());
			}
		}
	}
	
	public String describeVector(SchemaContext sc) {
		if (getColumns(sc).isEmpty())
			return null;
		return Functional.join(getColumns(sc), ",", new UnaryFunction<String,PEColumn>() {

			@Override
			public String evaluate(PEColumn object) {
				return object.getName().getUnqualified().get();
			}
			
		});
	}
	
	public Model getModel() { return this.model; }
	
	public boolean usesColumns(SchemaContext sc) {
		return model.getUsesColumns();
	}
	
	public void setTable(SchemaContext sc, PEAbstractTable<?> t) {
		ofTable = t;
	}
	
	public PEAbstractTable<?> getTable() {
		return ofTable;
	}
		
	public VectorRange getRangeDistribution() {
		return null;
	}
	
	public PEContainer getContainer(SchemaContext sc) {
		return null;
	}
	
	public RangeDistribution getDistributedWhollyOnTenantColumn(SchemaContext sc) {
		return null;
	}
	
	public boolean usesTenantColumn(SchemaContext sc) {
		for(PEColumn c : getColumns(sc))
			if (c.isTenantColumn())
				return true;
		return false;
	}
	
	public boolean isComplete(SchemaContext sc, Column<?> c) {
		ListSet<PEColumn> tmp = getColumns(sc);
		return (tmp.size() == 1 && tmp.contains(c));
	}
	
	public boolean isComplete(SchemaContext sc, Set<PEColumn> cols, boolean partialOk) {
		HashSet<PEColumn> copy = new HashSet<PEColumn>(getColumns(sc));
		copy.removeAll(cols);
		return copy.isEmpty();
	}
	
	public boolean comparableForDistribution(SchemaContext pc, DistributionVector other, Map<PEColumn, PEColumn> joinedColumns, boolean schemaOnly) {
		if (isBroadcast() || other.isBroadcast())
			return true;
		// must have same distribution model 
		if (!model.equals(other.getModel()))
			return false;
		// unless the model is random
		if (model.equals(Model.RANDOM))
			return false;
		// or range but on different ranges
		if (model.equals(Model.RANGE) && !getRangeDistribution().getDistribution(pc).equals(other.getRangeDistribution().getDistribution(pc))) 
			return false;
		if (getColumns(pc).size() != other.getColumns(pc).size())
			return false;
		return comparableJoinForDistribution(pc, other, joinedColumns, schemaOnly);
	}

	// don't call this except from comparableForDistribution - we rely on 'sameness'
	protected boolean comparableJoinForDistribution(SchemaContext pc, DistributionVector other, Map<PEColumn, PEColumn> joinedColumns, boolean schemaOnly) {
		// if this is the tenant distribution (i.e. both are distributed on mtid) and we are in mt mode, then
		// say we're colocated.
		if (schemaOnly || (pc.getPolicyContext().isSchemaTenant() || pc.getPolicyContext().isDataTenant())) {
			// we already did the range test - so just verify we're both distributed on the tenant column
			if (getColumns(pc).size() == 1 && other.getColumns(pc).size() == 1 &&
					getColumns(pc).get(0).isTenantColumn() && other.getColumns(pc).get(0).isTenantColumn())
				return true;
		}
		
		for(PEColumn mc : getColumns(pc)) {
			PEColumn oc = joinedColumns.get(mc);
			if (oc == null || !mc.comparableForDistribution(pc, oc))
				return false;
		}
		return true;
	}
	

	public boolean hasJoinRequiredColumns(SchemaContext pc, ListSet<PEColumn> joinCols) {
		HashSet<PEColumn> mycols = new HashSet<PEColumn>(getColumns(pc));
		mycols.removeAll(joinCols);
		return mycols.isEmpty();
	}
	
	public boolean contains(SchemaContext pc, PEColumn c) {
		return getColumns(pc).contains(c);
	}
	
	public boolean isBroadcast() {
		return model == Model.BROADCAST;
	}
	
	public boolean isRandom() {
		return model == Model.RANDOM;
	}
	
	public boolean isRange() {
		return model == Model.RANGE;
	}
	
	public boolean isContainer() {
		return model == Model.CONTAINER;
	}
	
	public boolean mustRedistributeForJoin() {
		return isRandom() || isRange();
	}
	
	public KeyTemplate buildKeyTemplate(SchemaContext pc) {
		KeyTemplate kt = new KeyTemplate();
		for(PEColumn c : getColumns(pc))
			kt.add(c.getPersistent(pc));
		return kt;
	}
	
	public KeyValue buildEmptyKeyValue(SchemaContext pc) {
		// only for models that don't use columns
		if (usesColumns(pc))
			return null;
		return new KeyValue(ofTable.getPersistent(pc));
	}
	
	public List<PEColumn> getDistributionTemplate(SchemaContext sc) {
		return Functional.toList(getColumns(sc));
	}

	@Override
	public PlanningConstraint buildEmptyConstraint(SchemaContext sc, TableKey tk, ListOfPairs<PEColumn,ConstantExpression> values) {
		return buildDistKey(sc,tk,values);
	}

	@Override
	public long getCardRatio(SchemaContext sc) {
		return 0;
	}
	
	public DistributionKey buildDistKey(SchemaContext sc, TableKey tk, ListOfPairs<PEColumn,ConstantExpression> values) {
		return new DistributionKey(tk, getDistributionTemplate(sc),values);
	}
	
	
	public String describe(SchemaContext sc) {
		StringBuffer buf = new StringBuffer(32);
		buf.append("Distribution model ").append(model.getSQL());
		if (ofTable != null)
			buf.append(" on table ").append(ofTable.getName(sc));
		if (usesColumns(sc)) {
			buf.append(" columns { ");
			buf.append(Functional.join(getColumns(sc), ",", new UnaryFunction<String,PEColumn>() {

				@Override
				public String evaluate(PEColumn object) {
					return object.getName().get();
				}
				
			}));
			buf.append(" }");
		}
		return buf.toString();
	}
	
	
	@Override
	public Name getName() {
		throw new IllegalStateException("DistributionVectors do not have names");
	}

	@Override
	public boolean collectDifferences(SchemaContext sc, List<String> messages,
			Persistable<DistributionVector, DistributionModel> oth, boolean first, @SuppressWarnings("rawtypes") Set<Persistable> visited) {
		DistributionVector other = oth.get();
		
		if (visited.contains(this) && visited.contains(other)) {
			return false;
		}
		visited.add(this);
		visited.add(other);
		
		Model omodel = (other == null ? null : other.getModel());
		if (maybeBuildDiffMessage(sc, messages, "distribution model", getModel(), omodel, first, visited))
			return true;
		if (maybeBuildDiffMessage(sc, messages, "uses columns", usesColumns(sc), other.usesColumns(sc), first, visited))
			return true;
		return false;
	}
	
	@Override
	protected String getDiffTag() {
		return "DistributionVector";
	}

	// build a unary key value
	public KeyValue build(SchemaContext pc,Object v) {
		if (getColumns(pc).size() > 1)
			throw new SchemaException(Pass.PLANNER, "Too many values");
		else if (ofTable == null)
			throw new SchemaException(Pass.PLANNER, "No table for distribution vector");
		KeyValue kv = new KeyValue(ofTable.getPersistent(pc));
		PEColumn single = getColumns(pc).iterator().next();
		kv.put(single.getPersistent(pc).getName(), new ColumnDatum(single, v));
		return kv;
	}

	@Override
	public Persistable<DistributionVector, DistributionModel> reload(
			SchemaContext usingContext) {
		throw new IllegalStateException("Cannot reload a lone distribution vector");
	}

	@Override
	protected int getID(DistributionModel p) {
		return p.getId();
	}

	@Override
	protected DistributionModel lookup(SchemaContext sc) throws PEException {
		Map<String, DistributionModel> persistentModels = sc.getCatalog().getDistributionModelMap();
		return persistentModels.get(model.getPersistentName());
	}

	public static DistributionModel buildModel(Model in) throws PEException {
		DistributionModel backing = null;
		if (in == Model.BROADCAST)
			backing = new BroadcastDistributionModel();
		else if (in == Model.RANDOM)
			backing = new RandomDistributionModel();
		else if (in == Model.RANGE)
			backing = new RangeDistributionModel();
		else if (in == Model.STATIC)
			backing = new StaticDistributionModel();
		else if (in == Model.CONTAINER)
			backing = new ContainerDistributionModel();
		else 
			throw new PEException("Unknown distribution model: " + in.getSQL());
		return backing;
	}
	
	@Override
	protected DistributionModel createEmptyNew(SchemaContext sc) throws PEException {
		DistributionModel backing = buildModel(model);
		sc.getCatalog().persistToCatalog(backing);
		return backing;
	}

	@Override
	protected void populateNew(SchemaContext sc, DistributionModel p) throws PEException {
	}

	@Override
	protected Persistable<DistributionVector, DistributionModel> load(SchemaContext sc, 
			DistributionModel p) throws PEException {
		return null;
	}

	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return DistributionModel.class;
	}	

	protected List<PEColumn> adaptColumns(final SchemaContext sc, final PEAbstractTable<?> ontoTable) {
		return Functional.apply(columns, new UnaryFunction<PEColumn,PEColumn>() {

			@Override
			public PEColumn evaluate(PEColumn object) {
				return (PEColumn) object.getIn(sc, ontoTable);
			}
			
		});
	}
	
	public DistributionVector adapt(final SchemaContext sc, final PEAbstractTable<?> ontoTable) {
		DistributionVector dv = new DistributionVector(sc,adaptColumns(sc,ontoTable),model,true);
		dv.setTable(sc,ontoTable);
		return dv;
	}
	
	protected void persistForTable(PEAbstractTable<?> pet, UserTable targ, SchemaContext sc) throws PEException {
	}

	public static DistributionVector orderByPriority(DistributionVector a, DistributionVector b) {
		Model m = Model.orderByPriority(a.getModel(), b.getModel());
		if (m == a.getModel()) return a;
		return b;
	}
	
	public static final Comparator<DistributionVector> orderByModel = new Comparator<DistributionVector>() {

		@Override
		public int compare(DistributionVector o1, DistributionVector o2) {
			return Model.priorityComparator.compare(o1.getModel(), o2.getModel());
		}
		
	};
	
	public static DistributionVector findMaximal(Collection<DistributionVector> dvs) {
		DistributionVector odv = null;
		for(DistributionVector dv : dvs) {
			if (odv == null) {
				odv = dv;
				continue;
			} else {
				odv = DistributionVector.orderByPriority(odv, dv);
			}
		}
		return odv;
	}

	public IKeyValue getKeyValue(SchemaContext sc) {
		return new DetachedKeyValue(sc,this);
	}

	private static class DetachedKeyValue implements IKeyValue {

		private SchemaContext context;
		private DistributionVector dv;

		public DetachedKeyValue(SchemaContext sc, DistributionVector distVect) {
			context = sc;
			dv = distVect;
		}
		
		@Override
		public int getUserTableId() {
			return dv.getTable().getPersistentID();
		}

		@Override
		public String getQualifiedTableName() {
			return dv.getTable().getQualifiedPersistentName(context);
		}

		@Override
		public int compare(IKeyValue other) throws PEException {
			return KeyValue.compare(this,other);
		}

		@Override
		public int compare(RangeLimit rangeLimit) throws PEException {
			return KeyValue.compare(this, rangeLimit);
		}

		@Override
		public PersistentGroup getPersistentGroup() {
			return dv.getTable().getPersistentStorage(context).getPersistent(context);
		}
		
		@Override
		public int getPersistentGroupId() {
			return dv.getTable().getPersistentStorage(context).getPersistentID();
		}
		
		@Override
		public DistributionModel getDistributionModel() {
			return dv.getModel().getSingleton();
		}


		@Override
		public LinkedHashMap<String, ? extends IColumnDatum> getValues() {  //NOPMD
			return new LinkedHashMap<String, IColumnDatum>();
		}
		

		@Override
		public int hashCode() {
			return KeyValue.buildHashCode(this);
		}

		@Override
		public DistributionModel getContainerDistributionModel() {
			if (!dv.isContainer()) return null;
			return ((ContainerDistributionVector) dv).getContainerDistributionModel(context).getSingleton();
		}


	}

}
