// OS_STATUS: public
package com.tesora.dve.sql.schema;

import java.util.List;
import java.util.Set;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.distribution.RangeTableRelationship;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;

public class VectorRange extends Persistable<VectorRange, RangeTableRelationship> {

	private PEAbstractTable<?> table;
	private final SchemaEdge<RangeDistribution> range;
	
	private VectorRangeCacheKey override = null;
	
	@SuppressWarnings("unchecked")
	public VectorRange(SchemaContext pc, RangeDistribution rd) {
		super(null);
		table = null;
		range = StructuralUtils.buildEdge(pc,rd,false);
		setPersistent(pc,null,null);
	}

	private VectorRange(SchemaContext pc, SchemaEdge<RangeDistribution> rd, PEAbstractTable<?> ofTab) {
		super(buildRangeKey(ofTab));
		table = ofTab;
		range = rd;
	}
	
	public void setTable(SchemaContext sc, PEAbstractTable<?> t) {
		if (t.isTempTable())
			table = t;
		else if (range.get(sc).getStorageGroup(sc).equals(t.getPersistentStorage(sc)))
			table = t;
		else {
			throw new SchemaException(Pass.SECOND, "Range " + range.get(sc).getName() + " cannot be used for table " + t.getName() + " because they use different persistent groups");
		}
		override = buildRangeKey(t);
	}
	
	public PEAbstractTable<?> getTable(SchemaContext sc) {
		return table;
	}

	@Override
	public SchemaCacheKey<?> getCacheKey() {
		if (override != null) return override;
		return super.getCacheKey();
	}
	
	public VectorRange adapt(SchemaContext sc, PEAbstractTable<?> ontoTab) {
		VectorRange out = new VectorRange(sc, range, ontoTab);
		return out;
	}
	
	public RangeDistribution getDistribution(SchemaContext sc) {
		return range.get(sc);
	}
	
	public SchemaEdge<RangeDistribution> getDistributionEdge() {
		return range;
	}
	
	@SuppressWarnings("unchecked")
	private VectorRange(RangeTableRelationship rel, SchemaContext pc, PEAbstractTable<?> ofTable) {
		super((ofTable == null ? null : buildRangeKey(ofTable)));
		pc.startLoading(this, rel);
		this.table = ofTable; 
		this.range = StructuralUtils.buildEdge(pc,RangeDistribution.load(rel.getRange(), pc),true);
		setPersistent(pc, rel,rel.getId());
		pc.finishedLoading(this, rel);
	}
	
	public static VectorRange load(RangeTableRelationship rel, SchemaContext pc, PEAbstractTable<?> ofTable) {
		VectorRange vr = (VectorRange)pc.getLoaded(rel,buildRangeKey(ofTable));
		if (vr == null)
			vr = new VectorRange(rel, pc, ofTable);
		return vr;
	}
	
	@Override
	public boolean collectDifferences(SchemaContext sc, List<String> messages,
			Persistable<VectorRange, RangeTableRelationship> other,
			boolean first, @SuppressWarnings("rawtypes") Set<Persistable> visited) {
		return false;
	}

	@Override
	protected String getDiffTag() {
		return null;
	}

	@Override
	protected int getID(RangeTableRelationship p) {
		return p.getId();
	}

	@Override
	protected RangeTableRelationship lookup(SchemaContext pc) throws PEException {
		UserTable ut = table.persistTree(pc);
		if (pc.getLoaded(ut,PETable.getTableKey(ut)) != null) 
			return pc.getCatalog().findRangeTableRelationship(ut);
		return null;
	}

	@Override
	protected RangeTableRelationship createEmptyNew(SchemaContext pc) throws PEException {
		UserTable ut = table.persistTree(pc);
		RangeTableRelationship rtr = new RangeTableRelationship(ut, range.get(pc).persistTree(pc));
		pc.getSaveContext().add(this,rtr);
		return rtr;
	}

	@Override
	protected void populateNew(SchemaContext pc, RangeTableRelationship p) throws PEException {
	}

	@Override
	protected Persistable<VectorRange, RangeTableRelationship> load(SchemaContext pc, 
			RangeTableRelationship p) throws PEException {
		return new VectorRange(p,pc,null);
	}

	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return RangeTableRelationship.class;
	}
	
	@SuppressWarnings("unchecked")
	public static VectorRangeCacheKey buildRangeKey(PEAbstractTable<?> tab) {
		SchemaCacheKey<PETable> tabKey = (SchemaCacheKey<PETable>)tab.getCacheKey();
		if (tabKey == null) return null;
		return new VectorRangeCacheKey(tabKey);
	}
	
	public static class VectorRangeCacheKey extends SchemaCacheKey<VectorRange> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private final SchemaCacheKey<PETable> tableKey;
		
		public VectorRangeCacheKey(SchemaCacheKey<PETable> tck) {
			super();
			this.tableKey = tck;
		}
		
		@Override
		public int hashCode() {
			return initHash(VectorRange.class,tableKey.hashCode());
		}
		
		@Override
		public String toString() {
			return "VectorRange(" + tableKey.toString() + ")";
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof VectorRangeCacheKey) {
				VectorRangeCacheKey vrk = (VectorRangeCacheKey) o;
				return tableKey.equals(vrk.tableKey);
			}
			return false;
		}

		@Override
		public VectorRange load(SchemaContext sc) {
			throw new IllegalStateException("Invalid load: vector range");
		}
		
	}
}
