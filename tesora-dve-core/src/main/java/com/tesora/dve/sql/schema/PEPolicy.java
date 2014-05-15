// OS_STATUS: public
package com.tesora.dve.sql.schema;



import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.DynamicGroupClass;
import com.tesora.dve.common.catalog.DynamicPolicy;
import com.tesora.dve.common.catalog.IDynamicPolicy;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;

public class PEPolicy extends Persistable<PEPolicy, DynamicPolicy> implements IDynamicPolicy {

	private boolean strict;
	private LinkedHashMap<PolicyClass,PEPolicyClassConfig> classes;
	
	public PEPolicy(SchemaContext pc, Name name, boolean strict, List<PEPolicyClassConfig> givenClasses) {
		super(getPolicyKey(name.getUnquotedName().get()));
		this.strict = strict;
		this.classes = new LinkedHashMap<PolicyClass, PEPolicyClassConfig>();
		if (givenClasses != null) {
			for(PEPolicyClassConfig pepcc : givenClasses) {
				classes.put(pepcc.getPolicyClass(), pepcc);
			}
		}
		setName(name);
	}

	public boolean isStrict() {
		return strict;
	}
	
	public void setStrict(boolean v) {
		strict = v;
	}
	
	public Collection<PEPolicyClassConfig> getPolicyClasses() {
		return classes.values();
	}
	
	public PEPolicyClassConfig getConfig(PolicyClass pc) {
		return classes.get(pc);
	}
	
	public void replacePolicy(PEPolicyClassConfig newConfig) {
		PEPolicyClassConfig existing = classes.get(newConfig.getPolicyClass());
		if (existing != null) 
			newConfig.mergeDefaultsFrom(existing);
        classes.put(newConfig.getPolicyClass(),newConfig);
	}
	
	public void replacePolicy(List<PEPolicyClassConfig> ncs) {
		for(PEPolicyClassConfig p : ncs)
			replacePolicy(p);
	}
	
	public static PEPolicy load(DynamicPolicy dp, SchemaContext sc) {
		PEPolicy pep = (PEPolicy)sc.getLoaded(dp,getPolicyKey(dp.getName()));
		if (pep == null)
			pep= new PEPolicy(dp, sc);
		return pep;

	}
	
	private PEPolicy(DynamicPolicy pers, SchemaContext sc) {
		super(getPolicyKey(pers.getName()));
		sc.startLoading(this,pers);
		setName(new UnqualifiedName(pers.getName()));
		classes = new LinkedHashMap<PolicyClass, PEPolicyClassConfig>();
		if (pers.getAggregationClass() != null)
			classes.put(PolicyClass.AGGREGATE,new PEPolicyClassConfig(PolicyClass.AGGREGATE, pers.getAggregationClass()));
		if (pers.getSmallClass() != null)
			classes.put(PolicyClass.SMALL,new PEPolicyClassConfig(PolicyClass.SMALL, pers.getSmallClass()));
		if (pers.getMediumClass() != null)
			classes.put(PolicyClass.MEDIUM, new PEPolicyClassConfig(PolicyClass.MEDIUM, pers.getMediumClass()));
		if (pers.getLargeClass() != null)
			classes.put(PolicyClass.LARGE,new PEPolicyClassConfig(PolicyClass.LARGE, pers.getLargeClass()));
		strict = pers.getStrict();
		setPersistent(sc, pers, pers.getId());
		sc.finishedLoading(this, pers);
	}
	
	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return DynamicPolicy.class;
	}

	@Override
	protected DynamicPolicy lookup(SchemaContext sc) throws PEException {
		return sc.getCatalog().findPolicy(getName().get());
	}

	@Override
	protected DynamicPolicy createEmptyNew(SchemaContext sc) throws PEException {
		return new DynamicPolicy();
	}

	@Override
	protected void populateNew(SchemaContext sc, DynamicPolicy p) throws PEException {
		updateExisting(sc,p);
	}

	@Override
	protected void updateExisting(SchemaContext sc, DynamicPolicy p) throws PEException {
		p.setName(getName().get());
		p.setStrict(isStrict());
		for(PEPolicyClassConfig c : classes.values()) {
			if (c.getPolicyClass() == PolicyClass.AGGREGATE) {
				p.setAggregationClass(c.update(p.getAggregationClass()));
			} else if (c.getPolicyClass() == PolicyClass.SMALL) {
				p.setSmallClass(c.update(p.getSmallClass()));
			} else if (c.getPolicyClass() == PolicyClass.MEDIUM) {
				p.setMediumClass(c.update(p.getMediumClass()));
			} else if (c.getPolicyClass() == PolicyClass.LARGE) {
				p.setLargeClass(c.update(p.getLargeClass()));
			}
		}		
	}
	
	@Override
	protected Persistable<PEPolicy, DynamicPolicy> load(SchemaContext sc, DynamicPolicy p)
			throws PEException {
		return null;
	}

	@Override
	protected int getID(DynamicPolicy p) {
		return p.getId();
	}

	@Override
	protected String getDiffTag() {
		return "POLICY";
	}

	public static SchemaCacheKey<?> getPolicyKey(String n) {
		return new PolicyCacheKey(n);
	}
	
	private static class PolicyCacheKey extends SchemaCacheKey<PEPolicy> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		// uniquely identified by name
		private final String name;
		
		public PolicyCacheKey(String n) {
			super();
			name = n;
		}
		
		@Override
		public int hashCode() {
			return initHash(PolicyCacheKey.class,name.hashCode());
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof PolicyCacheKey) {
				PolicyCacheKey ock = (PolicyCacheKey) o;
				return name.equals(ock.name);
			}
			return false;
		}

		@Override
		public PEPolicy load(SchemaContext sc) {
			DynamicPolicy dp = sc.getCatalog().findPolicy(name);
			if (dp == null) return null;
			return PEPolicy.load(dp, sc);
		}

		@Override
		public String toString() {
			return "PEPolicy:" + name;
		}
		
	}

	@Override
	public DynamicGroupClass getAggregationClass() {
		return getGroupClass(PolicyClass.AGGREGATE);
	}

	@Override
	public DynamicGroupClass getSmallClass() {
		return getGroupClass(PolicyClass.SMALL);
	}

	@Override
	public DynamicGroupClass getMediumClass() {
		return getGroupClass(PolicyClass.MEDIUM);
	}

	@Override
	public DynamicGroupClass getLargeClass() {
		return getGroupClass(PolicyClass.LARGE);
	}

	private DynamicGroupClass getGroupClass(PolicyClass pc) {
		PEPolicyClassConfig configured = classes.get(pc);
		if (configured == null) return null;
		return configured.getDynamicClass();
	}
	
	@Override
	public boolean getStrict() {
		return strict;
	}
	
}
