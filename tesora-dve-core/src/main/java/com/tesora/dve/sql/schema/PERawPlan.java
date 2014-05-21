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

import java.util.Iterator;
import java.util.List;

import com.tesora.dve.common.PEXmlUtils;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.RawPlan;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.raw.RawToExecConverter;
import com.tesora.dve.sql.raw.jaxb.Rawplan;
import com.tesora.dve.sql.schema.cache.CacheSegment;
import com.tesora.dve.sql.schema.cache.RegularCachedPlan;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.cache.SchemaSourceFactory;
import com.tesora.dve.sql.schema.validate.SimpleValidateResult;
import com.tesora.dve.sql.schema.validate.ValidateResult;

public class PERawPlan extends Persistable<PERawPlan, RawPlan> {

	protected String comment;
	protected Rawplan thePlan;
	protected SchemaEdge<PEDatabase> db;
	protected boolean enabled;
	protected String planCacheKey;
	
	public PERawPlan(SchemaContext sc, UnqualifiedName planName, PEDatabase db, String body, boolean active, String comment) {
		super(getRawPlanKey(planName));
		setName(planName);
		setComment(comment);
		setDatabase(sc,db,false);
		setEnabled(active);
		setPlan(build(body));
		setPersistent(sc,null,null);
		RegularCachedPlan rcp = getPlan(sc);
		planCacheKey = rcp.getKey().getShrunk();
	}
	
	private PERawPlan(RawPlan rp, SchemaContext pc) {
		super(getRawPlanKey(rp.getName()));
		pc.startLoading(this, rp);
		setName(new UnqualifiedName(rp.getName()));
		setComment(rp.getComment());
		setDatabase(pc,PEDatabase.load(rp.getDatabase(), pc),true);
		setEnabled(rp.isEnabled());
		setPlan(build(rp.getDefinition()));
		planCacheKey = rp.getCacheKey();
		setPersistent(pc,rp,rp.getId());
		pc.finishedLoading(this, rp);
	}
	
	public String getComment() {
		return comment;
	}
	
	public void setComment(String c) {
		comment = c;
	}
	
	public void setPlan(String body) {
		setPlan(build(body));
	}
	
	public void setPlan(Rawplan rp) {
		thePlan = rp;
	}

	public Rawplan getRawPlan() {
		return thePlan;
	}
	
	public void setEnabled(boolean v) {
		enabled = v;
	}
	
	public boolean isEnabled() {
		return enabled;
	}
	
	public String getPlanCacheKey() {
		return planCacheKey;
	}
	
	public PEDatabase getDatabase(SchemaContext sc) {
		return db.get(sc);
	}
	
	public static Rawplan build(String in) {
		try {
			return PEXmlUtils.unmarshalJAXB(in, Rawplan.class);
		} catch (PEException pe) {
			throw new SchemaException(Pass.SECOND, "Unable to unmarshall raw plan",pe);
		}
	}
	
	public static String build(Rawplan t) {
		try {
			return PEXmlUtils.marshalJAXB(t);
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to marshall raw plan",pe);
		}
	}
	
	public static PERawPlan load(RawPlan rp, SchemaContext pc) {
		PERawPlan perp = (PERawPlan)pc.getLoaded(rp,getRawPlanKey(rp.getName()));
		if (perp == null)
			perp = new PERawPlan(rp,pc);
		return perp;
	}
	
	@SuppressWarnings("unchecked")
	public void setDatabase(SchemaContext sc, PEDatabase pdb, boolean persistent) {
		db = StructuralUtils.buildEdge(sc, pdb, persistent);
	}
	
	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return RawPlan.class;
	}

	@Override
	protected int getID(RawPlan p) {
		return p.getId();
	}

	@Override
	protected RawPlan lookup(SchemaContext sc) throws PEException {
		return null;
	}

	@Override
	protected RawPlan createEmptyNew(SchemaContext sc) throws PEException {
		RegularCachedPlan rcp = getPlan(sc);
		RawPlan rp = new RawPlan(getName().getUnquotedName().get(),
				build(thePlan),
				db.get(sc).getPersistent(sc),
				enabled,
				rcp.getKey().getShrunk(),				
				comment);
		return rp;
	}

	@Override
	protected void populateNew(SchemaContext sc, RawPlan p) throws PEException {
	}

	@Override
	protected void updateExisting(SchemaContext sc, RawPlan p) throws PEException {
		p.setComment(comment);
		p.setDefinition(build(thePlan));
		p.setEnabled(enabled);
	}
	
	@Override
	protected Persistable<PERawPlan, RawPlan> load(SchemaContext sc, RawPlan p)
			throws PEException {
		return null;
	}

	@Override
	protected String getDiffTag() {
		return null;
	}

	@Override
	public void checkValid(SchemaContext sc, List<ValidateResult> acc) {
		if (!enabled) return;
		List<PERawPlan> enabled = sc.findEnabledRawPlans();
		for(Iterator<PERawPlan> iter = enabled.iterator(); iter.hasNext();) {
			PERawPlan perp = iter.next();
			if (perp.getName().equals(getName())) {
				iter.remove();
				continue;
			}
			if (!perp.getDatabase(sc).getCacheKey().equals(db.get(sc).getCacheKey())) 
				continue;
			if (perp.getPlanCacheKey().equals(planCacheKey)) 
				acc.add(new SimpleValidateResult(this,true,"Duplicate raw plan cache key '" + planCacheKey + "'"));
		}
		int limit = SchemaSourceFactory.getCacheLimits(sc).getLimit(CacheSegment.RAWPLAN);
		if ((enabled.size() + 1) >= limit)
			acc.add(new SimpleValidateResult(this,false,"Too many enabled raw plans.  Found " + (enabled.size() + 1) + " but can only cache " + limit));
	}

	
	public static SchemaCacheKey<PERawPlan> getRawPlanKey(UnqualifiedName n) {
		return getRawPlanKey(n.getUnquotedName().get());
	}
	
	public static SchemaCacheKey<PERawPlan> getRawPlanKey(String n) {
		return new RawPlanCacheKey(n);
	}
	
	public static class RawPlanCacheKey extends SchemaCacheKey<PERawPlan> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		String name;
		
		public RawPlanCacheKey(String n) {
			name = n;
		}

		@Override
		public CacheSegment getCacheSegment() {
			return CacheSegment.RAWPLAN;
		}
			
		@Override
		public int hashCode() {
			return initHash(PERawPlan.class,name.hashCode());
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof RawPlanCacheKey) {
				RawPlanCacheKey oct = (RawPlanCacheKey) o;
				return name.equals(oct.name);
			}
			return false;
		}

		@Override
		public PERawPlan load(SchemaContext sc) {
			RawPlan rp = sc.getCatalog().findRawPlan(name);
			if (rp == null)
				return null;
			return PERawPlan.load(rp, sc);
		}

		@Override
		public String toString() {
			return "PERawPlan:" + name;
		}
		
	}

	public RegularCachedPlan getPlan(SchemaContext sc) {
		return RawToExecConverter.convert(sc,thePlan, db.get(sc));
	}
	
}
