package com.tesora.dve.sql.schema.cache;

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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.sql.expression.MTTableKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.parser.ExtractedLiteral;
import com.tesora.dve.sql.parser.ExtractedLiteral.Type;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.mt.IPETenant;
import com.tesora.dve.sql.schema.mt.PETenant;
import com.tesora.dve.sql.schema.mt.TableScope;
import com.tesora.dve.sql.schema.mt.TableScope.ScopeCacheKey;
import com.tesora.dve.sql.statement.CacheableStatement;
import com.tesora.dve.sql.transform.execution.ConnectionValuesMap;
import com.tesora.dve.sql.transform.execution.RootExecutionPlan;
import com.tesora.dve.sql.transform.execution.RebuiltPlan;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;

public class MTCachedPlan implements RegularCachedPlan {

	private final PlanCacheKey pck;
	private final List<ExtractedLiteral.Type> extractedTypes;
	private final List<CachedTSPlan> plans;
	private final Cache<SchemaCacheKey<PETenant>,MatchResult> tenants;
	private final LockType lockType;
	
	public MTCachedPlan(PlanCacheKey pck, List<ExtractedLiteral> extractedLiterals, int cacheDepth, LockType lt) {
		this.pck = pck;
		extractedTypes = new ArrayList<ExtractedLiteral.Type>();
		for(ExtractedLiteral el : extractedLiterals)
			extractedTypes.add(el.getType());
		plans = new ArrayList<CachedTSPlan>();
		lockType = lt;
		tenants = CacheBuilder.newBuilder().maximumSize(cacheDepth)
			.removalListener(new OnRemoval(this))
			.build();
	}

	@Override
	public boolean invalidate(SchemaCacheKey<?> unloaded) {
		if (unloaded.getCacheSegment() == CacheSegment.TENANT) {
			tenants.invalidate(unloaded);
			return tenants.size() == 0;
		} else if (unloaded.getCacheSegment() == CacheSegment.TABLE) {
			HashSet<CachedTSPlan> lost = new HashSet<CachedTSPlan>();
			for(CachedTSPlan ctsp : plans) {
				if (ctsp.invalidate(unloaded))
					lost.add(ctsp);
			}
			if (lost.isEmpty()) return false;
			HashSet<SchemaCacheKey<PETenant>> keys = new HashSet<SchemaCacheKey<PETenant>>(tenants.asMap().keySet());
			for(Iterator<SchemaCacheKey<PETenant>> iter = keys.iterator(); iter.hasNext();) {
				MatchResult thePlan = tenants.getIfPresent(iter.next());
				if (thePlan == null || !lost.contains(thePlan.getPlan()))
					iter.remove();
			}
			tenants.invalidateAll(keys);
			synchronized(plans) {
				plans.removeAll(lost);
			}
			return tenants.size() == 0 || plans.size() == 0;
		} else if (unloaded.getCacheSegment() == CacheSegment.SCOPE) {
			ScopeCacheKey sck = (ScopeCacheKey) unloaded;
			SchemaCacheKey<PETenant> tenantKey = sck.getTenantCacheKey(); 
			MatchResult results = tenants.getIfPresent(tenantKey);
			if (results != null) {
				if (results.matches(sck)) {
					tenants.invalidate(tenantKey);
				}
			}
		}
		return false;
	}

	
	@SuppressWarnings("unchecked")
	public void take(SchemaContext sc, CacheableStatement originalStatement, RootExecutionPlan thePlan) {
		// first build the CachedTSPlan and then try to merge it in.
		// we seek to minimize the time within the lock.
		List<VisibilityRecord> visible = new ArrayList<VisibilityRecord>();
		ArrayList<SchemaCacheKey<TableScope>> scopes = new ArrayList<SchemaCacheKey<TableScope>>();
		for(TableKey tk : originalStatement.getAllTableKeys()) {
			if (tk instanceof MTTableKey) {
				MTTableKey mtk = (MTTableKey) tk;
				VisibilityRecord vr = new VisibilityRecord(sc,mtk.getScope().getName(),mtk.getScope().getTable(sc));
				visible.add(vr);
				scopes.add((SchemaCacheKey<TableScope>) mtk.getScope().getCacheKey());
			}
		}
		scopes.trimToSize();
		SchemaEdge<IPETenant> ick = sc.getCurrentTenant();
		PETenant ten = (PETenant)ick.get(sc);
		SchemaCacheKey<PETenant> ck = (SchemaCacheKey<PETenant>) ten.getCacheKey();
		CachedTSPlan np = new CachedTSPlan(this,thePlan,visible);
		int max = plans.size();
		MatchResult mr = null;
		for(int i = 0; i < max; i++) {
			CachedTSPlan existing = plans.get(i);
			if (existing.matches(sc,np)) {
				existing.increment();
				mr = new MatchResult(existing,scopes);
				tenants.put(ck, mr);
				return;
			}
		}
		synchronized(plans) {
			plans.add(np);
		}
		if (mr == null)
			mr = new MatchResult(np,scopes);
		tenants.put(ck,mr);
	}
	
	protected void onRemoval(MatchResult ctsp) {
		if (ctsp.getPlan().decrement()) {
			synchronized(plans) {
				plans.remove(ctsp);
			}
		}
	}
	
	@Override
	public PlanCacheKey getKey() {
		return pck;
	}

	@Override
	public List<ExtractedLiteral.Type> getLiteralTypes() {
		return null;
	}

	@Override
	public RebuiltPlan showPlan(SchemaContext sc,
			List<ExtractedLiteral> literals) throws PEException {
		return null;
	}
	

	@Override
	public RebuiltPlan rebuildPlan(SchemaContext sc,
			List<ExtractedLiteral> literals) throws PEException {
		if (!isValidLiterals(literals)) return new RebuiltPlan(null, null, true,null,lockType);

		IPETenant currentTenant = sc.getCurrentTenant().get(sc);
		if (currentTenant == null)
			return new RebuiltPlan(null, null, true,null,lockType);
		
		MatchResult candidate = findMatching(sc, (PETenant)currentTenant);
		if (candidate == null)
			return new RebuiltPlan(null, null, true,null,lockType);
		
		return candidate.getPlan().rebuildPlan(sc, literals, 
				Functional.apply(candidate.getScopes(), new UnaryFunction<SchemaCacheKey<?>,SchemaCacheKey<TableScope>>() {

					@Override
					public SchemaCacheKey<?> evaluate(
							SchemaCacheKey<TableScope> object) {
						return object;
					}
					
				}));
	}

	@Override
	public boolean isValid(SchemaContext sc, List<ExtractedLiteral> literals)
			throws PEException {
		if (!isValidLiterals(literals))
			return false;
		return findMatching(sc, (PETenant)sc.getCurrentTenant().get(sc)) != null;
	}
	
	@SuppressWarnings("unchecked")
	private MatchResult findMatching(SchemaContext sc, PETenant tenant) throws PEException {
		SchemaCacheKey<PETenant> ck = (SchemaCacheKey<PETenant>) tenant.getCacheKey();
		MatchResult candidate = tenants.getIfPresent(ck);
		if (candidate != null) return candidate;
		int max = plans.size();
		for(int i = 0; i < max; i++) {
			CachedTSPlan ctsp = plans.get(i);
			List<SchemaCacheKey<TableScope>> matching = ctsp.slowMatch(sc, tenant, lockType);
			if (matching != null) {
				ctsp.increment();
				MatchResult mr = new MatchResult(ctsp,matching);
				tenants.put(ck, mr);
				return mr;
			}
		}
		
		return null;		
	}

	private boolean isValidLiterals(List<ExtractedLiteral> literals) {
		if (extractedTypes.size() != literals.size())
			return false;

		for(int i = 0; i < extractedTypes.size(); i++) {
			if (extractedTypes.get(i) != literals.get(i).getType())
				return false;
		}
		return true;
	}
	
	// in mt mode, a cached plan consists of cached table specific (TS) plans
	// each TS plan contains a list of mappings from visible names to backing PETables
	// as well as a set of SchemaCacheKeys that point to tenants
	// each TS is for a different set of backing PETables
	
	public static class CachedTSPlan implements RegularCachedPlan {
		
		private final RootExecutionPlan thePlan;
		private final List<VisibilityRecord> mapping;		
		private final MTCachedPlan parent;
		private final AtomicInteger refCount;
		
		public CachedTSPlan(MTCachedPlan mtcp, RootExecutionPlan thePlan, List<VisibilityRecord> theMapping) {
			parent = mtcp;
			this.thePlan = thePlan;
			this.thePlan.setOwningCache(this);
			this.mapping = theMapping;
			this.refCount = new AtomicInteger(1);
		}

		public void increment() {
			refCount.incrementAndGet();
		}
		
		public boolean decrement() {
			int was = refCount.decrementAndGet();
			return (was <= 0);
		}
		
		@Override
		public PlanCacheKey getKey() {
			return parent.getKey();
		}

		@Override
		public RebuiltPlan rebuildPlan(SchemaContext sc,
				List<ExtractedLiteral> literals) throws PEException {
			ConnectionValuesMap cv = thePlan.resetForNewPlan(sc, literals);
			return new RebuiltPlan(thePlan, cv, false,null,parent.lockType);
		}

		public RebuiltPlan rebuildPlan(SchemaContext sc, 
				List<ExtractedLiteral> literals, List<SchemaCacheKey<?>> scopes) throws PEException {
			ConnectionValuesMap cv = thePlan.resetForNewPlan(sc, literals);
			return new RebuiltPlan(thePlan, cv, false,scopes.toArray(new SchemaCacheKey<?>[0]),parent.lockType);
		}
		
		@Override
		public boolean isValid(SchemaContext sc, List<ExtractedLiteral> literals)
				throws PEException {
			return parent.isValid(sc, literals);
		}
		
		public List<SchemaCacheKey<TableScope>> slowMatch(SchemaContext sc, PETenant pet, LockType lt) {
			ArrayList<SchemaCacheKey<TableScope>> matching = new ArrayList<SchemaCacheKey<TableScope>>();
			for(VisibilityRecord vr : mapping) {
				SchemaCacheKey<TableScope> m = vr.matches(sc, pet, lt);
				if (m == null)
					return null;
				matching.add(m);
			}
			matching.trimToSize();
			return matching;
		}
		
		public boolean matches(SchemaContext sc, CachedTSPlan other) {
			Set<String> mine = visibilityShortHand(sc);
			Set<String> yours = other.visibilityShortHand(sc);
			return mine.equals(yours);
		}
		
		protected Set<String> visibilityShortHand(SchemaContext sc) {
			HashSet<String> out = new HashSet<String>();
			for(VisibilityRecord vr : mapping)
				out.add(vr.buildKey(sc));
			return out;
		}

		@Override
		public boolean invalidate(SchemaCacheKey<?> unloaded) {
			if (unloaded.getCacheSegment() != CacheSegment.TABLE) return false;
			for(VisibilityRecord vr : mapping) {
				if (vr.getCacheKey().equals(unloaded))
					return true;
			}
			return false;
		}

		@Override
		public RebuiltPlan showPlan(SchemaContext sc,
				List<ExtractedLiteral> literals) throws PEException {
			return null;
		}

		@Override
		public List<Type> getLiteralTypes() {
			return null;
		}		
	}
	
	private static class VisibilityRecord {
		
		private final Name localName;
		private final SchemaCacheKey<PETable> backingTable;
		
		@SuppressWarnings("unchecked")
		public VisibilityRecord(SchemaContext sc, Name visibleName, PETable backing) {
			this.localName = visibleName;
			this.backingTable = (SchemaCacheKey<PETable>) backing.getCacheKey();
		}
		
		@SuppressWarnings("unchecked")
		public SchemaCacheKey<TableScope> matches(SchemaContext sc, PETenant pet, LockType lt) {
			TableScope ts = pet.lookupScope(sc, localName, new LockInfo(lt,"lookup scope for mt cached plan"));
			if (ts == null) return null;
			if (!backingTable.equals(ts.getTableKey())) return null;
			return (SchemaCacheKey<TableScope>) ts.getCacheKey();
		}
		
		public String buildKey(SchemaContext sc) {
			return localName.get() + "/" + backingTable;
		}
		
		public SchemaCacheKey<PETable> getCacheKey() {
			return backingTable;
		}
	}

	private static class OnRemoval implements RemovalListener<SchemaCacheKey<PETenant>,MatchResult> {

		private final MTCachedPlan parent;
		
		public OnRemoval(MTCachedPlan ctcp) {
			parent = ctcp;
		}
		
		@Override
		public void onRemoval(
				RemovalNotification<SchemaCacheKey<PETenant>, MatchResult> arg0) {
			parent.onRemoval(arg0.getValue());
		}
		
	}

	private static class MatchResult {
		
		private List<SchemaCacheKey<TableScope>> scopes;
		private CachedTSPlan plan;
		
		public MatchResult(CachedTSPlan thePlan, List<SchemaCacheKey<TableScope>> matching) {
			scopes = matching;
			plan = thePlan;
		}
		
		public CachedTSPlan getPlan() {
			return plan;
		}
		
		public boolean matches(SchemaCacheKey<TableScope> in) {
			return scopes.contains(in);
		}
		
		public List<SchemaCacheKey<TableScope>> getScopes() {
			return scopes;
		}
	}

}
