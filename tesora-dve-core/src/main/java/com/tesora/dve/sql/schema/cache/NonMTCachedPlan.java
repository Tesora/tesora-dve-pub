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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.CollectionUtils;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.parser.ExtractedLiteral;
import com.tesora.dve.sql.parser.ExtractedLiteral.Type;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.CacheableStatement;
import com.tesora.dve.sql.transform.execution.ConnectionValuesMap;
import com.tesora.dve.sql.transform.execution.RootExecutionPlan;
import com.tesora.dve.sql.transform.execution.RebuiltPlan;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;

public class NonMTCachedPlan implements RegularCachedPlan {
	public static final String GLOBAL_CACHE_NAME = "X8U_GLOBAL_5NT";
	
	@SuppressWarnings("rawtypes")
	private final SchemaCacheKey[] tks;
	private final PlanCacheKey key;
	private final List<ExtractedLiteral.Type> extractedTypes;
	private final LockType lockType;
	private Map<String, SpecificCachedPlan> specificCachedPlanMap = new ConcurrentHashMap<String, NonMTCachedPlan.SpecificCachedPlan>();

	public NonMTCachedPlan(ListSet<TableKey> tabs, PlanCacheKey pck, List<ExtractedLiteral> extractedLiterals, LockType lt) {
		this(tabs,Functional.apply(extractedLiterals,ExtractedLiteral.typeAccessor),pck, lt);
	}
	
	public NonMTCachedPlan(ListSet<TableKey> tabs, List<ExtractedLiteral.Type> extractedLiterals, PlanCacheKey pck, LockType lt) {
		tks = buildTableKeySet(tabs);
		key = pck;
		extractedTypes = extractedLiterals;
		lockType = lt;
	}

	@Override
	public PlanCacheKey getKey() {
		return key;
	}
	
	@Override
	public RebuiltPlan rebuildPlan(SchemaContext sc, List<ExtractedLiteral> literals) throws PEException {
		if (!isValid(sc, literals))
			return new RebuiltPlan(null, null, true, tks, lockType);
		
		SpecificCachedPlan nmtcp = specificCachedPlanMap.get(sc.getConnection().getCacheName() == null ? GLOBAL_CACHE_NAME : sc.getConnection().getCacheName());
		if (nmtcp != null) {
			return nmtcp.rebuildPlan(sc, literals);
		} else {
			// could not find the plan for the specified cache but don't clear this plan from the cache
			return new RebuiltPlan(null, null, false, tks, lockType);
		}
	}
	
	@Override
	public RebuiltPlan showPlan(SchemaContext sc, List<ExtractedLiteral> literals) throws PEException {
		return specificCachedPlanMap.get(GLOBAL_CACHE_NAME).showPlan(sc,literals);
	}
	
	@Override
	public List<ExtractedLiteral.Type> getLiteralTypes() {
		return extractedTypes;
	}
	
	@Override
	public boolean isValid(SchemaContext sc, List<ExtractedLiteral> literals) throws PEException {
		for(int i = 0; i < extractedTypes.size(); i++) {
			if (extractedTypes.get(i) != literals.get(i).getType())
				return false;
		}
		return true;
	}

	@Override
	public boolean invalidate(SchemaCacheKey<?> unloaded) {
		if (unloaded.getCacheSegment() != CacheSegment.TABLE) return false;
		for(int i = 0; i < tks.length; i++) {
			if (tks[i].equals(unloaded)) {
				specificCachedPlanMap.clear();
				return true;
			}
		}

		return false;
	}

	public boolean take(SchemaContext sc, CacheableStatement originalStatement, RootExecutionPlan thePlan) {
		boolean ret = true;

		if (!keysMatch(originalStatement.getAllTableKeys()) && !thePlan.isEmptyPlan()) {
			// remove this entry from the cache
			ret = false;
		} else {
			SpecificCachedPlan np = new SpecificCachedPlan(this, thePlan);
			specificCachedPlanMap.put(sc.getConnection().getCacheName() == null ? GLOBAL_CACHE_NAME : sc.getConnection().getCacheName(), np);
		}
		return ret;
	}

	@SuppressWarnings("rawtypes")
	private SchemaCacheKey[] buildTableKeySet(ListSet<TableKey> tabs) {
		ArrayList<SchemaCacheKey<?>> buf = new ArrayList<SchemaCacheKey<?>>();
		
		for(TableKey tk : tabs) {
			SchemaCacheKey<?> sck = tk.getCacheKey();
			buf.add(sck);
		}
		return buf.toArray(new SchemaCacheKey[0]);
	}
	
	private boolean keysMatch(ListSet<TableKey> allTableKeys) {
		return CollectionUtils.isEqualCollection(
				Arrays.asList(tks), Arrays.asList(buildTableKeySet(allTableKeys)));
	}

	public static class SpecificCachedPlan implements RegularCachedPlan {

		private final NonMTCachedPlan parent;
		private final RootExecutionPlan thePlan;
		
		public SpecificCachedPlan(NonMTCachedPlan nmtcp, RootExecutionPlan ep) {
			parent = nmtcp;
			thePlan = ep;
			thePlan.setOwningCache(parent);
		}

		@Override
		public PlanCacheKey getKey() {
			return parent.getKey();
		}

		@Override
		public RebuiltPlan showPlan(SchemaContext sc, List<ExtractedLiteral> literals) throws PEException {
			ConnectionValuesMap cvs = thePlan.resetForNewPlan(sc, literals);
			return new RebuiltPlan(thePlan, cvs, false,null, null);
		}
		
		@Override
		public RebuiltPlan rebuildPlan(SchemaContext sc,
				List<ExtractedLiteral> literals) throws PEException {
			if (!isValid(sc, literals))
				return new RebuiltPlan(null, null, true, parent.tks, parent.lockType);

			if (thePlan.isEmptyPlan()) {
				return new RebuiltPlan(thePlan, null, false, parent.tks, parent.lockType);
			}
			
			ConnectionValuesMap cv = thePlan.resetForNewPlan(sc, literals);
			return new RebuiltPlan(thePlan, cv, false, parent.tks, parent.lockType);
		}
		
		@Override
		public boolean isValid(SchemaContext sc, List<ExtractedLiteral> literals)
				throws PEException {
			if (thePlan.getValueManager().getNumberOfLiterals() != literals.size())
				return false;
			return true;
		}
		
		@Override
		public boolean invalidate(SchemaCacheKey<?> unloaded) {
			return false;
		}

		@Override
		public List<Type> getLiteralTypes() {
			return null;
		}
	}
}
