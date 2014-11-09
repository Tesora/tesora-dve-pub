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

import java.util.List;

import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.lockmanager.LockSpecification;
import com.tesora.dve.sql.PlannerStatisticType;
import com.tesora.dve.sql.PlannerStatistics;
import com.tesora.dve.sql.expression.MTTableKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.parser.CandidateParser;
import com.tesora.dve.sql.parser.ExtractedLiteral;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaPolicyContext;
import com.tesora.dve.sql.statement.CacheableStatement;
import com.tesora.dve.sql.transform.execution.RootExecutionPlan;
import com.tesora.dve.sql.transform.execution.RebuiltPlan;
import com.tesora.dve.sql.util.Pair;

public abstract class PlanCacheUtils {
		
	private static final Logger logger = Logger.getLogger(PlanCacheUtils.class);
	private static final boolean emit = Boolean.getBoolean("plancache.parameterize");
	
	private static boolean isDebugEnabled() {
		return emit || logger.isDebugEnabled();
	}
	
	private static void log(String m) {
		if (emit)
			System.err.println(m);
		if (logger.isDebugEnabled())
			logger.debug(m);
	}
	
	public static CachedPlan maybeCachePlan(SchemaContext sc, SchemaSourcePlanCache toCache, CacheableStatement origStatement, RootExecutionPlan thePlan,
			String theSQL,
			CandidateParser precomputed) {
		if (sc.getSource().getType() == CacheType.MUTABLE) return null;
		if (!thePlan.isCacheable() || !thePlan.getValueManager().isCacheable()) {
            switch (thePlan.getValueManager().getCacheStatus()){
                case NOCACHE_TOO_MANY_LITERALS:
                    PlannerStatistics.increment(PlannerStatisticType.UNCACHEABLE_EXCEEDED_LITERAL_LIMIT);
                    break;
                case NOCACHE_DYNAMIC_FUNCTION:
                    PlannerStatistics.increment(PlannerStatisticType.UNCACHEABLE_DYNAMIC_FUNCTION);
                    break;
                default:
			        PlannerStatistics.increment(PlannerStatisticType.UNCACHEABLE_PLAN);
            }
			if (isDebugEnabled())
				log("Unable to cache " + theSQL + " because plan not cacheable");
			return null;
		}
		if (sc.getCurrentDatabase(false) == null) return null;
		String key = null;
		List<ExtractedLiteral> extracted = null;
		CandidateParser cp = null;
		if (precomputed == null) {
			cp = new CandidateParser(theSQL);
			// not cacheable
			if (!cp.shrink()) {
				if (isDebugEnabled())
					log("Unable to cache " + theSQL + " because not shrinkable");
				return null;
			}
			key = cp.getShrunk();
			extracted = cp.getLiterals();
		} else {
			cp = precomputed;
			key = precomputed.getShrunk();
			extracted = precomputed.getLiterals();
		}			
		if (!isValidParameterization(theSQL, cp, thePlan)) {
			PlannerStatistics.increment(PlannerStatisticType.UNCACHEABLE_INVALID_CANDIDATE);
			return null;
		}
		// this is an mt plan if any of the table keys are mt table keys
		boolean mt = false;
		for(TableKey tk : origStatement.getAllTableKeys()) {
			if (tk instanceof MTTableKey) {
				mt= true;
				break;
			}
		}
		PlanCacheKey pck = buildCacheKey(key,sc.getCurrentDatabase(),sc.getPolicyContext());
		RegularCachedPlan results = null;
		if (mt) {
			// for mt plans, see if there is an existing entry cache
			results = toCache.getCachedPlan(sc,pck);
			MTCachedPlan mtcp = null;
			if (results instanceof MTCachedPlan) {
				mtcp = (MTCachedPlan) results;
			} else {
				mtcp = new MTCachedPlan(pck,extracted, SchemaSourceFactory.getCacheLimits(sc).getLimit(CacheSegment.TENANT), origStatement.getLockType()); 
				results = mtcp;
			}
			mtcp.take(sc,origStatement, thePlan);
		} else {
			results = toCache.getCachedPlan(sc,pck);
			NonMTCachedPlan nmtcp = null;
			if (results instanceof NonMTCachedPlan) {
				nmtcp = (NonMTCachedPlan) results;
			} else {
				nmtcp = new NonMTCachedPlan(origStatement.getAllTableKeys(), pck, extracted, origStatement.getLockType());
				results = nmtcp;
			}
			if (!nmtcp.take(sc,origStatement, thePlan)) {
				// table keys changed - clear the cache
				toCache.clearCachedPlan(results);
				results = null;
			}
		}
		if (results != null)
			toCache.putCachedPlan(results);
		return results;
	}
	
	public static boolean isValidParameterization(String theSQL, CandidateParser cp, RootExecutionPlan ep) {
		List<ExtractedLiteral> literals = cp.getLiterals();
		if (literals.size() != ep.getValueManager().getNumberOfLiterals()) {
			if (isDebugEnabled())
				log("Unable to cache " + theSQL + " because shrunk literals=" + literals.size() + " but plan literals=" + ep.getValueManager().getNumberOfLiterals());
			return false;
		}
		for(int i = 0; i < literals.size(); i++) {
			String ex = literals.get(i).getText();
			String p = ep.getValueManager().getOriginalLiteralText(i);
			if (!ex.equals(p)) {
				if (isDebugEnabled())
					log("Unable to cache " + theSQL + " because literal " + i + " extracted as " + ex + " but parsed as " + p);
				return false;
			}
		}
		return true;
	}
	
	public static CandidateCachedPlan getCachedPlan(SchemaContext sc, String theSQL, PlanCacheCallback inpcb) throws PEException {
		if (sc.getCurrentDatabase(false) == null)
			return new CandidateCachedPlan(null,null,null,false);
		CandidateParser cp = new CandidateParser(theSQL);
		// uncacheable if the input sql is unshrinkable
		if (!cp.shrink()) {
			return new CandidateCachedPlan(null,null,null,false);
		}
		String key = cp.getShrunk();

		PlanCacheCallback pcb = inpcb;
		
		if (pcb == null) pcb = DefaultPlanCacheCallback.getInstance(); 

		PlanCacheKey pck = buildCacheKey(key,sc.getCurrentDatabase(),sc.getPolicyContext()); 
		
		RegularCachedPlan plan = sc.getSource().getCachedPlan(sc,pck); 
		if (plan == null) {
			pcb.onMiss(theSQL);
			// we have no entry, but we can shrink, so indicate that we should try to cache
			return new CandidateCachedPlan(cp,null,null,true);
		}
		RebuiltPlan rp = plan.rebuildPlan(sc, cp.getLiterals());
		if (rp.getEp() == null) {
			// bad cache
			if (rp.isClearCache()) {
				sc.getSource().clearCachedPlan(plan);
			}
			// this is a miss
			pcb.onMiss(theSQL);
			// we were unable to interpret the literals - next time something like this happens
			// we'll got through the first branch above (immediate return) and try again
			return new CandidateCachedPlan(cp,null,null,!rp.isClearCache());
		}
		pcb.onHit(theSQL);
		SchemaCacheKey<?>[] keys = rp.getCacheKeys();
		if (keys != null) {
			for(SchemaCacheKey<?> sck : keys) {
				if (sck == null) continue; // info schema
				LockSpecification ls = sck.getLockSpecification("plan cache hit");
				if (ls == null) continue;
				sc.getConnection().acquireLock(ls, rp.getLockType());
			}
		}
		return new CandidateCachedPlan(cp,rp.getEp(),rp.getBoundValues(),false);
	}
		
	public static PlanCacheKey buildCacheKey(String key, Database<?> db, SchemaPolicyContext spc) {
		PlanCacheKey pck = null;
		if (spc.isContainerContext()) 
			pck = new ContainerPlanCacheKey(key,db,!spc.isDataTenant());
		if (pck == null)
			pck = new PlanCacheKey(key,db);
		return pck;
	}
	
    public static interface PlanCacheCallback {
    	
    	public void onHit(String stmt);
    	
    	public void onMiss(String stmt);
    }

    private static class DefaultPlanCacheCallback implements PlanCacheCallback {

    	private static DefaultPlanCacheCallback instance = new DefaultPlanCacheCallback();
    	
    	public static PlanCacheCallback getInstance() {
    		return instance;
    	}
    	
		@Override
		public void onHit(String stmt) {
		}

		@Override
		public void onMiss(String stmt) {
		}
    	
    	
    }
    
    public static void registerPreparedStatementPlan(SchemaContext sc, CachedPreparedStatement cps, String prepareSQL, int connid, String stmtID, boolean reregister) throws PEException {
    	sc.getSource().putPreparedStatement(cps, connid, stmtID, prepareSQL, reregister);
    }
    
    public static void destroyPreparedStatement(SchemaContext sc, String stmtID) {
    	sc.getSource().clearPreparedStatement(sc.getConnection().getConnectionId(),stmtID);
    }

    public static Pair<RootExecutionPlan,ConnectionValues> bindPreparedStatement(SchemaContext sc, String stmtID, List<Object> params) throws PEException {
    	CachedPreparedStatement cps = sc.getSource().getPreparedStatement(sc, sc.getConnection().getConnectionId(), stmtID);    	
    	return cps.rebuildPlan(sc, params);
    }
    
}
