// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.StorageGroup.GroupScale;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.PEDynamicGroup;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;

public class TempGroupManager {

	protected TempGroupPlaceholder aggGroup;
	protected List<TempGroupPlaceholder> dynGroup;
	
	// a storage group can be pegged to be the persistent group
	protected PEStorageGroup pegged;
	
	public TempGroupManager() {
		aggGroup = new TempGroupPlaceholder(true);
		dynGroup = new ArrayList<TempGroupManager.TempGroupPlaceholder>();
	}
	
	public void pegGroup(PEPersistentGroup p) {
		if (pegged != null && !(pegged instanceof PEPersistentGroup))
			throw new SchemaException(Pass.PLANNER, "Attempt to peg temp group to transient group");
		pegged = p;
	}
	
	public TempGroupPlaceholder getGroup(boolean agg) {
		return aggGroup;
	}
	
	public TempGroupPlaceholder getGroup(int cost) throws PEException {
		TempGroupPlaceholder tgph = new TempGroupPlaceholder(cost);
		
		dynGroup.add(tgph);
		
		return tgph;
	}
	
	public TempGroupPlaceholder getLatestGroup(int cost) throws PEException {
		if (dynGroup.size() > 0) {
			TempGroupPlaceholder tgph = dynGroup.get(dynGroup.size()-1);
			if (tgph.getCost() < cost) {
				tgph.setCost(cost);
			}
			return tgph;
		}
		return getGroup(cost);
	}
	
	public void plan(SchemaContext context) throws PEException {
		if (pegged != null) {
			for(TempGroupPlaceholder tgph : dynGroup) {
				tgph.setBacking(context, pegged);
			}
			aggGroup.setBacking(context, pegged.anySite(context));		
		} else {
			for(TempGroupPlaceholder tgph : dynGroup) {
				tgph.createBacking(context);
			}
			aggGroup.createBacking(context);
		}
	}
	
	public void resetForCache(SchemaContext sc) {
	}
	
	public static class TempGroupPlaceholder implements PEStorageGroup {

		// true if this is an aggregation group - in that case we can't really swap the actual group for something larger
		protected boolean aggregation;
		// track the cost
		protected int cost;
		// if fulfilled by a dynamic group, index into the conn values
		protected int index = -1;
		
		public TempGroupPlaceholder(boolean agg) {
			aggregation = agg;
		}
		
		public String getLoadedBy() {
			return "TempGroupPlaceholder";
		}
		
		public TempGroupPlaceholder(int initialCost) {
			aggregation = false;
			cost = initialCost;
		}
		
		// raw plan support
		public TempGroupPlaceholder(SchemaContext sc, GroupScale gs) {
			index = sc.getValueManager().allocatePlaceholderGroup(sc, new PEDynamicGroup(gs));
		}
		
		
		protected void setBacking(SchemaContext sc, PEStorageGroup a) {
			index = sc.getValueManager().allocatePlaceholderGroup(sc, a);
		}
		
		protected void createBacking(SchemaContext sc) throws PEException {
			GroupScale scale = null;
			if (aggregation)
				scale = GroupScale.AGGREGATE;
			else {
				if (cost >= 0)
					scale = GroupScale.SMALL;
				if (cost >= 10)
					scale = GroupScale.MEDIUM;
				if (cost >= 100)
					scale = GroupScale.LARGE;
			}
			index  = sc.getValueManager().allocatePlaceholderGroup(sc, new PEDynamicGroup(scale));
		}
		
		@Override
		public PEStorageGroup getPEStorageGroup(SchemaContext sc) {
			return sc.getValueManager().getPlaceholderGroup(sc, index);
		}
		
		@Override
		public StorageGroup getPersistent(SchemaContext sc) {
			if (index == -1)
				throw new SchemaException(Pass.PLANNER, "Attempt to obtain temp group before planning complete");
			return sc.getValueManager().getPlaceholderGroup(sc, index).getPersistent(sc);
		}

		@Override
		public PersistentGroup persistTree(SchemaContext sc) throws PEException {
			return null;
		}
		@Override
		public StorageGroup getScheduledGroup(SchemaContext sc) {
			return getPersistent(sc);
		}
		
		@Override
		public PEPersistentGroup anySite(SchemaContext sc) throws PEException {
			return null;
		}

		@Override
		public boolean comparable(SchemaContext sc, PEStorageGroup storage) {
			if (storage == this) return true;
			return false;
		}
		
		@Override
		public boolean isSubsetOf(SchemaContext sc, PEStorageGroup storage) {
			return storage == this;
		}

		@Override
		public boolean isSingleSiteGroup() {
			return aggregation;
		}

		@Override
		public void setCost(int score) throws PEException {
			if (score > cost)
				cost = score;
		}

		public int getCost() {
			return cost;
		}
		
		@Override
		public boolean isTempGroup() {
			return true;
		}
		
		public String toString() {
			return "TempGroupPlaceholder/" + cost + (isSingleSiteGroup() ? "/agg" : "/dyn" + "/" + index);
		}

	}

}
