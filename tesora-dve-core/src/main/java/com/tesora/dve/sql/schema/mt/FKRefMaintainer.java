// OS_STATUS: public
package com.tesora.dve.sql.schema.mt;

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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.MultiMap.HashedCollectionFactory;
import com.tesora.dve.common.catalog.TableState;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.PEAbstractTable.TableCacheKey;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.ChangeSource;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.CreateTableOperation;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.DirectAllocatedTable;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.LateFKFixup;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.LazyAllocatedTable;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.TableFlip;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.CompositeNestedOperation;
import com.tesora.dve.sql.schema.mt.AdaptiveMTDDLPlannerUtils.UpdateForeignKeyTargetTable;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;

public abstract class FKRefMaintainer {

	protected final PETenant tenant;
	protected DependencyTree dt;
	protected ListOfPairs<TableScope, TaggedFK> rootSet;
	protected HashMap<PETable,ModificationBlock> modifications;
	protected ListSet<PETable> applicationOrder;
	protected HashMap<PETable, CreateTableOperation> forwarding;
	
	public FKRefMaintainer(PETenant onTenant) {
		this.tenant = onTenant;
		this.dt = null;
		this.modifications = new HashMap<PETable,ModificationBlock> ();
		this.applicationOrder = new ListSet<PETable>();
		this.forwarding = new HashMap<PETable,CreateTableOperation>();;
	}

	// the first value is the target; the second is the fk that will be effected by that change.
	public abstract ListOfPairs<TableScope,TaggedFK> computeRootSet(SchemaContext sc);

	public abstract void modifyRoot(SchemaContext sc, Pair<TableScope, TaggedFK> r, Map<PETable,ModificationBlock> blocks);
	
	public DependencyTree getDepTree(SchemaContext sc) {
		if (dt == null) {
			dt = new DependencyTree(tenant);
			rootSet = computeRootSet(sc);
			for(Pair<TableScope, TaggedFK> p : rootSet) {
				dt.addDependency(sc, p.getSecond().getFK(), p.getSecond().getEnclosing(), p.getSecond().getScopeOfEnclosing(), p.getFirst().getTable(sc),p.getFirst());
			}
		}
		return dt;
	}
	
	protected void createBlocks(SchemaContext sc) {
		for(PETable pet : dt.getAllTables()) {
			modifications.put(pet, new ModificationBlock(sc,dt.getScopeMapping().get(pet),pet));
		}
		for(ModificationBlock mb : modifications.values()) {
			mb.buildInitialState(sc, modifications);
		}
	}

	public void maintain(SchemaContext sc) {
		// build the entire dep tree starting from the root set
		getDepTree(sc);
		createBlocks(sc);
		LinkedList<PETable> queue = new LinkedList<PETable>();
		for(Pair<TableScope, TaggedFK> p : rootSet) {
			modifyRoot(sc,p,modifications);
			if (applicationOrder.add(p.getFirst().getTable(sc)))
				queue.add(p.getFirst().getTable(sc));
			applicationOrder.add(p.getSecond().getEnclosing());
			queue.add(p.getSecond().getEnclosing());
		}
		while(!queue.isEmpty()) {
			PETable pet = queue.removeFirst();
			applicationOrder.add(pet);
			ModificationBlock mb = modifications.get(pet);

			if (mb.propagateChanges(sc, modifications)) {
				// queue up all of my referrers
				Collection<TaggedFK> refs = dt.getReferring(mb.getSubject());
				if (refs == null || refs.isEmpty()) continue;
				for(TaggedFK tfk : refs)
					queue.add(tfk.getEnclosing());				
			}			
		}		
	}

	public void schedule(SchemaContext sc, ExecutionSequence es, CompositeNestedOperation uno) {
		List<CreateTableOperation> nts = new ArrayList<CreateTableOperation>();
		List<ChangeSource> cs = new ArrayList<ChangeSource>();
		schedule(sc,nts,cs);
		for(CreateTableOperation cto : nts) {
			AdaptiveMTDDLPlannerUtils.addDDLCallback(sc,
					cto.getDefinition().getPEDatabase(sc),
					cto.getDefinition().getPersistentStorage(sc),
					cto.getDefinition(), Action.CREATE, es,cto,
					null);
		}
		uno.withChanges(cs);
	}
	
	public void schedule(SchemaContext sc, List<CreateTableOperation> newTabs, List<ChangeSource> changes) {
		// find all the tables that are different
		LinkedList<ModificationBlock> different = new LinkedList<ModificationBlock>();
		for(ModificationBlock mb : modifications.values()) {
			if (mb.isFlipped())
				different.add(mb);
		}
		// once the change order is set, we can flatten the blocks down to either a fk defs or a create table + a flip
		ListSet<ModificationBlock> changeOrder = new ListSet<ModificationBlock>();
		while(!different.isEmpty()) {
			int before = different.size();
			for(Iterator<ModificationBlock> iter = different.iterator(); iter.hasNext();) {
				ModificationBlock mb = iter.next();
				// compute how the fk states have changed
				Map<PEForeignKey,FKState> mods = mb.getModified();
				// this table can be scheduled if for all modifiable fk states the target tables have already been scheduled
				boolean ok = true;
				for(FKState fks : mods.values()) {
					if (fks instanceof ModifiableFKState) {
						ModifiableFKState mkfs = (ModifiableFKState) fks;
						if (mkfs.mb.isFlipped() && !changeOrder.contains(mkfs.mb)) {
							ok = false;
							break;
						}
					}
				}
				if (ok) {
					changeOrder.add(mb);
					iter.remove();
				}				
			}
			int after = different.size();
			if (after == before)
				break;
		}
		if (!different.isEmpty()) {
			throw new SchemaException(Pass.PLANNER, "Cannot build unique decl order");
		}
		// we can add everything else now - doesn't matter the order
		changeOrder.addAll(modifications.values());

		for(ModificationBlock mb : changeOrder) {
			// if the table changed - use the old def + converting the fkmods to late resolving fks - build a cto
			// then schedule a flip.
			// if the table is not different - just schedule fkupdates
			List<LateFKFixup> fixups = new ArrayList<LateFKFixup>();
			Map<PEForeignKey,FKState> mods = mb.getModified();
			if (mods.isEmpty()) continue;
			for(Map.Entry<PEForeignKey, FKState> m : mods.entrySet()) {
				if (m.getValue() instanceof ForwardFKState) {
					// reverted to forward
					ForwardFKState f = (ForwardFKState) m.getValue();
					fixups.add(new LateFKFixup(m.getKey().getSymbol(),f.targTab,"maintenance"));
				} else if (m.getValue() instanceof ModifiableFKState) {
					ModifiableFKState fkms = (ModifiableFKState) m.getValue();
					LazyAllocatedTable lat = null;
					CreateTableOperation cto = forwarding.get(fkms.mb.getSubject());
					if (cto != null) {
						lat = cto;
					} else {
						lat = new DirectAllocatedTable((TableCacheKey)fkms.mb.getSubject().getCacheKey(), fkms.mb.isWellFormed(sc, modifications));
					}
					fixups.add(new LateFKFixup(m.getKey().getSymbol(),lat,"maintenance"));
				} else if (m.getValue() instanceof DirectFKState) {
					DirectFKState dfks = (DirectFKState) m.getValue();
					LazyAllocatedTable lat = null;
					CreateTableOperation cto = forwarding.get(dfks.target);
					if (cto != null)
						lat = cto;
					else
						lat = new DirectAllocatedTable((TableCacheKey)dfks.target.getCacheKey(),AdaptiveMTDDLPlannerUtils.isWellFormed(sc, dfks.target, null));
					fixups.add(new LateFKFixup(m.getKey().getSymbol(),lat,"maintenance"));
				} else {
					throw new SchemaException(Pass.PLANNER, "Unknown fk state kind " + m.getValue());
				}
			}
			TableCacheKey subjectCacheKey = (TableCacheKey) mb.getSubject().getCacheKey();
			if (mb.isFlipped()) {
				// new table
				Map<UnqualifiedName,LateFKFixup> mapped = new LinkedHashMap<UnqualifiedName,LateFKFixup>();
				for(LateFKFixup f : fixups) {
					mapped.put(f.getSymbolName(),f);
				}
				CreateTableOperation cto = new CreateTableOperation(sc, mb.getSubject(), mb.scope.getName().getUnqualified(), tenant,
						(mb.isWellFormed(sc, modifications) ? TableState.SHARED : TableState.FIXED), mapped);
				forwarding.put(mb.getSubject(),cto);
				newTabs.add(cto);
				// also schedule a flip for it
				changes.add(new TableFlip(mb.scope, subjectCacheKey, cto, false,
						new CacheInvalidationRecord(mb.scope.getCacheKey(),InvalidationScope.LOCAL)));
			} else {
				// update any fks, but scheduling one fk update for each fixup
				if (mb.getSubject().getState() == TableState.SHARED)
					throw new IllegalStateException("Changing fk targets on shared table");
				CacheInvalidationRecord record = new CacheInvalidationRecord(mb.scope.getCacheKey(),InvalidationScope.LOCAL);
				for(LateFKFixup f : fixups) {
					if (f.getTargetTable() == null) {
						changes.add(new UpdateForeignKeyTargetTable(subjectCacheKey, f.getSymbolName(), new UnqualifiedName(f.getTargetName()),record));
					} else {
						changes.add(new UpdateForeignKeyTargetTable(subjectCacheKey, f.getSymbolName(), f.getTargetTable(), record));
					}
				}				
			}			
		}
	}
	
	
	public static class DependencyTree {
		
		// reverse - key is table, value is fks referencing
		// every value in this is reachable from the original table (whether it was new or not) 
		MultiMap<PETable,TaggedFK> deps;
		final PETenant tenant;
		
		// inverse mapping from PETable to TableScope for this tenant
		final HashMap<PETable,TableScope> scopes;
		
		public DependencyTree(PETenant onTenant) {
			deps = new MultiMap<PETable, TaggedFK>(new HashedCollectionFactory<TaggedFK>());
			this.tenant = onTenant;
			this.scopes = new HashMap<PETable,TableScope>();
		}
		
		public Collection<TaggedFK> getReferring(PETable pet) {
			return deps.get(pet);
		}

		public Map<PETable,TableScope> getScopeMapping() {
			return scopes;
		}
		
		public Set<PETable> getAllTables() {
			LinkedHashSet<PETable> out = new LinkedHashSet<PETable>();
			out.addAll(deps.keySet());
			for(TaggedFK tfk : deps.values()) {
				out.add(tfk.getEnclosing());
			}
			return out;
		}
		
		public void addDependency(SchemaContext sc, PEForeignKey pefk, PETable enc, TableScope encScope, PETable target, TableScope targScope) {
			// for the root set this will never short circuit, since the map is empty
			if (!deps.put(target, new TaggedFK(pefk,enc, encScope)))
				return;
			scopes.put(target,targScope);
			scopes.put(enc,encScope);
			// now process enc
			List<TableScope> yonScopes = sc.findScopesForFKTargets(enc, tenant);
			HashMap<PETable,TableScope> inverse = new HashMap<PETable,TableScope>();
			ListOfPairs<PEForeignKey,PETable> toProcess = new ListOfPairs<PEForeignKey,PETable>();
			for(TableScope ts : yonScopes) {
				PETable backing = ts.getTable(sc);
				inverse.put(backing,ts);
				for(PEKey pek : backing.getKeys(sc)) {
					if (!pek.isForeign()) continue;
					PEForeignKey ipefk = (PEForeignKey) pek;
					if (ipefk.isForward()) continue;
					PETable itarg = ipefk.getTargetTable(sc);
					if (itarg.getCacheKey().equals(enc.getCacheKey())) {
						toProcess.add(ipefk,backing);
					}
				}
			}
			for(Pair<PEForeignKey, PETable> p : toProcess) {
				addDependency(sc,p.getFirst(),p.getSecond(),inverse.get(p.getSecond()),enc, encScope);
			}
		}
		
	}
	
	public static class ModificationBlock {
		
		private static class StateEntry {
			
			private FKState state;
			private FKState orig;
			
			public StateEntry(FKState o) {
				state = o;
				orig = o;
			}
			
			public void update(FKState n) {
				state = n;
			}
			
			public FKState getState() {
				return state;
			}
			
			public boolean isModified() {
				return orig != state;
			}
			
		}
		
		private static class Version {
			
			private boolean wellFormed;
			private boolean flipped;
			
			public Version(boolean wf, boolean fl) {
				this.wellFormed = wf;
				this.flipped = fl;
			}
		}
		
		protected PETable subject;
		protected HashMap<PEForeignKey,StateEntry> refState;
		protected final TableScope scope;
		protected boolean flipped;
		protected Version state;
		
		public ModificationBlock(SchemaContext sc, TableScope onScope, PETable subj) {
			scope = onScope;
			subject = subj;
			refState = new HashMap<PEForeignKey,StateEntry>();
		}
		
		public void buildInitialState(SchemaContext sc, Map<PETable, ModificationBlock> blocks) {
			boolean wellFormed = true;
			for(PEKey pek : subject.getKeys(sc)) {
				if (!pek.isForeign()) continue;
				PEForeignKey pefk = (PEForeignKey) pek;
				if (pefk.isForward()) {
					refState.put(pefk, new StateEntry(new ForwardFKState(pefk.getTargetTableName(sc))));
					wellFormed = false;
				} else {
					PETable targ = pefk.getTargetTable(sc);
					boolean twf = AdaptiveMTDDLPlannerUtils.isWellFormed(sc, targ, null);
					if (!twf) wellFormed = false;
					ModificationBlock mb = blocks.get(targ);
					if (mb == null) {
						refState.put(pefk, new StateEntry(new DirectFKState(targ,twf)));
					} else {
						refState.put(pefk, new StateEntry(new ModifiableFKState(mb, twf)));
					}
				}
			}
			state = new Version(wellFormed,false);
		}

		public PETable getSubject() {
			return subject;
		}

		public void setFlipped() {
			flipped = true;
		}
		
		public boolean isFlipped() {
			return flipped;
		}
		
		public boolean propagateChanges(SchemaContext sc, Map<PETable,ModificationBlock> mods) {
			Version current = state;
			Version nstate = new Version(isWellFormed(sc,mods),flipped);
			boolean propagate = false;
			if (current.flipped != nstate.flipped)
				// we must propagate flips because dependents may need to be flipped as well
				propagate = true;
			if (current.wellFormed != nstate.wellFormed) {
				// if the table is shared and the formedness changed, we will need to flip
				// or if the table used to be not well formed (and fixed) but is now well formed (flip to shared)
				if (subject.getState() == TableState.SHARED || (subject.getState() == TableState.FIXED && nstate.wellFormed))
					setFlipped();
				propagate = true;
			}
			state = nstate;
			return propagate;
		}
		
		// modified if the state was modified, or if a modifiable target and it has been flipped
		public Map<PEForeignKey, FKState> getModified() {
			LinkedHashMap<PEForeignKey, FKState> out = new LinkedHashMap<PEForeignKey,FKState>();
			for(Map.Entry<PEForeignKey, StateEntry> m : refState.entrySet()) {
				if (m.getValue().isModified())
					out.put(m.getKey(),m.getValue().getState());
				else if (m.getValue().getState() instanceof ModifiableFKState) {
					ModifiableFKState mkfs = (ModifiableFKState) m.getValue().getState();
					if (mkfs.mb.isFlipped())
						out.put(m.getKey(),mkfs);
				}
			}
			return out;
		}
		
		// all the mods I can do
		public void unresolveFK(PEForeignKey pefk, UnqualifiedName forwardName) {
			// see if I have existing state
			if (subject.getState() == TableState.SHARED)
				setFlipped();
			StateEntry se = refState.get(pefk);
			FKState e = (se == null ? null : se.getState());
			if (e == null || e instanceof ModifiableFKState) {
				ForwardFKState ns = new ForwardFKState(forwardName);
				if (se == null) {
					se = new StateEntry(ns);
				} else {
					se.update(ns);
				}
				refState.put(pefk, se);
			} else  {
				se.update(new ForwardFKState(forwardName));
			}
		}
		
		public void resolveFK(SchemaContext sc, Map<PETable,ModificationBlock> blocks, PEForeignKey pefk, ModificationBlock mb) {
			if (subject.getState() == TableState.SHARED)
				setFlipped();
			StateEntry se = refState.get(pefk);
			FKState e = (se == null ? null : se.getState());
			if (e == null || e instanceof ForwardFKState) {
				if (se == null) {
					se = new StateEntry(new ModifiableFKState(mb,mb.isWellFormed(sc, blocks)));
				} else {
					se.update(new ModifiableFKState(mb,mb.isWellFormed(sc, blocks)));
				}
				refState.put(pefk, se);
			} else {
				ModifiableFKState em = (ModifiableFKState) e;
				if (em.mb != mb) {
					se.update(new ModifiableFKState(mb,mb.isWellFormed(sc,blocks)));
				}
			}
		}
		
		public boolean isWellFormed(SchemaContext sc, Map<PETable, ModificationBlock> mods) {
			// guard against loops - check direct and forward first
			for(Map.Entry<PEForeignKey, StateEntry> m : refState.entrySet()) {
				if (m.getValue().getState() instanceof ModifiableFKState) continue;
				FKState fkm = m.getValue().getState();
				if (fkm instanceof ForwardFKState) return false;
				if (fkm instanceof DirectFKState) {
					if (!fkm.wasWellFormed()) return false;
				}				
			}
			// now do the others
			for(Map.Entry<PEForeignKey, StateEntry> m : refState.entrySet()) {
				if (!(m.getValue().getState() instanceof ModifiableFKState)) continue;
				ModifiableFKState mfks = (ModifiableFKState) m.getValue().getState();
				if (mfks.modify(sc, mods)) {
					if (subject.getState() == TableState.SHARED)
						setFlipped();
				}
				if (!mfks.wasWellFormed()) return false;
			}
			return true;
		}		
	}

	// fk states
	protected static abstract class FKState {
		
		protected boolean lastWF;
		protected boolean lastFlipped;
		
		public FKState(boolean initWF) {
			lastWF = initWF;
			lastFlipped = false;
		}
		
		public abstract boolean modify(SchemaContext sc, Map<PETable,ModificationBlock> mods);
		
		public boolean wasWellFormed() {
			return lastWF;
		}
		
		public boolean wasFlipped() {
			return lastFlipped;
		}
		
	}

	// a table for which there is no mod block - it's state can never change
	protected static class DirectFKState extends FKState {
		
		protected PETable target;
		public DirectFKState(PETable targ, boolean wf) {
			super(wf);
			target = targ;
		}
		
		@Override
		public boolean modify(SchemaContext sc, Map<PETable, ModificationBlock> mods) {
			return false;
		}
	}

	// forward state - it might change
	protected static class ForwardFKState extends FKState {
		
		protected Name targTab;
		public ForwardFKState(Name unq) {
			super(false);
			targTab = unq;
		}
		@Override
		public boolean modify(SchemaContext sc,
				Map<PETable, ModificationBlock> mods) {
			return false;
		}
	}
	
	protected static class ModifiableFKState extends FKState {
		
		protected ModificationBlock mb;
		public ModifiableFKState(ModificationBlock targ, boolean wf) {
			super(wf);
			mb = targ;
		}
		@Override
		public boolean modify(SchemaContext sc, Map<PETable, ModificationBlock> mods) {
			boolean wf = mb.isWellFormed(sc, mods);
			boolean sig = false;
			if (lastWF != wf) {
				lastWF = wf;
				sig = true;
			}
			if (lastFlipped != mb.isFlipped()) {
				lastFlipped = mb.isFlipped();
				sig = true;
			}
			return sig;
		}
	}
	
	public static class TaggedFK {
		
		protected final PEForeignKey pefk;
		protected final PETable enclosing;
		protected final TableScope scope; // scope associated with enclosing
		
		public TaggedFK(PEForeignKey pefk, PETable enc, TableScope ts) {
			this.pefk = pefk;
			this.enclosing = enc;
			this.scope = ts;
		}
		
		public PEForeignKey getFK() {
			return pefk;
		}
		
		public PETable getEnclosing() {
			return enclosing;
		}

		public TableScope getScopeOfEnclosing() {
			return scope;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((enclosing == null) ? 0 : enclosing.hashCode());
			result = prime * result + ((pefk == null) ? 0 : pefk.hashCode());
			result = prime * result + ((scope == null) ? 0 : scope.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			TaggedFK other = (TaggedFK) obj;
			if (enclosing == null) {
				if (other.enclosing != null)
					return false;
			} else if (!enclosing.equals(other.enclosing))
				return false;
			if (pefk == null) {
				if (other.pefk != null)
					return false;
			} else if (!pefk.equals(other.pefk))
				return false;
			if (scope == null) {
				if (other.scope != null)
					return false;
			} else if (!scope.equals(other.scope))
				return false;
			return true;
		}
		
	}

}
