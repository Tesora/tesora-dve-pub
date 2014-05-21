// OS_STATUS: public
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.common.catalog.FKMode;
import com.tesora.dve.common.catalog.IndexType;
import com.tesora.dve.common.catalog.Key;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.jg.DunPart;
import com.tesora.dve.sql.jg.JoinEdge;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.validate.ForeignKeyValidateResult;
import com.tesora.dve.sql.schema.validate.ValidateResult;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.util.Functional;

public class PEForeignKey extends PEKey {
	
	private ForeignKeyAction updateAction;
	private ForeignKeyAction deleteAction;
	private Name targetTableName;
	private SchemaEdge<PETable> targetTable;
	private UnqualifiedName physicalSymbol;
	
	@SuppressWarnings("unchecked")
	public PEForeignKey(SchemaContext pc, Name n, PETable tab, Name missingTableName, List<PEKeyColumn> cols, 
			ForeignKeyAction updateAction, ForeignKeyAction deleteAction) {
		super(n, IndexType.BTREE, cols, null);
		setConstraint(ConstraintType.FOREIGN);
		this.updateAction = updateAction;
		this.deleteAction = deleteAction;
		targetTableName = missingTableName;
		if (tab != null)
			targetTable = StructuralUtils.buildEdge(pc,tab, false);
	}

	public void setSymbol(Name symbol) {
		super.setSymbol(symbol);
		if (symbol != null)
			physicalSymbol = symbol.getUnqualified();
	}

	public void setPhysicalSymbol(UnqualifiedName psym) {
		physicalSymbol = psym;
	}
	
	public UnqualifiedName getPhysicalSymbol() {
		return physicalSymbol;
	}
	
	
	public ForeignKeyAction getUpdateAction() {
		return updateAction;
	}
	
	public ForeignKeyAction getDeleteAction() {
		return deleteAction;
	}
	
	public boolean isForward() {
		return targetTableName != null;
	}
	
	public PETable getTargetTable(SchemaContext sc) {
		if (targetTable == null) return null;
		return targetTable.get(sc);
	}
	
	@SuppressWarnings("unchecked")
	public void setTargetTable(SchemaContext sc, PETable pet) {
		if (pet != null) {
			targetTable = StructuralUtils.buildEdge(sc,pet, false);
			targetTableName = null;
		}
	}
	
	public void revertToForward(SchemaContext sc) {
		PETable targ = targetTable.get(sc);
		targetTableName = new QualifiedName(targ.getPEDatabase(sc).getName().getUnqualified(),targ.getName().getUnqualified());
		revertToForwardInternal(sc);
	}
	
	// use ONLY in mt fk support, and ONLY in the tschema
	public void revertToForwardMT(SchemaContext sc, Name forwardTableName) {
		if (forwardTableName.isQualified())
			targetTableName = forwardTableName;
		else
			targetTableName = new QualifiedName(targetTable.get(sc).getPEDatabase(sc).getName().getUnqualified(),forwardTableName.getUnqualified());
		revertToForwardInternal(sc);
	}
	
	private void revertToForwardInternal(SchemaContext sc) {
		for(PEKeyColumn pekc : getKeyColumns()) {
			PEForeignKeyColumn pefkc = (PEForeignKeyColumn) pekc;
			pefkc.revertToForward(sc);
		}
		targetTable = null;		
	}
	
	public Name getTargetTableName(SchemaContext sc) {
		if (targetTable != null) {
			if (!targetTable.get(sc).getPEDatabase(sc).getCacheKey().equals(getTable(sc).getPEDatabase(sc).getCacheKey())) {
				// target table in different database - use fully qualified name
				return targetTable.get(sc).getName().prefix(targetTable.get(sc).getPEDatabase(sc).getName());
			}
			return targetTable.get(sc).getName().getUnqualified();
		}
		if (targetTableName.isQualified()) {
			QualifiedName qn = (QualifiedName) targetTableName;
			UnqualifiedName dbn = qn.getNamespace();
			if (getTable(sc).getPEDatabase(sc).getName().equals(dbn))
				return targetTableName.getUnqualified();
			return targetTableName;
		}
		return targetTableName.getUnqualified();
	}

	// use ONLY in mt fk support, and ONLY in the tschema
	public void resetTargetTableName(UnqualifiedName unq) {
		if (!isForward()) throw new SchemaException(Pass.PLANNER,"Internal error: attempt to set target table name when not forward");
		targetTableName = unq;
	}
	
	public List<PEColumn> getTargetColumns(SchemaContext sc) {
		if (isForward()) return null;
		List<PEColumn> out = new ArrayList<PEColumn>();
		for(PEKeyColumn pekc : getKeyColumns()) {
			PEForeignKeyColumn pefkc = (PEForeignKeyColumn) pekc;
			out.add(pefkc.getTargetColumn(sc));
		}
		return out;
	}
	
	public PEKey findPrefixKey(SchemaContext sc, PETable inTable) {
		List<PEKey> candidates = new ArrayList<PEKey>();
		for(PEKey pek : inTable.getKeys(sc)) {
			if (pek.getConstraint() == ConstraintType.FOREIGN) continue;
			if (getKeyColumns().size() > pek.getColumns(sc).size()) continue;
			candidates.add(pek);
		}
		for(int i = 0; i < getKeyColumns().size(); i++) {
			PEColumn spc = getKeyColumns().get(i).getColumn();
			for(Iterator<PEKey> iter = candidates.iterator(); iter.hasNext();) {
				PEKey pek = iter.next();
				if (!pek.getColumns(sc).get(i).equals(spc))
					iter.remove();
			}
		}
		if (candidates.isEmpty()) return null;
		return candidates.get(0);
	}
	
	public void addColumn(int index, PEColumn src, UnqualifiedName targ) {
		PEForeignKeyColumn pefkc = new PEForeignKeyColumn(src, targ, isForward());
		columns.add(index, pefkc);
		pefkc.setKey(this);
	}
	
	public PEKey buildPrefixKey(SchemaContext sc, PETable inTable) {
		List<PEKeyColumn> cols = new ArrayList<PEKeyColumn>();
		for(PEKeyColumn c : getKeyColumns()) {
			cols.add(new PEKeyColumn(c.getColumn(),null,-1));
		}
		return new PEKey(null,IndexType.BTREE,cols,null,true);
	}
	
	public static PEForeignKey load(Key k, SchemaContext sc, PETable enclosingTable) {
		PEForeignKey p = (PEForeignKey) sc.getLoaded(k,null);
		if (p == null) {
			if (k.isForeignKey())
				return new PEForeignKey(sc,k, enclosingTable);
			throw new SchemaException(Pass.SECOND,"Invalid call to PEForeignKey.load - found key of type " + k.getType());
		}
		return p;
	}

	
	@SuppressWarnings("unchecked")
	protected PEForeignKey(SchemaContext sc, Key k, PETable enclosingTable) {
		super(sc,k, enclosingTable);
		this.updateAction = ForeignKeyAction.fromPersistent(k.getFKUpdateAction());
		this.deleteAction = ForeignKeyAction.fromPersistent(k.getFKDeleteAction());
		if (k.getReferencedTable() != null) {
			this.targetTable = StructuralUtils.buildEdge(sc,PETable.load(k.getReferencedTable(),sc), true);
			this.targetTableName = null;
		} else {
			this.targetTable = null;
			this.targetTableName = new QualifiedName(new UnqualifiedName(k.getReferencedSchemaName()), new UnqualifiedName(k.getReferencedTableName()));
		}
		setPhysicalSymbol(new UnqualifiedName(k.getPhysicalSymbol()));
	}
	
	private void updatePersistent(SchemaContext sc, Key p) throws PEException {
		p.setFKDeleteAction(deleteAction.getPersistent());
		p.setFKUpdateAction(updateAction.getPersistent());
		p.setPersisted(persisted);
		p.setPhysicalSymbol(physicalSymbol.getUnquotedName().get());
		if (targetTable != null) {
			p.setReferencedTable(targetTable.get(sc).persistTree(sc));
		} else if (targetTableName != null) {
			String dbName = null;
			String tabName = null;
			if (targetTableName.isQualified()) {
				QualifiedName qn = (QualifiedName) targetTableName;
				dbName = qn.getNamespace().getUnquotedName().get();
				tabName = qn.getUnqualified().getUnquotedName().get();
			} else {
				tabName = targetTableName.getUnquotedName().get();
			}
			p.setReferencedTable(dbName, tabName);
		} 
	}
	
	@Override
	protected void populateNew(SchemaContext sc, Key p) throws PEException {
		super.populateNew(sc,p);
		updatePersistent(sc,p);
	}
	
	@Override
	protected void updateExisting(SchemaContext sc, Key p) throws PEException {
		super.updateExisting(sc, p);
		updatePersistent(sc,p);
	}	
	
	public boolean isColocated(SchemaContext sc) {
		PETable leftTab = getTable(sc);
		PETable rightTab = getTargetTable(sc);
		if (rightTab == null) return false;
		Map<PEColumn,PEColumn> mapping = new HashMap<PEColumn,PEColumn>();
		for(PEKeyColumn pekc : getKeyColumns()) {
			PEForeignKeyColumn pefkc = (PEForeignKeyColumn) pekc;
			mapping.put(pefkc.getColumn(), pefkc.getTargetColumn(sc));
		}
		TableKey lk = new TableKey(leftTab,1);
		TableKey rk = new TableKey(rightTab,2);
		DunPart lp = new DunPart(lk,1);
		DunPart rp = new DunPart(rk,2);
		return JoinEdge.computeColocated(sc, lp,Collections.singleton(lk), rp, rk, mapping, null,true);
	}
	
	@Override
	public void checkValid(SchemaContext sc, List<ValidateResult> results) {
		if (isForward() || !isPersisted()) return;
		
		boolean hasUniqueTargetCol = false;
		for(int i = 0; i < getKeyColumns().size(); i++) {
			PEForeignKeyColumn pefkc = (PEForeignKeyColumn) getKeyColumns().get(i);
			for(PEKey pek : pefkc.getTargetColumn(sc).getReferencedBy(sc)) {
				if (pek.isValidFkTarget(sc))
					hasUniqueTargetCol = true;
			}				
			
		}
		if (!hasUniqueTargetCol ) {
			results.add(new ForeignKeyValidateResult(sc,this,ForeignKeyValidateResult.FKValidateKind.NO_UNIQUE_KEY,true));
			return;
		}
		if (!isColocated(sc)) {
			// figure out the fk mode on the db in order to determine whether this is a warning or error
			FKMode fkm = getTable(sc).getPEDatabase(sc).getFKMode();
			results.add(new ForeignKeyValidateResult(sc,this,ForeignKeyValidateResult.FKValidateKind.NOT_COLOCATED,(fkm == FKMode.STRICT)));
		}
	}

	@Override
	public boolean collectDifferences(SchemaContext sc, List<String> messages, Persistable<PEKey, Key> oth,
			boolean first, @SuppressWarnings("rawtypes") Set<Persistable> visited) {
		PEForeignKey other = (PEForeignKey) oth.get();
		
		if (visited.contains(this) && visited.contains(other)) {
			return false;
		}
		visited.add(this);
		visited.add(other);
		
		if (maybeBuildDiffMessage(sc,messages, "delete action", getDeleteAction(), other.getDeleteAction(), first, visited))
			return true;
		if (maybeBuildDiffMessage(sc,messages, "update action", getUpdateAction(), other.getUpdateAction(), first, visited))
			return true;
		// then make sure that the targetTableName value is the same in both - this indicates whether the key is
		// forward or not
		if (maybeBuildDiffMessage(sc, messages, "forward ref", targetTableName, other.targetTableName, first, visited))
			return true;
		if (maybeBuildDiffMessage(sc, messages, "target table", getTargetTable(sc), other.getTargetTable(sc), first, visited))
			return true;
		
		if (super.collectDifferences(sc, messages, oth, first, visited))
			return true;
		return false;
	}	

	@Override
	public PEKey copy(SchemaContext sc, PETable containingTable) {
		List<PEKeyColumn> contained = new ArrayList<PEKeyColumn>();
		for(PEKeyColumn p : getKeyColumns()) {
			contained.add(p.copy(sc, containingTable));
		}
		PETable targetTab = (targetTable != null ? targetTable.get(sc) : null);
		PEForeignKey out = new PEForeignKey(sc,getName(),targetTab,targetTableName,contained,updateAction,deleteAction);
		out.setSymbol(getSymbol());
		out.setPhysicalSymbol(getPhysicalSymbol());
		return out;
	}
	
	public static void doForeignKeyChecks(SchemaContext sc, DeleteStatement ds) {
		TableInstance targ = ds.getTargetDeleteEdge().get();
		PETable targTab = targ.getAbstractTable().asTable();
		processFKChecksOnDelete(sc,targTab,null,new HashSet<PETable>());
	}
	
	public static void doForeignKeyChecks(SchemaContext sc, UpdateStatement us) {
		MultiMap<PETable,PEColumn> updated = new MultiMap<PETable,PEColumn>();
		for(ExpressionNode en : us.getUpdateExpressions()) {
			FunctionCall fc = (FunctionCall) en;
			ColumnInstance ci = (ColumnInstance) fc.getParametersEdge().get(0);
			PEColumn col = ci.getPEColumn();
			updated.put(col.getTable().asTable(),col);
		}
		for(PETable pet : updated.keySet()) {
			Collection<PEColumn> sub = updated.get(pet);
			if (sub == null || sub.isEmpty()) continue;
			processFKChecksOnUpdate(sc,pet,Functional.toList(sub),null,new HashSet<PETable>());
		}
	}
	
	// fk checks when modifying a parent:
	// parent op	child action	result
	// delete		restrict		do nothing
	// delete		no action		do nothing
	// delete		set null		if child is bcast allow, otherwise prohibit
	// delete		cascade			do nothing (allow)
	// update		restrict		do nothing
	// update		no action		do nothing
	// update		set null		if child is bcast allow, otherwise prohibit
	// update		cascade			if child is bcast allow, otherwise prohibit
	//
	// in all cases, when an fk is not persisted, break the chain (since the action would not propogate on the sites)
	// in mt mode if both tables are tenant id distributedonly allow the delete/update to proceed since it will only
	// effect one site.
	
	private static void processFKChecksOnDelete(SchemaContext sc,PETable modifiedTable,PETable referencingTable,Set<PETable> processed) {
		if (referencingTable == null) {
			for(SchemaCacheKey<PEAbstractTable<?>> referring : modifiedTable.getReferencingTables()) {
				PETable actual = sc.getSource().find(sc, referring).asTable();
				processFKChecksOnDelete(sc,modifiedTable,actual,processed);
			}
		} else {
			if (!processed.add(referencingTable)) return;
			RangeDistribution mr = modifiedTable.getDistributionVector(sc).getDistributedWhollyOnTenantColumn(sc);
			RangeDistribution rr = referencingTable.getDistributionVector(sc).getDistributedWhollyOnTenantColumn(sc);
			boolean bothTenantID = (mr != null && rr != null && mr.getCacheKey().equals(rr.getCacheKey()));
			for(PEKey pek : referencingTable.getKeys(sc)) {
				if (!pek.isForeign()) continue;
				PEForeignKey pefk = (PEForeignKey) pek;
				if (!pefk.isPersisted()) continue;
				PETable targTab = pefk.getTargetTable(sc);
				if (targTab == null) continue;
				if (targTab != modifiedTable) continue;
				ForeignKeyAction fka = pefk.getDeleteAction();
				boolean mustRecurse = false;
				if (fka == ForeignKeyAction.NO_ACTION || fka == ForeignKeyAction.RESTRICT) {
					// no cascade, any action will stop on the individual p sites
					mustRecurse = false;
				} else if (fka == ForeignKeyAction.CASCADE) {
					// we need to check referrers
					mustRecurse = true;
				} else if (fka == ForeignKeyAction.SET_NULL) {
					if (!referencingTable.getDistributionVector(sc).isBroadcast() && !bothTenantID) {
						throw new SchemaException(Pass.PLANNER, "Unable to delete from " + modifiedTable.getName() 
								+ " due to set null action on foreign key " + pefk.getName()
								+ " in table " + referencingTable.getName());
					}
					mustRecurse = true;
					// we've now converted a delete action to an update action, proceed as an update - use a new set
					processFKChecksOnUpdate(sc,referencingTable,pefk.getColumns(sc),null,new HashSet<PETable>());
				} else {
					throw new SchemaException(Pass.PLANNER, "Unknown foreign key action kind: " + fka);
				}
				if (mustRecurse)
					processFKChecksOnDelete(sc,referencingTable,null,processed);
			}
		}
	}
	
	private static void processFKChecksOnUpdate(SchemaContext sc,PETable modifiedTable,List<PEColumn> modifiedColumns, 
			PETable referencingTable,Set<PETable> processed) {
		if (referencingTable == null) {
			for(SchemaCacheKey<PEAbstractTable<?>> referring : modifiedTable.getReferencingTables()) {
				PETable actual = sc.getSource().find(sc, referring).asTable();
				processFKChecksOnUpdate(sc,modifiedTable,modifiedColumns,actual,processed);
			}
		} else {
			if (!processed.add(referencingTable)) return;
			RangeDistribution mr = modifiedTable.getDistributionVector(sc).getDistributedWhollyOnTenantColumn(sc);
			RangeDistribution rr = referencingTable.getDistributionVector(sc).getDistributedWhollyOnTenantColumn(sc);
			boolean bothTenantID = (mr != null && rr != null && mr.getCacheKey().equals(rr.getCacheKey()));
			for(PEKey pek : referencingTable.getKeys(sc)) {
				if (!pek.isForeign()) continue;
				PEForeignKey pefk = (PEForeignKey) pek;
				if (!pefk.isPersisted()) continue;
				PETable targTab = pefk.getTargetTable(sc);
				if (targTab == null) continue;
				if (targTab != modifiedTable) continue;
				List<PEColumn> targCols = pefk.getTargetColumns(sc);
				if (!CollectionUtils.isEqualCollection(modifiedColumns, targCols)) continue;
				ForeignKeyAction updateAction = pefk.getUpdateAction();
				boolean mustRecurse = false;
				if (updateAction == ForeignKeyAction.RESTRICT || updateAction == ForeignKeyAction.NO_ACTION) {
					mustRecurse = false;
				} else if (updateAction == ForeignKeyAction.SET_NULL || updateAction == ForeignKeyAction.CASCADE) {
					if (referencingTable.getDistributionVector(sc).isBroadcast() || bothTenantID) {
						// must propagate the update
						mustRecurse = true;
					} else {
						throw new SchemaException(Pass.PLANNER, "Unable to update table " + modifiedTable.getName() 
								+ " due to cascade/set null action on foreign key " + pefk.getName()
								+ " in table " + referencingTable.getName());
					}
				}
				if (mustRecurse)
					processFKChecksOnUpdate(sc,referencingTable,pefk.getColumns(sc),null,processed);
			}
		}
	}
}
