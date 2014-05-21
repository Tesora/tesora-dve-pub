// OS_STATUS: public
package com.tesora.dve.sql.statement.dml;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.Scope;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.transform.constraints.PlanningConstraint;
import com.tesora.dve.sql.util.ListSet;

public class DerivedInfo {

	protected DMLStatement on;
	// there are some things we care about for pretty much all dml statements
	// local table keys, nested stmts, groups, databases
	protected ListSet<TableKey> localTableKeys;
	protected ListSet<ProjectingStatement> nestedQueries;
	protected ListSet<VariableInstance> localVariables;
	protected ListSet<FunctionCall> functions;
	// any constraints - not always computed
	protected List<PlanningConstraint> constraints = null;
	
	protected ListSet<ColumnKey> correlatedOuterColumns;
	protected boolean computedCorrelated = false;
	
	// used to set the timestamp variable on the backend
	protected boolean setTimestampVariable = false;
	
	public DerivedInfo(DMLStatement dmls) {
		on = dmls;
		localTableKeys = new ListSet<TableKey>();
		nestedQueries = new ListSet<ProjectingStatement>();
		localVariables = new ListSet<VariableInstance>();
		functions = new ListSet<FunctionCall>();
		correlatedOuterColumns = new ListSet<ColumnKey>();
	}
	
	public void take(DerivedInfo other) {
		localTableKeys.addAll(other.localTableKeys);
		localVariables.addAll(other.localVariables);
		addNestedStatements(other.nestedQueries);
		setTimestampVariable = other.setTimestampVariable;
		constraints = (other.constraints == null ? null : new ArrayList<PlanningConstraint>(other.constraints));
		correlatedOuterColumns.addAll(other.correlatedOuterColumns);
		computedCorrelated = other.computedCorrelated;
	}
	
	public void copyTake(DerivedInfo other) {
		localTableKeys.addAll(other.localTableKeys);
		localVariables.addAll(other.localVariables);
		setTimestampVariable = other.setTimestampVariable;
		constraints = (other.constraints == null ? null : new ArrayList<PlanningConstraint>(other.constraints));
		correlatedOuterColumns.addAll(other.correlatedOuterColumns);
		computedCorrelated = other.computedCorrelated;
	}
	
	public void takeScope(Scope s) {
		localTableKeys.addAll(s.getLocalTables());
		addNestedStatements(s.getNestedQueries());
		localVariables.addAll(s.getVariables());
		functions.addAll(s.getFunctions());
	}
	
	public void takeCopy(CopyContext cc) {
		addNestedStatements(cc.getProjectingStatements());
		localVariables.addAll(cc.getVariables());
	}
	
	public void reload(SchemaContext sc) {
		for(TableKey tk : localTableKeys)
			tk.reload(sc);
		// also do all nested
		for(DMLStatement n : nestedQueries) {
			n.getDerivedInfo().reload(sc);
		}
	}
	
	public void addLocalTables(Collection<TableKey> localTabs) {
		if (localTabs != null)
			localTableKeys.addAll(localTabs);
	}
	
	public void addLocalTable(TableKey... local) {
		for(TableKey tk : local)
			localTableKeys.add(tk);
	}
	
	public void addNestedStatements(Collection<ProjectingStatement> ns) {
		if (ns != null) {
			nestedQueries.addAll(ns);
		}
	}

	public ListSet<TableKey> getLocalTableKeys() {
		return localTableKeys;
	}
	
	public ListSet<TableKey> getAllTableKeys() {
		ListSet<TableKey> all = new ListSet<TableKey>();
		all.addAll(localTableKeys);
		for(DMLStatement ss : nestedQueries) {
			if (ss == on) continue;
			all.addAll(ss.getDerivedInfo().getAllTableKeys());
		}
		return all;
	}

	public void addLocalTable(TableKey tk) {
		localTableKeys.add(tk);
	}
	
	// used in nested queries
	public void removeLocalTable(PEAbstractTable<?> vt) {
		for(Iterator<TableKey> iter = localTableKeys.iterator(); iter.hasNext();) {
			TableKey tk = iter.next();
			if (tk.getTable() == vt)
				iter.remove();
		}
	}
	
	
	public void clearLocalTables() {
		localTableKeys.clear();
	}

	public List<PEStorageGroup> getStorageGroups(SchemaContext sc) {
		List<PEStorageGroup> out = new ListSet<PEStorageGroup>();
		for(TableKey tk : getAllTableKeys()) {
			out.add(tk.getAbstractTable().getStorageGroup(sc));
		}
		return out;
	}
	
	// statements can override this if they want
	public Database<?> getDatabase(SchemaContext sc) {
		if (localTableKeys.isEmpty()) {
			ListSet<TableKey> tabs = getAllTableKeys();
			return tabs.get(0).getTable().getDatabase(sc);
		}
		return localTableKeys.get(0).getTable().getDatabase(sc);
	}

	public ListSet<Database<?>> getDatabases(SchemaContext sc) {
		ListSet<Database<?>> out = new ListSet<Database<?>>();
		ListSet<TableKey> tabs = getAllTableKeys();
		for(TableKey tk : tabs)
			out.add(tk.getTable().getDatabase(sc));
		return out;
	}
	
	public ListSet<ProjectingStatement> getLocalNestedQueries() {
		return nestedQueries;
	}
	
	public ListSet<ProjectingStatement> getAllNestedQueries() {
		ListSet<ProjectingStatement> out = new ListSet<ProjectingStatement>();
		for(DMLStatement ss : nestedQueries) {
			if (ss == on) continue;
			if (ss instanceof ProjectingStatement)
				out.add((ProjectingStatement)ss);
			out.addAll(ss.getDerivedInfo().getAllNestedQueries());
		}
		return out;
	}
	
	public ListSet<ProjectingStatement> getNestedQueries(LanguageNode reachableFrom) {
		return getNestedQueries(Collections.singleton(reachableFrom));
	}
	
	public <T extends LanguageNode> ListSet<ProjectingStatement> getNestedQueries(Collection<T> reachableFrom) {
		ListSet<ProjectingStatement> out = new ListSet<ProjectingStatement>();
		if (nestedQueries.isEmpty()) return out;
		for(ProjectingStatement ss : nestedQueries) {
			if (reachableFrom == null) continue;
			if (ss.ifAncestor(reachableFrom) != null)
				out.add(ss);
		}
		return out;
	}
	
	public ListSet<VariableInstance> getLocalVariables() {
		return localVariables;
	}
	
	public ListSet<VariableInstance> getAllVariables() {
		ListSet<VariableInstance> out = new ListSet<VariableInstance>();
		out.addAll(localVariables);
		if (nestedQueries.isEmpty()) return out;
		for(DMLStatement ss : nestedQueries) { 
			if (ss == on) continue;
			out.addAll(ss.getDerivedInfo().getAllVariables());
		}
		return out;
	}
	
	public ListSet<FunctionCall> getFunctions() {
		return functions;
	}

	public boolean doSetTimestampVariable() {
		return setTimestampVariable;
	}

	public void setSetTimestampVariable(boolean setTimestampVariable) {
		this.setTimestampVariable = setTimestampVariable;
	}
	
	public void setConstraints(List<PlanningConstraint> anything) {
		if (constraints == null)
			constraints = new ArrayList<PlanningConstraint>(anything);
		else
			constraints.addAll(anything);
	}
	
	public List<PlanningConstraint> getConstraints() {
		return constraints;
	}
	
	public ListSet<ColumnKey> getCorrelatedColumns() {
		if (!computedCorrelated) {
			computedCorrelated = true;
			ListSet<TableKey> mineAndDescendants = getAllTableKeys();
			for(ColumnInstance ci : ColumnInstanceCollector.getColumnInstances(on)) {
				ColumnKey ck = ci.getColumnKey();
				if (mineAndDescendants.contains(ck.getTableKey()) 
						|| ck.getTableKey().getAbstractTable().isVirtualTable() 
						|| ck.getTableKey().getAbstractTable().isView())
					// one of mine
					continue;
				// someone else's
				correlatedOuterColumns.add(ck);
			}
		}
		return correlatedOuterColumns;
	}
	
	public void clearCorrelatedColumns(){
		correlatedOuterColumns.clear();
		computedCorrelated = false;
	}
	
	// view support
	public void remap(Map<Long,Long> forwarding) {
		for(TableKey tk : localTableKeys) {
			long oldn = tk.getNode();
			Long newObj = forwarding.get(oldn);
			if (newObj == null)
				continue;
			tk.setNode(newObj);
		}
		for(ProjectingStatement ps : nestedQueries) {
			ps.getDerivedInfo().remap(forwarding);
		}
	}
	
}
