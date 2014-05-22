package com.tesora.dve.sql.transform;

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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.expression.VisitorContext;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryPredicate;

public class CopyContext extends VisitorContext {

	private Map<TableKey, TableKey> tables;
	private Map<ColumnKey, ColumnKey> columns;
	
	// temp tables sometimes map expressions into columns - so the mapper
	// has to go back through contexts looking for a mapping context
	private Map<ExpressionKey, ColumnKey> exprs;
	
	private Map<ExpressionAlias, ExpressionAlias> exprAliases;
	
	private String createdBy;
	private boolean fixed;
	
	private Stack<CopyScope> scopes;
	
	public CopyContext(String creator) {
		tables = new HashMap<TableKey, TableKey>();
		columns = new HashMap<ColumnKey, ColumnKey>();
		exprs = new HashMap<ExpressionKey, ColumnKey>();
		exprAliases = new HashMap<ExpressionAlias, ExpressionAlias>();
		createdBy = creator;
		fixed = false;
		scopes = new Stack<CopyScope>();
	}

	public void setFixed(boolean v) {
		fixed = v;
	}
	
	public boolean isFixed() {
		return fixed;
	}
	
	public TableKey getTableKey(TableKey tk) {
		return tables.get(tk);
	}
	
	public ColumnKey getColumnKey(ColumnKey ck) {
		return columns.get(ck);
	}
	
	public ColumnKey getColumnKey(ExpressionKey ek) {
		return exprs.get(ek);
	}
	
	public boolean isTargetTable(TableKey tk) {
		return tables.containsValue(tk);
	}
	
	public TableInstance getTableInstance(TableInstance in) {
		TableKey tk = in.getTableKey();
		TableKey otk = tables.get(tk);
		if (otk == null) return null;
		return otk.toInstance();
	}
	
	public ColumnInstance getColumnInstance(ColumnInstance in) {
		ColumnKey ck = in.getColumnKey();
		ColumnKey fk = columns.get(ck);
		if (fk == null) return null;
		return fk.toInstance();
	}
	
	public ExpressionAlias getExpressionAlias(ExpressionAlias in) {
		return exprAliases.get(in);
	}
	
	public ColumnInstance put(ColumnInstance orig, ColumnInstance repl) {
		checkFixed(orig, repl);
		// also put the tables
		put(orig.getTableInstance(), repl.getTableInstance());
		put(orig.getColumnKey(), repl.getColumnKey());
		return repl;
	}
	
	public TableInstance put(TableInstance orig, TableInstance repl) {
		checkFixed(orig, repl);
		put(orig.getTableKey(), repl.getTableKey());
		return repl;
	}

	public ExpressionAlias putExpressionAlias(ExpressionAlias orig, ExpressionAlias repl) {
		checkFixed(orig, repl);
		exprAliases.put(orig, repl);
		return repl;
	}
	
	public void removeTable(final TableKey tk) {
		remove(new UnaryPredicate<TableKey>() {

			@Override
			public boolean test(TableKey object) {
				return object.equals(tk);
			}
			
		});
	}
	
	public void removeTable(final PEAbstractTable<?> pet) {
		remove(new UnaryPredicate<TableKey>() {

			@Override
			public boolean test(TableKey object) {
				return object.getAbstractTable() == pet;
			}
			
		});
	}
	
	private void remove(UnaryPredicate<TableKey> test) {
		for(Iterator<Map.Entry<TableKey,TableKey>> iter = tables.entrySet().iterator(); iter.hasNext();) {
			Map.Entry<TableKey,TableKey> me = iter.next();
			if (test.test(me.getKey()) || test.test(me.getValue()))
				iter.remove();
		}
		for(Iterator<Map.Entry<ColumnKey, ColumnKey>> iter = columns.entrySet().iterator(); iter.hasNext();) {
			Map.Entry<ColumnKey, ColumnKey> me = iter.next();
			if (test.test(me.getKey().getTableKey()) || test.test(me.getValue().getTableKey()))
				iter.remove();
		}
		
	}
	
	public void put(ColumnKey orig, ColumnKey repl) {
		columns.put(orig, repl);
	}
	
	public void put(TableKey orig, TableKey repl) {
		tables.put(orig, repl);
	}
	
	public void put(ExpressionKey orig, ColumnKey repl) {
		exprs.put(orig, repl);
	}
	
	public Collection<TableKey> getMappedToTables() {
		return tables.values();
	}
	
	private void checkFixed(ExpressionNode orig, ExpressionNode repl) {
		if (fixed)
			throw new SchemaException(Pass.REWRITER, "Attempt to modify fixed mapping by adding '" + orig + "' => '" + repl + "'");
	}
	
	public static CopyContext compose(CopyContext a, CopyContext b) {
		CopyContext res = new CopyContext("compose");
		res.take(a);
		res.take(b);
		return res;
	}
	
	public void take(CopyContext other) {
		tables.putAll(other.tables);
		columns.putAll(other.columns);
	}	
	
	public String getCreatedBy() {
		return createdBy;
	}
	
	public void pushScope() {
		scopes.push(new CopyScope());
	}
	
	public void popScope() {
		scopes.pop();
	}
	
	public void registerProjectingStatement(ProjectingStatement ss) {
		if (scopes.isEmpty()) return;
		scopes.peek().getProjectingStatements().add(ss);
	}
	
	public void registerVariable(VariableInstance vi) {
		if (scopes.isEmpty()) return;
		scopes.peek().getVariables().add(vi);
	}
	
	public ListSet<ProjectingStatement> getProjectingStatements() {
		return scopes.peek().getProjectingStatements();
	}
	
	public ListSet<VariableInstance> getVariables() {
		return scopes.peek().getVariables();
	}
	
	private static class CopyScope {
		
		private ListSet<ProjectingStatement> selects;
		private ListSet<VariableInstance> variables;
		
		public CopyScope() {
			selects = new ListSet<ProjectingStatement>();
			variables = new ListSet<VariableInstance>();
		}
		
		public ListSet<ProjectingStatement> getProjectingStatements() {
			return selects;
		}
		
		public ListSet<VariableInstance> getVariables() {
			return variables;
		}
	}

}
