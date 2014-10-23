package com.tesora.dve.sql.expression;

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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.errmap.DVEErrors;
import com.tesora.dve.errmap.ErrorInfo;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.Alias;
import com.tesora.dve.sql.node.expression.AliasInstance;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.NameInstance;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.node.expression.WildcardTable;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.parser.LexicalLocation;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.Capability;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.MultiMapLookup;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.Schema;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaLookup;
import com.tesora.dve.sql.schema.SubqueryTable;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.schema.TableResolver;
import com.tesora.dve.sql.schema.TableResolver.MissingTableFunction;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryFunction;

public class ScopeEntry implements Scope {
		
	// table namespace
	private MultiMap<Name, TableInstance> tableNamespace;
	// derived column namespace: aliases declared in the projection
	private MultiMap<Name, ExpressionAlias> columnNamespace;
	// columns explicitly referenced in the projection at the top level
	// used to resolve ambiguity on order by, having, group by  which may reference columns from the projection
	private ProjectionMap projectionNamespace;
	// names referenced in the group bys
	private MultiMap<Name, ExpressionNode> groupByNamespace;
	
	private List<ExpressionNode> projection;
	
	private final int scopeID;
	
	// if in the context of building a table definition, record columns by name as they are created here.
	// we need them when we look up columns for keys, or for distribution vectors, etc.
	private SchemaLookup<PEColumn> columnLookup;
	private Table<?> tableInProcess;

	private ScopeParsePhase phase;
	private ListSet<NameInstance> unresolved;
	private ListSet<NameInstance> unresolvedChildren;
	
	private List<Scope> nested;
	private ListSet<ProjectingStatement> nestedQueries;
	private ListSet<VariableInstance> variables;

	private Map<Name, FunctionCall> functions;
	
	public ScopeEntry(boolean doresolution, int id) {
		scopeID = id;
		tableNamespace = new MultiMap<Name, TableInstance>();
		columnNamespace = new MultiMap<Name, ExpressionAlias>();
		projectionNamespace = new ProjectionMap();
		groupByNamespace = new MultiMap<Name, ExpressionNode>();
		columnLookup = new SchemaLookup<PEColumn>(null, false, false);
		nested = new ArrayList<Scope>();
		nestedQueries = new ListSet<ProjectingStatement>();
		variables = new ListSet<VariableInstance>();
		unresolved = new ListSet<NameInstance>();
		phase = (doresolution ? ScopeParsePhase.RESOLVING: ScopeParsePhase.UNRESOLVING);
		functions = new HashMap<Name, FunctionCall>();
	}

	public int getID() {
		return scopeID;
	}
	
	@Override
	public ScopeParsePhase getPhase() {
		return phase;
	}

	@Override
	public void setPhase(ScopeParsePhase spp) {
		phase = spp;
	}
	
	@Override
	public void storeProjection(List<ExpressionNode> proj) {
		projection = proj;
		phase = ScopeParsePhase.RESOLVING_CURRENT;
	}
	
	@Override
	public List<Scope> getNested() {
		return nested;
	}

	@Override
	public ListSet<ProjectingStatement> getNestedQueries() {
		return nestedQueries;
	}
	
	@Override
	public ListSet<VariableInstance> getVariables() {
		return variables;
	}
	
	// the errors we throw.
	private void objectAmbiguous(String what, Name origName) throws SchemaException {
		throw new SchemaException(Pass.SECOND, "Ambiguous " + what + " reference: " + origName.getSQL());
	}
	
	private void throwNonUniqueTableException(final Name ambiguousTableName) {
		throw new SchemaException(new ErrorInfo(DVEErrors.NON_UNIQUE_TABLE, ambiguousTableName.getUnquotedName().get()));
	}
	
	private static void tableNotFound(SchemaContext sc, Schema<?> schema, Name givenName) throws SchemaException {
		ErrorInfo ei = null;
		if (givenName.isQualified()) {
			QualifiedName qn = (QualifiedName) givenName;
			ei = new ErrorInfo(DVEErrors.TABLE_DNE,
					qn.getNamespace().getUnquotedName().get(),
					qn.getUnqualified().getUnquotedName().get());
		} else {
			UnqualifiedName db = schema.getSchemaName(sc);
			ei = new ErrorInfo(DVEErrors.TABLE_DNE,
					db.getUnquotedName().get(),
					givenName.getUnquotedName().get());
		}
		throw new SchemaException(ei);
	}
	
	private static void columnNotFound(Name columnName, LexicalLocation location) throws SchemaException {
		throw new SchemaException(new ErrorInfo(DVEErrors.COLUMN_DNE,
				columnName.getUnquotedName().get(),
				location.getExternal()));
	}
	
	public static final TableResolver resolver = new TableResolver().withMTChecks()
			.withMissingTableFunction(new MissingTableFunction() {

				@Override
				public void onMissingTable(SchemaContext sc, Schema<?> schema,
						Name name) {
					tableNotFound(sc,schema,name);
				}
				
			});
			
	
	// add a declared table to the scope, using any alias or the table name as the key.  if the key
	// is not unique, emit an error
	@Override
	public TableInstance buildTableInstance(Name inTableName, UnqualifiedName alias, Schema<?> inSchema, SchemaContext sc, LockInfo info) {
		TableInstance ti = null;
		if (sc.getCapability() == Capability.PARSING_ONLY) {
			ti = new TableInstance(null,inTableName,alias,false);
		} else {
			TableInstance raw = resolver.lookupShowTable(sc, inSchema, inTableName, info);
			ti = raw.adapt(inTableName.getUnqualified(), alias, (sc == null ? 0 : sc.getNextTable()),
					(sc != null && sc.getOptions().isResolve()));			
		}
		insertTable(ti,alias,inTableName.getUnqualified());
		return ti;
	}

	@Override
	public TableInstance buildTableInstance(Name inTableName, UnqualifiedName alias, SchemaContext sc, LockInfo info) {
		TableInstance raw = resolver.lookupTable(sc, inTableName, info);
		TableInstance ti = raw.adapt(inTableName.getUnqualified(), alias, (sc == null ? 0 : sc.getNextTable()),
				(sc != null && sc.getOptions().isResolve()));
		insertTable(ti,alias,inTableName.getUnqualified());
		return ti;
	}
		
	@Override
	public void pushVirtualTable(SubqueryTable sqt, UnqualifiedName alias, SchemaContext sc) {
		TableInstance ti = new TableInstance(sqt,alias,alias,sc.getNextTable(),false);
		insertTable(ti,alias,sqt.getName());
	}

	private void insertTable(TableInstance ti, Name alias, Name tableName) {
		if (alias != null) {
			if (tableNamespace.containsKey(alias)) 
				throwNonUniqueTableException(alias);
			tableNamespace.put(alias, ti);
		} else {
			if (tableNamespace.containsKey(tableName)) 
				throwNonUniqueTableException(tableName);
			tableNamespace.put(tableName, ti);
		}		
	}
	
	@Override
	public void insertTable(TableInstance ti) {
		if (ti.getAlias() != null) {
			if (tableNamespace.containsKey(ti.getAlias())) 
				throwNonUniqueTableException(ti.getAlias());
			tableNamespace.put(ti.getAlias(), ti);
		} else {
			if (tableNamespace.containsKey(ti.getTable().getName())) 
				throwNonUniqueTableException(ti.getTable().getName());
			tableNamespace.put(ti.getTable().getName(), ti);
		}
	}
	
	@Override
	public TableInstance lookupTableInstance(SchemaContext sc, Name given, boolean required) {
		if (!given.isQualified()) {
			Collection<TableInstance> sub = tableNamespace.get(given);
			if (sub == null || sub.isEmpty()) {
				if (required) {
					tableNotFound(sc,sc.getCurrentDatabase().getSchema(),given);
				} else {
					return null;
				}
			} else if (sub.size() > 1) {
				throwNonUniqueTableException(given);
			} else {
				return sub.iterator().next();
			}
		} else {
			throw new SchemaException(Pass.SECOND, "Internal error: qualified table name: " + given.getSQL());
		}
		return null;
	}

	private ColumnInstance buildColumnInstance(SchemaContext sc, Name given, Column<?> c, TableInstance ti) {
		if (sc != null && sc.getOptions().isRawPlanStep() && ti.getTable() instanceof TempTable)
			return new ColumnInstance(c,ti);
		else
			return new ColumnInstance(given,c,ti);
	}
	
	@Override
	public ExpressionNode buildColumnInstance(SchemaContext sc, Name igiven) {
		Name given = igiven;
		// still building the projection - delay resolution until later
		if (phase == ScopeParsePhase.UNRESOLVING) {
			NameInstance ni = new NameInstance(given,null);
			unresolved.add(ni);
			return ni;
		}
		if (given.isQualified()) {
			QualifiedName qn = (QualifiedName)given;
			UnqualifiedName tableName = qn.getNamespace();
			UnqualifiedName columnName = given.getUnqualified();
			TableInstance ti = lookupTableInstance(sc, tableName, false);
			if (ti == null) 
				columnNotFound(given,phase.getLocation());
			@SuppressWarnings("null")
			Column<?> c = ti.getTable().lookup(sc,given.getUnqualified());
			if (columnName.isAsterisk()) {
				return new WildcardTable(tableName, ti);
			}
			if (c == null) 
				columnNotFound(given,phase.getLocation());
			return buildColumnInstance(sc,given,c,ti);
		}
		// if we're in a restricted namespace, try to build by derived first, then try by table
		ExpressionNode any = null;
		// the order we search depends on the phase
		switch (phase) {
		case RESOLVING_CURRENT:
		case RESOLVING:
			// from and where clauses, can only build by table
			any = buildColumnRefByTable(sc, given);
			break;
		case GROUPBY:
			// start from table, then look at column ref by projection, finally column ref by derived
			// once found, put into the group by namespace
			any = buildColumnRefByDerived(given);
			if (any == null)
				any = buildColumnRefByProjection(given);
			if (any == null)
				any = buildColumnRefByTable(sc, given);
			if (any != null)
				groupByNamespace.put(given, any);
			break;
		case HAVING:
			// start from group by namespace, column ref by projection, column ref by derived
			// if we have an expression, we can't use the group by column
			any = buildColumnRefByTable(sc, given);
			if (any == null)
				any = buildColumnRefByProjection(given);
			if (any == null)
				any = buildColumnRefByDerived(given/*, false*/);
			if (any == null)
				any = buildColumnRefByGroupBy(given);
			break;
		case TRAILING:
			// start from column ref by derived, then column ref by projection
			any = buildColumnRefByDerived(given);
			if (any == null)
				any = buildColumnRefByProjection(given);
			if (any == null)
				any = buildColumnRefByTable(sc, given);
			break;
		default:
			throw new IllegalArgumentException("Invalid scope parse phase: " + phase);
		}
		if (any == null)
			columnNotFound(given,phase.getLocation());
		return any;
	}

	@Override
	public void resolveProjection(SchemaContext sc) {

		if (unresolved != null && unresolved.size() > 0 && tableNamespace.keySet().size() == 0) {
			// will not resolve so try later
			return;
		}
		
		phase = ScopeParsePhase.RESOLVING;
		// look for any top level name instances
		HashMap<NameInstance,Integer> offsets = new HashMap<NameInstance,Integer>();
		for(int i = 0; i < projection.size(); i++) {
			ExpressionNode en = projection.get(i);
			if (en instanceof NameInstance) {
				offsets.put((NameInstance)en,i);
			}
		}
		for(NameInstance ni : unresolved) {
			Name embedded = ni.getName();
			ExpressionNode ci = buildColumnInstance(sc, embedded);
			if (ni.isNegated()) {
				ci.setNegated();
			}
			if (ni.getParent() == null) {
				Integer index = offsets.get(ni);
				if (index != null) {
					projection.remove(index.intValue());
					projection.add(index.intValue(), ci);

					if (ci instanceof ColumnInstance) {
						ColumnInstance nci = (ColumnInstance) ci;
						projectionNamespace.add(nci);
					} else if (ci instanceof WildcardTable) {
						WildcardTable wct = (WildcardTable) ci;
						for(Column<?> c : wct.getTableInstance().getTable().getColumns(sc)) {
							projectionNamespace.add(new ColumnInstance(c,wct.getTableInstance()));
						}
					}
				} else {
					index = null;
					for(int i = 0; i < getUnresolvedChildren().size(); i++) {
						NameInstance urcni = getUnresolvedChildren().get(i);
						if (StringUtils.equals(urcni.getName().get(), ni.getName().get())) {
							index = i;
							urcni.getParentEdge().set(ci);
							getUnresolvedChildren().remove(index.intValue());
							break;
						}
					}
				}
				if (index == null)
					throw new SchemaException(Pass.SECOND, "Lost unresolved column: " + embedded.getSQL());
			} else {
				ni.getParentEdge().set(ci);
			}
		}
		unresolved.clear();
	}
	
	// find a column ref by looking at ref'd tables first
	private ExpressionNode buildColumnRefByTable(SchemaContext sc, Name given) {
		// look up among the table namespaces
		// we might have two unaliased tables with the same name - so keep looking after we find our first candidate
		Pair<TableInstance,Column<?>> candidate = null;
		for(TableInstance ti : tableNamespace.values()) {
			Column<?> c = ti.getTable().lookup(sc,given);
			if (c != null) {
				if (candidate != null) {
					final LanguageNode parent = ti.getParent();
					/*
					 * MySQL now treats the common columns of NATURAL or USING
					 * joins as a single column, so when a query refers to such
					 * columns, the query compiler does not consider them as
					 * ambiguous.
					 */
					if ((parent instanceof JoinedTable) && ((JoinedTable) parent).getJoinType().isNaturalJoin()) {
						continue;
					}
					objectAmbiguous("Column", given);
				} else {
					candidate = new Pair<TableInstance, Column<?>>(ti, c);
				}
			}
		}
		if (candidate == null) return null;
		// build the reference
		return buildColumnInstance(sc,given,candidate.getSecond(),candidate.getFirst());
	}

	private ExpressionNode buildColumnRefByDerived(Name given) {
		Collection<ExpressionAlias> sub = columnNamespace.get(given);
		if (sub == null || sub.isEmpty())
			return null;
		if (sub.size() > 1)
			objectAmbiguous("Derived column",given);
		
		ExpressionAlias targ = sub.iterator().next();
		return new AliasInstance(targ, given.getOrig());
	}

	private ExpressionNode buildColumnRefByProjection(Name given) {
		Collection<ColumnInstance> sub = projectionNamespace.lookup(given);
		if (sub == null || sub.isEmpty())
			return null;
		if (sub.size() > 1)
			objectAmbiguous("Referenced column", given);
		ColumnInstance ci = sub.iterator().next();
		return (ExpressionNode) ci.copy(null);
	}
	
	private ExpressionNode buildColumnRefByGroupBy(Name given) {
		Collection<ExpressionNode> sub = groupByNamespace.get(given);
		if (sub == null || sub.isEmpty())
			return null;
		if (sub.size() > 1)
			objectAmbiguous("Referenced column", given);
		ExpressionNode en = sub.iterator().next();
		return (ExpressionNode)en.copy(null);
	}
	
	@Override
	public void insertColumn(UnqualifiedName alias, ExpressionAlias e) {
		columnNamespace.put(alias, e);
	}
	
	@Override
	public ExpressionNode buildExpressionAlias(ExpressionNode e, Alias alias, SourceLocation sloc) {
		ExpressionAlias ea = new ExpressionAlias(e, alias,sloc);
		if (alias.isName()) {
			columnNamespace.put(alias.getNameAlias(), ea);
		}
		return ea;
	}
		
	@Override
	public Set<String> getAllAliases() {
		HashSet<String> ret = new HashSet<String>();
		ret.addAll(Functional.apply(tableNamespace.keySet(), buildUnquoted));
		ret.addAll(Functional.apply(columnNamespace.keySet(), buildUnquoted));
		return ret;
	}

	@Override
	public ListSet<TableKey> getLocalTables() {
		ListSet<TableKey> tabs = new ListSet<TableKey>();
		for(TableInstance ti : tableNamespace.values()) {
			if (ti.getTable() instanceof PETable) {
				PETable pet = (PETable) ti.getTable();
				if (pet.isVirtualTable()) continue;
			}
			tabs.add(ti.getTableKey());
		}
		return tabs;
	}

	@Override
	public ListSet<TableKey> getAllVisibleTables() {
		ListSet<TableKey> out = new ListSet<TableKey>();
		out.addAll(getLocalTables());
		for(Scope c : nested)
			out.addAll(c.getAllVisibleTables());
		return out;
	}
	
	@Override
	public ListSet<FunctionCall> getFunctions() {
		ListSet<FunctionCall> funcs = new ListSet<FunctionCall>(functions.values());
		return funcs;
	}
	
	@Override
	public void registerFunction(FunctionCall fc) {
		functions.put(fc.getFunctionName().getUnquotedName(), fc);
	}
	
	@Override
	public PEColumn registerColumn(PEColumn c) {
		columnLookup.add(c);
		return c;
	}
	
	@Override
	public void registerAlterColumns(SchemaContext sc, PETable tab) {
		tableInProcess = tab;
		for(PEColumn c : tab.getColumns(sc)) {
			registerColumn(c);
		}
	}
	
	@Override
	public PEColumn lookupInProcessColumn(Name n) {
		return columnLookup.lookup(n);
	}
	
	@Override
	public Table<?> getAlteredTable() {
		return tableInProcess;
	}
	
	public ListSet<NameInstance> getUnresolved() {
		return unresolved;
	}

	@Override
	public ListSet<NameInstance> getUnresolvedChildren() {
		if (unresolvedChildren == null) {
			unresolvedChildren = new ListSet<NameInstance>();
		}
		return unresolvedChildren;
	}

	private static final UnaryFunction<String, Name> buildUnquoted = new UnaryFunction<String, Name>() {

		@Override
		public String evaluate(Name object) {
			return object.get();
		}
		
	};

	private static class ProjectionMap extends MultiMapLookup<ColumnInstance> {

		public ProjectionMap() {
			super(false, false, new UnaryFunction<Name[], ColumnInstance>() {

				@Override
				public Name[] evaluate(ColumnInstance object) {
					return new Name[] { object.getColumn().getName() };
				}
				
			});
		}
		
		public void add(ColumnInstance ci) {
			add(Collections.singleton(ci));
		}
	}
	
}
