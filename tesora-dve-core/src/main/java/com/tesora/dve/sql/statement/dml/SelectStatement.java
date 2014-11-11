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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang.ObjectUtils;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.db.Emitter;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.db.mysql.MysqlEmitter;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnAttribute;
import com.tesora.dve.resultset.ColumnInfo;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.MTTableKey;
import com.tesora.dve.sql.expression.RewriteKey;
import com.tesora.dve.sql.expression.SetQuantifier;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.node.SingleEdge;
import com.tesora.dve.sql.node.expression.Alias;
import com.tesora.dve.sql.node.expression.AliasInstance;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.NameAlias;
import com.tesora.dve.sql.node.expression.StringLiteralAlias;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.Wildcard;
import com.tesora.dve.sql.node.expression.WildcardTable;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.structural.LimitSpecification;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.parser.SourceLocation;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.ExplainOptions.ExplainOption;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.mt.PETenant;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;
import com.tesora.dve.sql.transform.execution.RootExecutionPlan;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.transform.execution.ProjectingExecutionStep;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.ListSetMap;

public class SelectStatement extends ProjectingStatement {

	private MultiEdge<SelectStatement, ExpressionNode> projection =
		new MultiEdge<SelectStatement, ExpressionNode>(SelectStatement.class, this, EdgeName.PROJECTION);
	private MultiEdge<SelectStatement, SortingSpecification> groupBys =
		new MultiEdge<SelectStatement, SortingSpecification>(SelectStatement.class, this, EdgeName.GROUPBY);
	private SingleEdge<SelectStatement, ExpressionNode> havingClause =
		new SingleEdge<SelectStatement, ExpressionNode>(SelectStatement.class, this, EdgeName.HAVING);
	
	private SetQuantifier setQuantifier = null;
	private List<MysqlSelectOption> options = null;
	private Boolean locking = null;
	// not set until we normalize
	private ProjectionInfo projectionMetadata = null;

	@SuppressWarnings("rawtypes")
	private List edges = Arrays.asList(new Edge[] { tableReferences, projection, whereClause, groupBys, orderBys, limitExpression, havingClause });

	private static UnqualifiedName getColumnName(final ColumnKey ck) {
		return ck.getColumn().getName().getUnqualified();
	}
		
	public SelectStatement(AliasInformation ai) {
		super(null);
		setAliases(ai);
	}
	
	public SelectStatement(List<FromTableReference> tables, 
			List<ExpressionNode> projExprs,
			ExpressionNode where, 
			List<SortingSpecification> order, 
			LimitSpecification limit,
			SetQuantifier setQuant,
			List<MysqlSelectOption> options,
			List<SortingSpecification> grouping,
			ExpressionNode havingExpr,
			Boolean locking,
			AliasInformation ai,
			SourceLocation loc) {
		super(loc);
		setAliases(ai);
		setTables(tables);
		setProjection(projExprs);
		setWhereClause(where);
		setOrderBy(order);
		setLimit(limit);
		setGroupBy(grouping);
		setHavingExpression(havingExpr);
		this.setQuantifier = setQuant;
		setSelectOptions(options);
		this.locking = locking;
	}

	@Override
	public SelectStatement setTables(List<FromTableReference> oth) { tableReferences.set(oth); return this; }
	public SelectStatement setTables(TableInstance ti) {
		ArrayList<FromTableReference> ftrs = new ArrayList<FromTableReference>();
		ftrs.add(new FromTableReference(ti));
		return setTables(ftrs);
	}
	
	public List<ExpressionNode> getProjection() { return projection.getMulti(); }
	public MultiEdge<SelectStatement, ExpressionNode> getProjectionEdge() { return projection; }
	public SelectStatement setProjection(List<ExpressionNode> p) {
		// so, we're going to remove any existing aliases used in the projection and add new ones
		AliasInformation info = getAliases();
		for(ExpressionNode en : projection.getMulti()) {
			if (en instanceof ExpressionAlias) {
				ExpressionAlias ea = (ExpressionAlias) en;
				info.removeAlias(ea.getAlias().get());
			}
		}
		for(ExpressionNode en : p) {
			if (en instanceof ExpressionAlias) {
				ExpressionAlias ea = (ExpressionAlias)en;
				info.addAlias(ea.getAlias().get());
			}
		}
		projection.set(p);
		return this;
	}
	
	@Override
	public List<List<ExpressionNode>> getProjections() {
		return Collections.singletonList(getProjection());
	}
	
	@Override
	public ExpressionNode getWhereClause() { return whereClause.get(); }
	@Override
	public SelectStatement setWhereClause(ExpressionNode ex) { whereClause.set(ex); return this; }
	
	public SetQuantifier getSetQuantifier() { return this.setQuantifier; }
	public SelectStatement setSetQuantifier(SetQuantifier sq) { this.setQuantifier = sq; return this; }
	
	public List<MysqlSelectOption> getSelectOptions() { return this.options; }
	@SuppressWarnings("unchecked")
	public void setSelectOptions(List<MysqlSelectOption> in) { 
		this.options = (in == null ? Collections.EMPTY_LIST : new ArrayList<MysqlSelectOption>(in)); 
	}
	
	public List<SortingSpecification> getGroupBys() { return groupBys.getMulti(); }
	public MultiEdge<SelectStatement, SortingSpecification> getGroupBysEdge() { return groupBys; }
	public SelectStatement setGroupBy(List<SortingSpecification> order) { 
		groupBys.set(order);
		SortingSpecification.setOrdering(order, Boolean.FALSE);
		return this;
	}
	
	public ExpressionNode getHavingExpression() { return havingClause.get(); }
	public SelectStatement setHavingExpression(ExpressionNode e) { havingClause.set(e); return this;}
	
	public Edge<SelectStatement,ExpressionNode> getHavingEdge() {
		return havingClause;
	}
	
	public boolean isLocking() {
		return (locking == null ? false : locking.booleanValue());
	}
	
	public SelectStatement setLocking() {
		locking = true;
		return this;
	}

	@Override
	public ProjectionInfo getProjectionMetadata(SchemaContext sc) {
		// we can't actually normalize yet - produces the wrong results for prepare
		if (projectionMetadata == null) {
			List<ExpressionNode> proj = getProjection();
			proj = expandWildcards(sc, proj);
			projectionMetadata = buildProjectionMetadata(sc,proj);
			setProjection(proj);
		}
		return projectionMetadata;
	}
	
	@Override
	public List<ExpressionNode> normalize(SchemaContext pc, List<ExpressionNode> in) {
		return expandWildcards(pc, in);
	}
	
	@Override
	public void normalize(SchemaContext pc) {
		// we've already resolved everything.  now we:
		// [1] expand all wildcards and table wildcards (except for count(*))
		//     this is easy for the projection - walk down the list and look for naked wildcards or
		//     table wildcards
		List<ExpressionNode> proj = getProjection();
		proj = expandWildcards(pc, proj);
		if (projectionMetadata == null || projectionMetadata.getWidth() != proj.size())
			projectionMetadata = buildProjectionMetadata(pc,proj);
		// [2] assign aliases for everything that does not already use an alias
		//     for the projection, this is easy - just walk down the list and anything that is
		//     not a DerivedColumn must need an alias
		proj = assignProjectionAliases(pc, proj, getAliases());
		setProjection(proj);
		// for order by, group by, etc. now that the projection aliases are set, use them in any clauses
		normalizeSorts(pc);
	}

	// methods for normalization
	// for either a projection or an insert column specificaiton
	private List<ExpressionNode> expandWildcards(SchemaContext sc, List<ExpressionNode> in) {
		ArrayList<ExpressionNode> np = new ArrayList<ExpressionNode>();
		
		for(ExpressionNode e : in) {
			if (e instanceof WildcardTable) {
				WildcardTable wct = (WildcardTable)e;
				// something like select m.* from Foo as m - expand out to all columns using the m. prefix
				addColumns(sc, np, wct.getTableInstance());
			} else if (e instanceof Wildcard) {
				// something like select * from foo
				// could also be select * from foo inner join bar - in that case it's all the columns
				// the ordering is first by base, then by joined to, so base1, joined to base1, base2, .. etc.

				// Store the joined tables in a map together with their USING()
				// clause columns if any.
				final ListSetMap<TableInstance, Set<ColumnKey>> tablesAndJoinColumns = new ListSetMap<TableInstance, Set<ColumnKey>>();
				for (final FromTableReference ftr : tableReferences.getMulti()) {
					final TableInstance lhs = ftr.getBaseTable();
					if (lhs != null) {
						tablesAndJoinColumns.put(lhs, Collections.<ColumnKey> emptySet());
					}

					for (final JoinedTable jt : ftr.getTableJoins()) {
						final TableInstance rhs = jt.getJoinedToTable();

						if (jt.hasUsingSpec()) {
							final ListSet<ExpressionNode> joinConditions = ExpressionUtils.decomposeAndClause(jt.getJoinOn());
							if ((lhs != null) && tablesAndJoinColumns.get(lhs).isEmpty()) {
								final Set<ColumnKey> lhsColumns = collectColumnsFromJoinConditions(joinConditions, false);
								tablesAndJoinColumns.put(lhs, lhsColumns);
							}

							final Set<ColumnKey> rhsColumns = collectColumnsFromJoinConditions(joinConditions, true);
							if (jt.getJoinType().isRightOuterJoin()) {
								tablesAndJoinColumns.put(0, rhs, rhsColumns);
							} else {
								tablesAndJoinColumns.put(rhs, rhsColumns);
							}
						} else {

							/*
							 * Columns appear in the order in which
							 * they occur in their parent tables ordered from
							 * left to right.
							 */
							tablesAndJoinColumns.put(rhs, Collections.<ColumnKey> emptySet());
						}
					}
				}

				np.addAll(buildProjection(sc, tablesAndJoinColumns));
			} else {
				np.add(e);
			}
		}		
		return np;
	}
	
	/**
	 * @param useRhs Collect RHS columns if TRUE, and LHS columns otherwise.
	 */
	private Set<ColumnKey> collectColumnsFromJoinConditions(final ListSet<ExpressionNode> joinConditions, final boolean useRhs) {
		final int sideIndex = (useRhs) ? 1 : 0;
		final Set<ColumnKey> columnKeys = new TreeSet<ColumnKey>(new Comparator<ColumnKey>() {
			@Override
			public int compare(ColumnKey c1, ColumnKey c2) {
				final int p1 = c1.getPEColumn().getPosition();
				final int p2 = c2.getPEColumn().getPosition();
				return p1 - p2;
			}
		});

		for (final ExpressionNode condition : joinConditions) {
			final FunctionCall columnEquality = (FunctionCall) condition;
			final ColumnInstance ci = (ColumnInstance) columnEquality.getParameters().get(sideIndex);
			columnKeys.add(ci.getColumnKey());
		}
		
		return columnKeys;
	}
	
	/**
	 * The redundant column is eliminated and the column
	 * order is correct according to standard SQL:
	 * 
	 * First, coalesced common columns of the two joined
	 * tables,
	 * in the order in which they occur in the first table.
	 * 
	 * Second, columns unique to the first table, in order
	 * in which they occur in that table.
	 * 
	 * Third, columns unique to the second table, in order
	 * in which they occur in that table.
	 */
	private Collection<ExpressionNode> buildProjection(final SchemaContext sc, final Map<TableInstance, Set<ColumnKey>> tablesAndJoinColumns) {
		final ListSetMap<ColumnKey, ExpressionNode> projection = new ListSetMap<ColumnKey, ExpressionNode>();

		/*
		 * We need to coalesce the join columns by their unqualified names.
		 * Build a lookup map.
		 */
		final Map<ColumnKey, Set<ColumnInstance>> toCoalesceName = new TreeMap<ColumnKey, Set<ColumnInstance>>(new Comparator<ColumnKey>() {
			@Override
			public int compare(ColumnKey c1, ColumnKey c2) {
				final String n1 = getColumnName(c1).get();
				final String n2 = getColumnName(c2).get();
				return n1.compareTo(n2);
			}
		});

		/*
		 * Here we the projection. Join columns are inserted in order in which
		 * they occur in the first table and coalesced by name in set
		 * containers.
		 */
		for (final Map.Entry<TableInstance, Set<ColumnKey>> te : tablesAndJoinColumns.entrySet()) {
			final Set<ColumnKey> entryJoinColumns = te.getValue();
			for (final ColumnKey ck : entryJoinColumns) {
				final ColumnInstance ci = ck.toInstance();
				if (!toCoalesceName.containsKey(ck)) {
					projection.put(ck, ci);
					
					final Set<ColumnInstance> coalescedColumns = new HashSet<ColumnInstance>();
					coalescedColumns.add(ci);
					toCoalesceName.put(ck, coalescedColumns);
				} else {
					toCoalesceName.get(ck).add(ci);
				}
				
			}
			addColumns(sc, projection, te.getKey(), entryJoinColumns);
		}
		
		/*
		 * We already have the right projection.
		 */
		if (toCoalesceName.isEmpty()) {
			return projection.values();
		}

		/*
		 * It turns out that in case of multi-join statements MySQL also sorts
		 * adjacent join columns by the number of coalesced columns (by the
		 * frequency with which they appear in the USING clauses).
		 * 
		 * i.e.
		 * "SELECT * FROM pe251C c RIGHT JOIN pe251D d USING (id1,id2,id3) LEFT OUTER JOIN pe251E e USING (id1,id3) ORDER BY c.id1;"
		 * 
		 * Although the first-table order would be (id2, id1, id3):
		 * "COALESCE(id2, id2), COALESCE(id1, id1, id1), COALESCE(id3, id3, id3)"
		 * 
		 * MySQL sorts the columns as (id1, id3, id2):
		 * "COALESCE(id1, id1, id1), COALESCE(id3, id3, id3), COALESCE(id2, id2)"
		 * 
		 * Sort the adjacent coalesced column sets by their size using a stable
		 * algorithm.
		 */
		final Map<ColumnKey, Set<ColumnInstance>> toCoalesceByEntry = new HashMap<ColumnKey, Set<ColumnInstance>>(toCoalesceName);
		final int endIndex = projection.size();
		for (int i = 0; i < endIndex; ++i) {
			final ColumnKey ik = projection.getEntryAt(i).getKey();
			if (toCoalesceByEntry.containsKey(ik)) {
				int j = i;
				while (j > 0) {
					final ColumnKey jk1 = projection.getEntryAt(j).getKey();
					final ColumnKey jk2 = projection.getEntryAt(j - 1).getKey();
					if (toCoalesceByEntry.containsKey(jk1) && toCoalesceByEntry.containsKey(jk2)) {
						final Set<ColumnInstance> jv1 = toCoalesceByEntry.get(jk1);
						final Set<ColumnInstance> jv2 = toCoalesceByEntry.get(jk2);

						if (jv1.size() > jv2.size()) {
							projection.swap(jk1, jk2);
						} else {
							break;
						}
					} else {
						break;
					}
					--j;
				}
			}
		}

		/*
		 * Finally, convert the coalesced column sets into COALESCE() function
		 * calls.
		 */
		for (final Map.Entry<ColumnKey, Set<ColumnInstance>> coalesceEntry : toCoalesceName.entrySet()) {
			final ColumnKey key = coalesceEntry.getKey();
			final FunctionCall coalesce = new FunctionCall(FunctionName.makeCoalesce(), new ArrayList<ExpressionNode>(coalesceEntry.getValue()));
			final ExpressionAlias coalesceAlias = new ExpressionAlias(coalesce, new NameAlias(getColumnName(key)), false);
			projection.put(key, coalesceAlias);
		}
		
		return projection.values();
	}

	private List<ExpressionNode> assignProjectionAliases(SchemaContext pc, List<ExpressionNode> in, AliasInformation ai) {
		ArrayList<ExpressionNode> np = new ArrayList<ExpressionNode>();
		for(ExpressionNode e : in) {
			if (e instanceof ExpressionAlias) {
				// already has an alias
				ExpressionAlias ea = (ExpressionAlias) e;
				if (ea.isSynthetic()) {
					// check for dup aliases
					UnqualifiedName unq = ea.getAlias().getNameAlias();
					if (ai.isDuplicateAlias(unq)) {
						NameAlias prefix = ea.getTarget().buildAlias(pc);
						ea.setAlias(ai.buildNewAlias(prefix.getNameAlias()));
					}
				} else if (ea.getAlias() instanceof StringLiteralAlias) {
                    //mysql 5.5 docs state it is OK specify as a column alias as either a identifier or string quoted literal in the projection
                    //so here we convert the StringLiteralAlias 'foo' into NameAlias `foo`
					final StringLiteralAlias stringLit = (StringLiteralAlias) ea.getAlias();
					if (!stringLit.get().isEmpty()) {
						final UnqualifiedName unq = new UnqualifiedName(stringLit.get(), true);
						ea.setAlias(unq);
					} else {
						ea.setAlias(ai.buildNewAlias(null));
                    }
				}
				np.add(e);
			} else {
				// see if we can build something reasonable
				NameAlias prefix = e.buildAlias(pc);
				np.add(new ExpressionAlias(e, new NameAlias(ai.buildNewAlias(prefix.getNameAlias())), true));
			}
		}
		return np;
	}

	private void normalizeSorts(SchemaContext pc) {
		if (!EngineConstant.ORDERBY.has(this) && !EngineConstant.GROUPBY.has(this))
			return;
		Map<RewriteKey,ExpressionNode> projMap = ExpressionUtils.buildRewriteMap(getProjection());
		if (EngineConstant.ORDERBY.has(this))
			normalizeSorts(pc,projMap,EngineConstant.ORDERBY.getEdge(this));
		if (EngineConstant.GROUPBY.has(this))
			normalizeSorts(pc,projMap,EngineConstant.GROUPBY.getEdge(this));
	}
	
	private void normalizeSorts(SchemaContext pc, Map<RewriteKey, ExpressionNode> projMap, Edge<?,?> in) {
		@SuppressWarnings("unchecked")
		MultiEdge<?,SortingSpecification> min = (MultiEdge<?, SortingSpecification>) in;
		for(SortingSpecification ss : min.getMulti()) {
			ExpressionNode target = ss.getTarget();
			if (target instanceof AliasInstance)
				continue;
			else if (target instanceof LiteralExpression) {
				// i.e. order by 1,2,3
				LiteralExpression le = (LiteralExpression) target;
				Object value = le.getValue(pc.getValues());
				if (value instanceof Long) {
					Long index = (Long) value;
					if ((index.intValue() - 1) < projection.size()) {
						target = ExpressionUtils.getTarget(projection.get(index.intValue() - 1));
					}
				}
			}
			ExpressionNode inProjection = projMap.get(target.getRewriteKey());
			if (inProjection != null) {
				ExpressionAlias ea = (ExpressionAlias)inProjection.getParent();
				AliasInstance ai = ea.buildAliasInstance();
				ss.getTargetEdge().set(ai);
			}
		}
	}
	
	@Override
	public ExecutionStep buildSingleKeyStep(SchemaContext pc, TableKey tk, DistributionKey kv, DMLStatement sql) throws PEException {
		return ProjectingExecutionStep.build(pc,getDatabase(pc), getStorageGroup(pc), tk.getAbstractTable().getDistributionVector(pc), kv, sql, distKeyExplain);
	}

	@Override
	public DistKeyOpType getKeyOpType() {
		return isLocking() ? DistKeyOpType.SELECT_FOR_UPDATE : DistKeyOpType.QUERY;
	}

	@Override
	public ExecutionType getExecutionType() {
		return ExecutionType.SELECT;
	}

	private ProjectionInfo buildProjectionMetadata(SchemaContext pc, List<ExpressionNode> proj) {
        Emitter emitter = new MysqlEmitter(); // called during info schema initialization
		try {
			emitter.setOptions(EmitOptions.RESULTSETMETADATA);
			emitter.pushContext(pc.getTokens());
			ProjectionInfo pi = new ProjectionInfo(proj.size());
			for(int i = 0; i < proj.size(); i++) {
				ExpressionNode e = proj.get(i);
				String columnName = null;
				String aliasName = null;
				ColumnInstance ci = null;
				
				if (e.getSourceLocation() != null && e.getSourceLocation().isComputed()) {
					aliasName = e.getSourceLocation().getText();
				}
				
				if (e instanceof ExpressionAlias) {
					ExpressionAlias ea = (ExpressionAlias) e;
					Alias aname = ea.getAlias();
					if (aname != null)
						aliasName = PEStringUtils.dequote(aname.getSQL());
					ExpressionNode cname = ea.getTarget();
					StringBuilder buf = new StringBuilder();
					emitter.emitExpression(pc,pc.getValues(),cname, buf);
					columnName = buf.toString();
					if (cname instanceof ColumnInstance) {
						ci = (ColumnInstance) cname;
					} else {
						aliasName = PEStringUtils.dequote(aliasName); 
						columnName = aliasName; 
					}
				} else if (e instanceof ColumnInstance) {
					ci = (ColumnInstance) e;
					StringBuilder buf = new StringBuilder();
					emitter.emitExpression(pc, pc.getValues(),e, buf);
					aliasName = PEStringUtils.dequote(buf.toString());
					// always use the column name
					columnName = ci.getColumn().getName().getUnquotedName().get();
				} else {
					if (aliasName != null) {
						// via above
						columnName = aliasName;
					} else {
						StringBuilder buf = new StringBuilder(); 
						emitter.emitExpression(pc,pc.getValues(),e, buf); 
						columnName = (e instanceof LiteralExpression) ? PEStringUtils.dequote(buf.toString()) : buf.toString(); 
						aliasName = columnName;
					}
				}
				ColumnInfo colInfo = pi.addColumn(i + 1, columnName, (aliasName == null ?  columnName : aliasName));
				if (ci != null) {
					String tblName = null;
					String dbName = null;
					Column<?> backingColumn = ci.getColumn();
					TableKey tk = ci.getTableInstance().getTableKey();
					if (tk instanceof MTTableKey) {
						MTTableKey mtk = (MTTableKey) tk;
						tblName = mtk.getScope().getName().getUnqualified().getUnquotedName().get();
						PETenant tenant = mtk.getScope().getTenant(pc);
						PEDatabase pdb = tenant.getDatabase(pc);
						if (pdb.getMTMode() == MultitenantMode.ADAPTIVE)
							dbName = tenant.getName().getUnqualified().getUnquotedName().get();
						else
							dbName = pdb.getName().getUnqualified().getUnquotedName().get();
					} else {
						Table<?> tab = tk.getTable();
						if (tab.isInfoSchema()) {
							dbName = PEConstants.INFORMATION_SCHEMA_DBNAME;
						} else {
							Database<?> tabDb = tab.getDatabase(pc);
							if (tab.isTempTable() && (tabDb == null)) {
								tabDb = pc.getCurrentDatabase(false);
								if (tabDb == null) {
									tabDb = pc.getAnyNonSchemaDatabase();
								}
								final TempTable tabAstempTable = ((TempTable) tab);
								tabAstempTable.setDatabase(pc, (PEDatabase) tabDb, true);
								tabAstempTable.refreshColumnLookupTable();
							}

							if (tabDb != null) {
								dbName = tabDb.getName().getUnqualified().getUnquotedName().get();
							}
						}
						tblName = tab.getName(pc,pc.getValues()).getUnqualified().getUnquotedName().get();
					}
					if (tblName != null)
						colInfo.setDatabaseAndTable(dbName, tblName);
					if (backingColumn instanceof PEColumn) {
						// set flags
						PEColumn pec = (PEColumn) backingColumn;
						if (!pec.isNotNullable() || pec.isNullable())
							colInfo.setAttribute(ColumnAttribute.NULLABLE);
						if (pec.isAutoIncrement())
							colInfo.setAttribute(ColumnAttribute.AUTO_INCREMENT);
						if (pec.isKeyPart()) {
							colInfo.setAttribute(ColumnAttribute.KEY_PART);
							if (pec.isPrimaryKeyPart())
								colInfo.setAttribute(ColumnAttribute.PRIMARY_KEY_PART);
							if (pec.isUniquePart())
								colInfo.setAttribute(ColumnAttribute.UNIQUE_PART);
						}
					}
					
				}
			}
			return pi;
		} finally {
			emitter.popContext();
		}		
	}
	
	@Override
	public List<TableInstance> getBaseTables() {
		return computeBaseTables(tableReferences.getMulti());
	}	
	
	@SuppressWarnings("unchecked")
	@Override
	public <T extends Edge<?,?>> List<T> getEdges() {
		return edges;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public <T extends Edge<?,?>> List<T> getNaturalOrderEdges() {
		ArrayList out = new ArrayList();
		out.add(projection);
		out.add(tableReferences);
		out.add(whereClause);
		out.add(havingClause);
		out.add(groupBys);
		out.add(orderBys);
		out.add(limitExpression);
		return out;
	}

	@Override
	protected ExecutionPlan buildExplain(SchemaContext sc, BehaviorConfiguration config) throws PEException {
		boolean noplan = 
				explain.hasSetting(ExplainOption.NOPLAN);		
		if (noplan) {
			normalize(sc);
			ProjectingExecutionStep ses = ProjectingExecutionStep.build(sc,getDatabase(sc), getStorageGroups(sc).get(0), 
					EngineConstant.BROADEST_DISTRIBUTION_VECTOR.getValue(this,sc), null,
					this, DMLExplainReason.EXPLAIN_NOPLAN.makeRecord());
			ExecutionPlan expep = new RootExecutionPlan(null, sc.getValueManager(), StatementType.EXPLAIN); 
			expep.getSequence().append(ses);
			return expep;
		}
		// we would check for alternate configuration here, but not quite yet
		return super.buildExplain(sc, config);
	}

	@Override
	public StatementType getStatementType() {
		return StatementType.SELECT;
	}

	@Override
	protected SelectStatement findLeftmostSelect() {
		return this;
	}	

	public void removeTables(List<FromTableReference> toBeDeleted) {
		for(Iterator<FromTableReference> iter = getTablesEdge().iterator(); iter.hasNext();) {
			if (toBeDeleted.contains(iter.next())) {
				iter.remove();
			}
		}
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		SelectStatement ss = (SelectStatement) other;
		return ObjectUtils.equals(ss.setQuantifier, setQuantifier) &&
				ObjectUtils.equals(ss.locking, locking);
	}

	@Override
	protected int selfHashCode() {
		int result = 1;
		result = addSchemaHash(result, (setQuantifier == null ? 0 : setQuantifier.hashCode()));
		return addSchemaHash(result,(locking == null ? false : locking.booleanValue()));
	}	
}
