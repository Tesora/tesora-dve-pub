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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.common.catalog.TableVisibility;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.errmap.AvailableErrors;
import com.tesora.dve.errmap.ErrorInfo;
import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.MTTableKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.MTTableInstance;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.ForeignKeyAction;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.LockInfo;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PESchema;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaPolicyContext;
import com.tesora.dve.sql.schema.StructuralUtils;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.statement.EmptyStatement;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.PEAlterTableStatement;
import com.tesora.dve.sql.statement.ddl.PEAlterTenantTableStatement;
import com.tesora.dve.sql.statement.ddl.PECreateTableAsSelectStatement;
import com.tesora.dve.sql.statement.ddl.PECreateTableStatement;
import com.tesora.dve.sql.statement.ddl.PECreateTenantTableStatement;
import com.tesora.dve.sql.statement.ddl.PEDropTableStatement;
import com.tesora.dve.sql.statement.ddl.PEDropTenantTableStatement;
import com.tesora.dve.sql.statement.ddl.alter.AddIndexAction;
import com.tesora.dve.sql.statement.ddl.alter.AlterTableAction;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement.ValueHandler;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.util.ListSet;

public class AdaptiveMultitenantSchemaPolicyContext extends SchemaPolicyContext {

	public static AdaptiveMultitenantSchemaPolicyContext build(SchemaContext sc, PEDatabase db) {
		MultitenantMode mm = db.getMTMode();
		if (mm == null)
			throw new SchemaException(Pass.FIRST, "Missing multitenant mode");
		else if (MultitenantMode.ADAPTIVE == mm)
			return new AdaptiveMultitenantSchemaPolicyContext(sc);
		else
			throw new SchemaException(Pass.FIRST, "Unknown multitenant mode: " + mm.describe());
	}
		
	Logger logger = Logger.getLogger(AdaptiveMultitenantSchemaPolicyContext.class);
	
	public static final String TENANT_COLUMN = "___mtid";
	public static final UnqualifiedName tenantColumnName = new UnqualifiedName(TENANT_COLUMN);
	
	public static final String LANDLORD_TENANT = PEConstants.LANDLORD_TENANT;
	
	protected SchemaEdge<PEDatabase> mtdb;
	protected SchemaEdge<IPETenant> tenant;
	// when this is true, there is no tenant, but we are still in multitenant mode
	// when it is null, we haven't computed it yet
	protected Boolean landlord;
	
	public AdaptiveMultitenantSchemaPolicyContext(SchemaContext cntxt) {
		super(cntxt);
		mtdb = null;
		landlord = null;
	}
	
	@Override
	public boolean isSchemaTenant() {
		return getCurrentTenant(false) != null;
	}
	
	@Override
	public TableScope getOfTenant(UserTable ut) {
		PETenant currentTenant = getCurrentTenant(false);
		if (currentTenant == null) return null;
		TableVisibility tv = sc.getCatalog().findVisibilityRecord(ut.getId(),currentTenant.getPersistentID());
		if (tv == null) return null;
		return TableScope.load(tv, sc);
	}
	
	@Override
	public Name getLocalName(UserTable ut) {
		PETenant currentTenant = getCurrentTenant(false);
		if (currentTenant == null) return super.getLocalName(ut);
		TableVisibility tv = sc.getCatalog().findVisibilityRecord(ut.getId(),currentTenant.getPersistentID());
		if (tv == null || tv.getLocalName() == null) return super.getLocalName(ut);
		return new UnqualifiedName(tv.getLocalName());
	}
	
	protected void checkAllowedLandlord(String what) {
		if (landlord == null) getCurrentTenant(false);
		if (!landlord) return;
		throw new SchemaException(Pass.SECOND, "Invalid landlord operation (use <db> first): " + what);
	}
		
	@Override
	public Database<?> getCurrentDatabase(Database<?> peds, boolean mustExist) {
		if (isRoot())
			return peds;
		getCurrentTenant(true);
		return peds;
	}

	@Override
	public boolean requiresMTRewrites(ExecutionType et) {
		if (ExecutionType.INSERT == et) {
			return true;
		} else {
			return (getCurrentTenant(false) != null);
		}
	}

	@Override
	public Long getTenantID(boolean mustExist) {
		PETenant pet = getCurrentTenant(mustExist);
		if (mustExist && pet == null)
			throw new SchemaException(new ErrorInfo(AvailableErrors.NO_DATABASE_SELECTED));
		if (pet == null) return null;
		return pet.getTenantID();
	}

	@Override
	public void removeValue(TableScope ts, long value) {
		sc.getCatalog().removeNextIncrementValue(sc, ts, value);
	}

	
	@SuppressWarnings("unchecked")
	protected PEDatabase getMultitenantDB() {
		if (mtdb == null) 
			mtdb = StructuralUtils.buildEdge(sc, sc.findSingleMTDatabase(), true); 
		return mtdb.get(sc);
	}
	
	protected PETenant getCurrentTenant(boolean mustExist) {
		if (Boolean.TRUE.equals(landlord)) return null;
		IPETenant candidate = null;
		if (tenant != null) candidate = tenant.get(sc);
		if (candidate == null) {
			tenant = sc.getCurrentTenant();
			if (tenant != null)
				candidate = tenant.get(sc);
		}
		// figure out if this is landlord mode.  it is if the db is set but the tenant is not.
		if (candidate == null && sc.hasCurrentDatabase()) {
			landlord = true;
			return null;
		} 
		if (candidate == null && mustExist) {
			if (logger.isDebugEnabled())
				logger.debug("no current tenant");
			throw new SchemaException(Pass.SECOND, "No database selected");
		}
		if (candidate != null) landlord = false;
		return (PETenant)candidate;		
	}
	
	@Override
	public PETenant getCurrentTenant() {
		return getCurrentTenant(false);
	}

	@Override
	public void modifyTablePart(PETable nt) {
		addTenantColumn(nt,tenantColumnName);
		modifyUniqueKeys(nt);
	}
	
	private void modifyUniqueKeys(PETable nt) {
		if (getSchemaContext().getOptions().isOmitTenantColumnInjection()) return;
		TenantColumn tc = nt.getTenantColumn(getSchemaContext());
		for(PEKey k : nt.getKeys(getSchemaContext())) 
			modifyKey(k, tc);
	}	
	
	private void modifyKey(PEKey k, TenantColumn tc) {
		if (!k.containsColumn(tc)) {
			if (k.isForeign()) {
				PEForeignKey pefk = (PEForeignKey) k;
				if (pefk.getDeleteAction() != ForeignKeyAction.SET_NULL && pefk.getUpdateAction() != ForeignKeyAction.SET_NULL)
					pefk.addColumn(0, tc, tenantColumnName);
			} else {
				k.addColumn(0, tc);
			}
		}		
	}

	protected void ensureTenantColumn(PEAlterTableStatement in) {
		PETable targ = in.getTarget();
		TenantColumn tc = targ.getTenantColumn(getSchemaContext());
		for(AlterTableAction aa : in.getActions()) {
			if (aa instanceof AddIndexAction) {
				AddIndexAction aia = (AddIndexAction) aa;
				PEKey ntc = aia.getNewIndex();
				modifyKey(ntc,tc);
			}
		}		
	}
	
	public Statement maybePassthroughAlterStatement(PEAlterTableStatement in) {
		AlterTableAction passthrough = null;
		AlterTableAction actionable = null;
		for(AlterTableAction aa : in.getActions()) {
			if (aa.isPassthrough())
				passthrough = aa;
			else
				actionable = aa;
		}
		String passthroughSQL = null;
		if (passthrough != null) {
			StringBuilder buf = new StringBuilder();
            Singletons.require(HostService.class).getDBNative().getEmitter().emitAlterAction(getSchemaContext(), passthrough, buf);
			passthroughSQL = buf.toString();
		}
			
		if (passthrough != null && actionable != null) {
			StringBuilder buf = new StringBuilder();
			buf.append("Currently unable to execute ");
            Singletons.require(HostService.class).getDBNative().getEmitter().emitAlterAction(getSchemaContext(), actionable, buf);
			buf.append(" and ").append(passthroughSQL).append(" in the same alter statement");
			throw new SchemaException(Pass.PLANNER, buf.toString());
		}
		if (passthrough != null) {
			return new EmptyStatement(passthroughSQL);
		}
		return null;

	}
	
	@Override
	public boolean showTenantColumn() {
		return getTenantID(false) == null;
	}
	
	
	public void applyDegenerateMultitenantFilter(DMLStatement in, LiteralExpression tenantID) {
		// first, see if we have any nested queries
		ListSet<ProjectingStatement> nested = EngineConstant.NESTED.getValue(in,getSchemaContext());
		if (nested != null) {
			// rewrite all children that have tables.
			for(ProjectingStatement ss : nested) {
				ListSet<TableKey> embedTables = EngineConstant.TABLES.getValue(ss,getSchemaContext());
				if (embedTables == null || embedTables.isEmpty()) continue;
				applyMultitenantFilter(ss,tenantID);
			}
		}
		ListSet<TableKey> parentTables = EngineConstant.TABLES.getValue(in,getSchemaContext());
		if (parentTables == null || parentTables.isEmpty()) return;
		applyMultitenantFilter(in,tenantID);		
	}
	
	// the degenerate multitenant filter does:
	// figure out all base tables in the from clause.
	// figure out all nonbase tables in the from clause.
	// add a tenant column equijoin to all explicit joins.  this covers the nonbase tables.
	// in the where clause, add a filter on all base tables.
	// due to non equijoin join conditions we have to be complete.
	@Override
	public void applyDegenerateMultitenantFilter(DMLStatement in) {
		if (!requiresMTRewrites(in.getExecutionType()) || landlord || (in instanceof InsertIntoValuesStatement))
			return;
		applyDegenerateMultitenantFilter(in,getTenantIDLiteral(true));
	}
	
	private void applyMultitenantFilter(DMLStatement in, LiteralExpression tenantID) {
		// does not apply if there is no where clause
		Edge<?, LanguageNode> wce = EngineConstant.WHERECLAUSE.getEdge(in);
		if (wce == null)
			return;
		
		ListSet<TableInstance> baseTables = new ListSet<TableInstance>(); 
		Edge<?,? extends LanguageNode> fromClause = EngineConstant.FROMCLAUSE.getEdge(in);
		for(LanguageNode ln : fromClause.getMulti()) {
			if (ln instanceof TableInstance)
				baseTables.add((TableInstance)ln);
			else if (ln instanceof FromTableReference) {
				FromTableReference ftr = (FromTableReference) ln;
				if (ftr.getBaseTable() != null)
					baseTables.add(ftr.getBaseTable());
			}
		}
		Edge<?,?> fcedge = EngineConstant.FROMCLAUSE.getEdge(in);
		ModifyJoinExTraversal joinEx = new ModifyJoinExTraversal(getSchemaContext(),tenantID);
		joinEx.traverse(fcedge);
		// now the from clause is modified.  for every base table add a where clause filter on the tenant id.
		ExpressionNode cwc = (ExpressionNode) wce.get();
		ArrayList<ExpressionNode> filters = new ArrayList<ExpressionNode>();
		ListSet<TableKey> notYetSpecified = new ListSet<TableKey>();
		ListSet<TableKey> uniqueTables = new ListSet<TableKey>();
		for(TableInstance ti : baseTables)
			uniqueTables.add(ti.getTableKey());
		uniqueTables.addAll(joinEx.getRequiresWhereClause());
		if (cwc != null) {
			cwc.setGrouped();
			filters.add(cwc);
			// see what's already specified.
			TenantColumnFullySpecifiedTraversal tcfs = new TenantColumnFullySpecifiedTraversal(getSchemaContext(),uniqueTables);
			tcfs.traverse(cwc);
			notYetSpecified = tcfs.getUnfiltered();
		} else {
			notYetSpecified = uniqueTables; 
		}
		for(TableKey tk : notYetSpecified) {
			PEAbstractTable<?> tab = tk.getAbstractTable();
			if (tab.isTempTable())
				continue;
			PEColumn tc = tab.getTenantColumn(getSchemaContext());
			FunctionCall fc = new FunctionCall(FunctionName.makeEquals(),
					new ColumnInstance(tc,tk.toInstance()),
					(ExpressionNode)tenantID.copy(null));
			filters.add(fc);
		}
		if (filters.size() == 0)
			return;
		if (filters.size() == 1)
			wce.set(filters.get(0));
		else {
			FunctionCall nwc = new FunctionCall(FunctionName.makeAnd(),filters);
			wce.set(nwc);
		}		
	}
	
	private static class ModifyJoinExTraversal extends Traversal {

		private final ListSet<TableKey> requiresWC;
		private final SchemaContext context;
		private final ExpressionNode tenantID;
		
		public ModifyJoinExTraversal(SchemaContext sc, ExpressionNode tenid) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			requiresWC = new ListSet<TableKey>();
			context = sc;
			tenantID = tenid;
		}
		
		public ListSet<TableKey> getRequiresWhereClause() {
			return requiresWC;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (in instanceof JoinedTable) {
				JoinedTable jt = (JoinedTable) in;
				// so if the original is of the form lhs = constant (i.e. an explicit join to a particular value)
				// we won't have an lhs and an rhs - but we still need to bound the side that is not a constant
				ListSet<ExpressionNode> parts = ExpressionUtils.decomposeAndClause(jt.getJoinOn());
				ListSet<ExpressionNode> extras = new ListSet<ExpressionNode>();
				HashSet<TableKey> already = new HashSet<TableKey>();
				// each part is either of the form column = column or column = constant, or constant = column
				// for the last two, rewrite to column = constant and tenantcolumn = tenantid
				// for the first rewrite to column = column and tenantcolumn = tenantcolumn
				// if in the first case one of the sides doesn't have a tenant column, specify it in the other
				// case anyways
				for(ExpressionNode c : parts) {
					if (EngineConstant.FUNCTION.has(c, EngineConstant.EQUALS)) {
						FunctionCall fc = (FunctionCall) c;
						ExpressionNode lhs = fc.getParametersEdge().get(0);
						ExpressionNode rhs = fc.getParametersEdge().get(1);
						ColumnInstance lhc = null;
						ColumnInstance rhc = null;
						TableKey ltk = null;
						TableKey rtk = null;
						if (lhs instanceof ColumnInstance) {
							lhc = (ColumnInstance) lhs;
							ltk = lhc.getTableInstance().getTableKey();
						}
						if (rhs instanceof ColumnInstance) {
							rhc = (ColumnInstance) rhs;
							rtk = rhc.getTableInstance().getTableKey();
						}
						if (lhc != null && rhc != null) {
							PEColumn ltc = lhc.getTableInstance().getAbstractTable().getTenantColumn(context);
							PEColumn rtc = rhc.getTableInstance().getAbstractTable().getTenantColumn(context);
							if (ltc != null && rtc != null) {
								extras.add(new FunctionCall(FunctionName.makeEquals(), 
										new ColumnInstance(ltc,lhc.getTableInstance()), 
										new ColumnInstance(rtc,rhc.getTableInstance())));
								already.add(ltk);
								already.add(rtk);
							} else if (ltc != null && !already.contains(ltk)) {
								extras.add(new FunctionCall(FunctionName.makeEquals(),
										new ColumnInstance(ltc,lhc.getTableInstance()),
										(ExpressionNode)tenantID.copy(null)));
								already.add(ltk);
							} else if (rtc != null && !already.contains(rtk)) {
								extras.add(new FunctionCall(FunctionName.makeEquals(),
										new ColumnInstance(rtc,rhc.getTableInstance()),
										(ExpressionNode)tenantID.copy(null)));
								already.add(rtk);
							}
						} else if (lhc != null) {
							PEColumn ltc = lhc.getTableInstance().getAbstractTable().getTenantColumn(context);
							if (ltc != null && !already.contains(ltk)) {
								extras.add(new FunctionCall(FunctionName.makeEquals(),
										new ColumnInstance(ltc,lhc.getTableInstance()),
										(ExpressionNode)tenantID.copy(null)));
								already.add(ltk);
							} 
						} else if (rhc != null) {
							PEColumn rtc = rhc.getTableInstance().getAbstractTable().getTenantColumn(context);
							if (rtc != null && !already.contains(rtk)) {
								extras.add(new FunctionCall(FunctionName.makeEquals(),
										new ColumnInstance(rtc,rhc.getTableInstance()),
										(ExpressionNode)tenantID.copy(null)));
								already.add(rtk);
							}
						}
					}
				}
				if (extras.isEmpty())
					return in;
				extras.addAll(parts);
				ExpressionNode newClause = ExpressionUtils.safeBuildAnd(extras);
				jt.getJoinOnEdge().set(newClause);
			}
			return in;
		}
		
	}

	private static class TenantColumnFullySpecifiedTraversal extends Traversal {

		private final Map<ColumnKey, TableKey> givenColumns;
		private final Set<ColumnKey> found;
		private final Set<ColumnKey> used;
		private MultiMap<LanguageNode, ColumnKey> filtered;
		private boolean done = false;
		
		public TenantColumnFullySpecifiedTraversal(SchemaContext pc, ListSet<TableKey> tabs) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			givenColumns = new LinkedHashMap<ColumnKey, TableKey>();
			for(TableKey tk : tabs)
				givenColumns.put(new ColumnKey(tk, tk.getAbstractTable().getTenantColumn(pc)), tk);
			found = new HashSet<ColumnKey>();
			found.addAll(givenColumns.keySet());
			filtered = new MultiMap<LanguageNode, ColumnKey>();
			used = new HashSet<ColumnKey>();
		}
		
		public ListSet<TableKey> getUnfiltered() {
			// the stuff remaining is the key set from givenColumns - found.
			// recall that cks leave found if they are not amongst deps at some level, so what's left is only stuff
			// that is fully filtered - but we don't care about that (but make sure the thing was used at all)
			for(ColumnKey ck : found) {
				if (used.contains(ck))
					givenColumns.remove(ck);
			}
			Collection<TableKey> tabs = givenColumns.values();
			ListSet<TableKey> out = new ListSet<TableKey>();
			out.addAll(tabs);
			return out;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			// if all the watched columns are lost, we're done
			if (done || found.isEmpty()) return in;
			if (EngineConstant.COLUMN.has(in)) {
				ColumnInstance ci = (ColumnInstance) in;
				if (givenColumns.containsKey(ci.getColumnKey())) {
					filtered.put(in, ci.getColumnKey());
					used.add(ci.getColumnKey());
				}
			} else if (EngineConstant.FUNCTION.has(in)) {
				FunctionCall fc = (FunctionCall) in;
				// if this is a not function, we're done - for safety we have to slap everything on.
				// if this is an or function, we have to slap only those columns which are not represented in
				// every branch.
				// for any other function, propagate upwards.
				if (fc.getFunctionName().isEffectiveNot()) {
					done = true;
					return in;
				} else if (fc.getFunctionName().isOr()) {
					HashSet<ColumnKey> intersectedDeps = new HashSet<ColumnKey>();
					boolean first = true;
					for(LanguageNode ln : fc.getParameters()) {
						Collection<ColumnKey> deps = filtered.get(ln);
						if (deps == null || deps.isEmpty()) continue;
						if (EngineConstant.CONSTANT.has(ln)) continue;
						for(Iterator<ColumnKey> iter = found.iterator(); iter.hasNext();) {
							if (!deps.contains(iter.next()))
								iter.remove();
						}
						if (first) {
							intersectedDeps.addAll(deps);
						} else {
							intersectedDeps.retainAll(deps);
						}
					}
					filtered.putAll(in, intersectedDeps);
				} else {
					// any others, set all child deps on this node
					HashSet<ColumnKey> deps = new HashSet<ColumnKey>();
					if (fc.getParameters().isEmpty())
						filtered.putAll(in, givenColumns.keySet());
					for(LanguageNode ln : fc.getParameters()) {
						Collection<ColumnKey> subdeps = filtered.get(ln);
						if (subdeps == null || subdeps.isEmpty()) continue;
						deps.addAll(subdeps);
					}
					
					filtered.putAll(in, deps);
				}
			}
			return in;
		}
		
	}
	
	@Override
	public boolean allowTenantColumnDeclaration() {
		return allowTenantColumnDeclarationChecking();
	}

	@Override
	public TableInstance buildInCurrentTenant(PESchema schema, UnqualifiedName n, LockInfo info) {
		if (landlord)
			return super.buildInCurrentTenant(schema, n, info);
		return getCurrentTenant(true).build(getSchemaContext(),n, info);
	}
	
	@Override
	public boolean isMTMode() {
		return true;
	}
	
	@Override
	public long getNextAutoIncrBlock(TableInstance tab, long blockSize) {
		if (getCurrentTenant(false) == null)
			return super.getNextAutoIncrBlock(tab, blockSize);
		if (tab instanceof MTTableInstance) {
			MTTableInstance mti = (MTTableInstance) tab;
			return sc.getCatalog().getNextIncrementValueChunk(getSchemaContext(), mti.getTableScope(), blockSize);
		}
		return super.getNextAutoIncrBlock(tab, blockSize);
	}

	@Override
	public long readAutoIncrBlock(TableKey tab) {
		if (getCurrentTenant(false) == null)
			return super.readAutoIncrBlock(tab);
		return sc.getCatalog().readNextIncrementValue(getSchemaContext(),tab);
	}

	@Override
	public void removeValue(TableKey tab, long value) {
		if (getCurrentTenant(false) == null) {
			super.removeValue(tab, value);
		} else {
			sc.getCatalog().removeNextIncrementValue(getSchemaContext(),tab, value);
		}
	}

	@Override
	public ValueHandler handleTenantColumnUponInsert(InsertIntoValuesStatement stmt, PEColumn column) {
		return new TenantIDValueHandler(stmt, column); 
	}

	private static class TenantIDValueHandler extends ValueHandler {
		
		public TenantIDValueHandler(InsertIntoValuesStatement stmt, PEColumn col) {
			super(stmt,col);			
		}

		@Override
		public ExpressionNode handleMissing(SchemaContext sc) {
			return sc.getPolicyContext().getTenantIDLiteral(true);
		}
	}

	
	public static DeleteStatement buildTenantDeleteFromTableStatement(SchemaContext sc, PETable tab, TableScope ts) { 
		TableInstance ti = new MTTableInstance(tab,ts,tab.getName(),null,true);
		FromTableReference ftr = new FromTableReference(ti);
		ExpressionNode wc = new FunctionCall(FunctionName.makeEquals(),
				new ColumnInstance(ts.getTable(sc).getTenantColumn(sc),ti),LiteralExpression.makeAutoIncrLiteral(ts.getTenant(sc).getTenantID()));
		ArrayList<FromTableReference> froms = new ArrayList<FromTableReference>();
		froms.add(ftr);
		DeleteStatement ds = new DeleteStatement(froms, wc, false, null);
		ds.getDerivedInfo().addLocalTable(ti.getTableKey());
		return ds;
	}

	public boolean canCreatePrivateTables() {
		return true;
	}

	public boolean canCreateSharedTables() {
		return true;
	}

	@Override
	public Statement modifyCreateTable(PECreateTableStatement stmt) {
		checkAllowedLandlord("create a table");
		if (stmt instanceof PECreateTableAsSelectStatement)
			throw new SchemaException(Pass.PLANNER, "Currently unsupported in mt mode - create table as select");
		if (stmt.getTable().isUserlandTemporaryTable())
			throw new SchemaException(Pass.PLANNER, "Currently unsupported in mt mode - create temporary table");
		PETable newTable = (PETable) stmt.getRoot();
		PETenant currentTenant = getCurrentTenant(true);
		PETable existingTable = currentTenant.lookup(getSchemaContext(),newTable.getName(), new LockInfo(LockType.EXCLUSIVE, "create table"));
		if (existingTable != null) {
			String anyDiffs = existingTable.definitionDiffers(newTable);
			if (anyDiffs != null)
				throw new SchemaException(Pass.SECOND, "Invalid redeclaration of table: " + anyDiffs);
			if ((stmt.isIfNotExists() == null || !stmt.isIfNotExists().booleanValue()) && !getSchemaContext().getOptions().isAllowDuplicates())
				// already created, without the if not exists bit - this is an error
				throw new SchemaException(Pass.SECOND, "Table " + newTable.getName() + " already exists");
			stmt.setOld();
			return stmt;
		}

		return new PECreateTenantTableStatement(stmt,currentTenant,newTable.getAutoIncOffset(getSchemaContext()),newTable.getName().getUnqualified());
	}
	
		
	@Override
	public Statement modifyDropTable(PEDropTableStatement peds) {
		if (peds.getTarget() == null) return peds;
		List<TableKey> tks = peds.getDroppedTableKeys();
		if (tks.size() > 1)
			// this is a limitation of the current implementation - we can circle back to this in a bit.
			throw new SchemaException(Pass.PLANNER, "Too many tables in drop table stmt");
		TableKey tk = tks.get(0);
		if (tk.isUserlandTemporaryTable())
			throw new SchemaException(Pass.PLANNER, "No support for drop temporary table in mt mode");
		TableScope ts = null;
		if (tk instanceof MTTableKey) {
			MTTableKey mttk = (MTTableKey) tk;
			ts = mttk.getScope();
		}
		PETenant tenant = getCurrentTenant(true);
		return new PEDropTenantTableStatement(peds,ts,tenant);
	}
	
	@Override
	public Statement modifyAlterTableStatement(PEAlterTableStatement in) {
		TableKey tk = in.getTableKey();
		if (tk.isUserlandTemporaryTable())
			throw new SchemaException(Pass.PLANNER, "No support for alter temporary table in mt mode");
		TableScope tenantScope = null;
		if (tk instanceof MTTableKey) {
			MTTableKey mtk = (MTTableKey) tk;
			tenantScope = mtk.getScope();
		}
		
		if (tenantScope == null) 
			throw new SchemaException(Pass.PLANNER, "Unable to find tenant table scope for altered table");

		Statement passthrough = maybePassthroughAlterStatement(in);
		if (passthrough != null) return passthrough;
		
		ensureTenantColumn(in);
		return new PEAlterTenantTableStatement(getSchemaContext(),in, tenantScope, tenantScope.getTenant(getSchemaContext()));
	}
	
	@Override
	public MultitenantMode getMTMode() {
		return MultitenantMode.ADAPTIVE;
	}
	
	public static boolean canRetry(Throwable t) {
		String[] cns = { 
				"java.sql.SQLException", 
				"org.hibernate.HibernateException",
				"org.hibernate.exception.LockAcquisitionException",
				"org.hibernate.exception.ConstraintViolationException",
				"javax.persistence.EntityNotFoundException",
				"org.hibernate.StaleStateException"
		};
		Throwable p = t;
		while(p != null) {
			Class<?> c = p.getClass();
			String cn = c.getName();
			for(int i = 0; i < cns.length; i++)
				if (cns[i].equals(cn)) {
					if (i == 0) {
						String message = p.getMessage();
						if (!message.startsWith("Lock wait timeout exceeded"))
							continue;
					} else if (i == 1) {
						String message = p.getMessage();
						if (!message.startsWith("More than one row with the given identifier was found"))
							continue;
					}
					return true;
				}
			p = p.getCause();
		}
		return false;		
	}

}
