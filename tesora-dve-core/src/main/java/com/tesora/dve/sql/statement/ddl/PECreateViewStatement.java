package com.tesora.dve.sql.statement.ddl;

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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.TableState;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserView;
import com.tesora.dve.common.catalog.ViewMode;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.distribution.BroadcastDistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepUpdateAllOperation;
import com.tesora.dve.queryplan.QueryStepDDLNestedOperation.NestedOperationDDLCallback;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.structural.LimitSpecification;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PEUser;
import com.tesora.dve.sql.schema.PEView;
import com.tesora.dve.sql.schema.PEViewTable;
import com.tesora.dve.sql.schema.RangeDistributionVector;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaVariables;
import com.tesora.dve.sql.schema.TableComponent;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.UserScope;
import com.tesora.dve.sql.schema.VectorRange;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep;
import com.tesora.dve.sql.transform.execution.ComplexDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionPlan;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.worker.MysqlTextResultCollector;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

public class PECreateViewStatement extends
		PECreateStatement<PEView, UserView> {

	// always set - this is part of the persistent planner
	private final PEDatabase ofDB;
	private final PEStorageGroup ofGroup;
	private final List<UnqualifiedName> columnNames;
	
	private final PEViewTable tschema;
	
	private final boolean orReplace;
	
	protected PECreateViewStatement(PEView newView, boolean createOrReplace, PEDatabase ofDB, PEStorageGroup ofGroup, 
			List<UnqualifiedName> colNames) {
		super(newView, false, null, "VIEW", false);
		this.ofDB = ofDB;
		this.ofGroup = ofGroup;
		this.columnNames = colNames;
		this.tschema = null;
		this.orReplace = createOrReplace;
	}

	public PECreateViewStatement(SchemaContext sc, boolean createOrReplace, PEViewTable pevts) {
		super(pevts.getView(sc),false,null,"VIEW",false);
		this.ofDB = pevts.getPEDatabase(sc);
		this.ofGroup = pevts.getPersistentStorage(sc);
		this.columnNames = null;
		this.tschema = pevts;
		this.orReplace = createOrReplace;
	}
	
	public boolean isCreateOrReplace() {
		return orReplace;
	}
	
	// tschema ONLY
	public PEViewTable getViewTable() {
		return tschema;
	}
	
	public static PECreateViewStatement build(SchemaContext sc, Name name, ProjectingStatement definition, 
			UserScope definer, List<UnqualifiedName> givenNames,
			String algo, String security, String checkOption, 
			boolean replaceIfExists, List<TableComponent<?>> tschemaColDefs) {
		// we only build a minimal view here - for instance we're going to remove most of the column related info
		PEStorageGroup theGroup = null;
		try {
			theGroup = definition.getSingleGroup(sc);
		} catch (PEException pe) {
			throw new SchemaException(Pass.SECOND, "Unable to determine single group for view",pe);
		}
		PEDatabase theDB = (PEDatabase) definition.getDatabase(sc);
		
		PEUser user = null;
		if (definer == null || sc.getOptions().isIgnoreMissingUser())
			user = sc.getCurrentUser().get(sc);
		else {
			user = sc.findUser(definer.getUserName(), definer.getScope());
			if (user == null)
				throw new SchemaException(Pass.SECOND, "No such user: " + definer.getSQL());
		}
		
		PEAbstractTable<?> existing = sc.findTable(PEAbstractTable.getTableKey(theDB, name));
		if (existing != null) {
			if (existing.isTable()) {
				throw new SchemaException(Pass.SECOND, "Table " + name + " already exists");
			} else if (!replaceIfExists) {
				throw new SchemaException(Pass.SECOND, "View " + name + " already exists");
			}
		}
		
		// figure out the collation and charset
		String charset = SchemaVariables.getCharacterSetClient(sc);
		String collation = SchemaVariables.getCollationConnection(sc);

		ProjectingStatement copy = CopyVisitor.copy(definition);
		
		List<UnqualifiedName> columnNames = new ArrayList<UnqualifiedName>();
		if (givenNames != null && !givenNames.isEmpty())
			columnNames.addAll(givenNames);
		else {
			ProjectionInfo pi = copy.getProjectionMetadata(sc);
			HashSet<String> names = new HashSet<String>();
			for(int i = 1; i <= pi.getWidth(); i++) {
				String cn = pi.getColumnAlias(i);
				if (names.add(cn)) {
					columnNames.add(new UnqualifiedName(cn));
				} else {
					throw new SchemaException(Pass.PLANNER, "Duplicate column name " + cn);
				}
			}			
		}

		// we can push the view down if processing it only requires one step - so figure that out now
		ViewMode vm = null;
		ParserOptions pm = sc.getOptions();
		try {
			ParserOptions npm = pm.setInhibitSingleSiteOptimization();
			sc.setOptions(npm);
			ExecutionPlan ep = Statement.getExecutionPlan(sc, copy);
			if (ep.getSequence().getSteps().size() > 1)
				vm = ViewMode.EMULATE;
			else
				vm = ViewMode.ACTUAL;
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to compute view definition plan",pe);
		} finally {
			sc.setOptions(pm);
		}
				
		String checkMode = (checkOption == null ? "NONE" : checkOption);
		String algorithm = (algo == null ? "UNDEFINED" : algo);
		String sec = (security == null ? "DEFINER" : security);
		
		PEView nv = new PEView(sc,name.getUnqualified(),theDB,user,definition, 
				new UnqualifiedName(charset), new UnqualifiedName(collation), vm, checkMode, algorithm, sec);
						
		if (tschemaColDefs != null) {
			// make sure that the names match
			if (tschemaColDefs.size() != columnNames.size())
				throw new SchemaException(Pass.SECOND, "Invalid tschema table def - wrong number of columns");
			for(int i = 0; i < columnNames.size(); i++) {
				TableComponent<?> tc = tschemaColDefs.get(i);
				if (tc instanceof PEColumn) {
					PEColumn pec = (PEColumn) tc;
					if (pec.getName().equals(columnNames.get(i))) {
						// ok
					} else {
						throw new SchemaException(Pass.SECOND, "Invalid tschema table def - mismatched names");
					}
				} else {
					throw new SchemaException(Pass.SECOND, "Invalid tschema table def - keys not allowed");
				}
			}
			try {
				PEViewTable pevt = new PEViewTable(sc,name.getUnqualified(),tschemaColDefs,
						buildDistributionVector(sc,definition, vm, tschemaColDefs),
						(PEPersistentGroup)definition.getSingleGroup(sc),
						theDB, TableState.SHARED, nv);
				return new PECreateViewStatement(sc,replaceIfExists,pevt);
			} catch (PEException pe) {
				throw new SchemaException(Pass.SECOND, "Can't get persistent group", pe);
			}
		}
		
		return new PECreateViewStatement(nv,replaceIfExists,theDB,theGroup,columnNames);
	}

	protected static ProjectingStatement buildMetadataQuery(ProjectingStatement in) {
		ProjectingStatement ps = CopyVisitor.copy(in);
		ps.setLimit(new LimitSpecification(LiteralExpression.makeLongLiteral(0L),LiteralExpression.makeLongLiteral(0L)));
		return ps;
	}
	
	protected static DistributionVector buildDistributionVector(SchemaContext sc, ProjectingStatement in, ViewMode vm,
			List<TableComponent<?>> comps) {
		if (vm == ViewMode.EMULATE) {
			// multistep plan - so let's say it is random distributed.
			return new DistributionVector(sc,Collections.<PEColumn>emptyList(), DistributionVector.Model.RANDOM, true);
		} else {
			// if it's a passthrough view - we should still try to figure out what it is dist'd on.
			// but not if it is a union.
			ListSet<DistributionVector> vectors = EngineConstant.DISTRIBUTION_VECTORS.getValue(in, sc);
			// we prefer vectors that use columns over those that don't.
			ListSet<DistributionVector> columnCandidates = new ListSet<DistributionVector>();
			ListSet<DistributionVector> nonColumnCandidates = new ListSet<DistributionVector>();
			for(DistributionVector dv : vectors) {
				if (dv.getModel().getUsesColumns()) {
					columnCandidates.add(dv);
				} else {
					nonColumnCandidates.add(dv);
				}
			}
			if (!columnCandidates.isEmpty() && in instanceof SelectStatement) {
				// build a mapping from backing column keys to columns in the view
				ListOfPairs<ColumnKey,PEColumn> backing = new ListOfPairs<ColumnKey,PEColumn>();
				SelectStatement ss = (SelectStatement) in;
				for(int i = 0; i < ss.getProjectionEdge().size(); i++) {
					ExpressionNode targ = ExpressionUtils.getTarget(ss.getProjectionEdge().get(i));
					if (targ instanceof ColumnInstance) {
						ColumnInstance ci = (ColumnInstance) targ;
						backing.add(ci.getColumnKey(),(PEColumn)comps.get(i));
					}
				}
				DistributionVector candidate = null;
				List<PEColumn> vc = new ArrayList<PEColumn>();
				for(DistributionVector dv : columnCandidates) {
					vc.clear();
					List<PEColumn> cols = dv.getColumns(sc);
					TableKey tk = null;
					for(PEColumn pec : cols) {
						for(Pair<ColumnKey, PEColumn> p : backing) {
							if (p.getFirst().getColumn() == pec) {
								if (tk == null) {
									tk = p.getFirst().getTableKey();
									vc.add(p.getSecond());
								} else if (tk.equals(p.getFirst().getTableKey())) {
									vc.add(p.getSecond());
								}
							}
						}
					}
					if (vc.size() == cols.size()) {
						candidate = dv;
						break;
					}
				}
				if (candidate != null) {
					// we just need to copy over the range
					if (candidate.isRange()) {
						// have to create a new vector range
						VectorRange vr = new VectorRange(sc, candidate.getRangeDistribution().getDistribution(sc));
						return new RangeDistributionVector(sc, vc, false, vr);
					}
				}
			}
			if (!columnCandidates.isEmpty()) {
				// couldn't figure it out - but it definitely has non bcast tables in it so set it to random
				return new DistributionVector(sc,Collections.<PEColumn>emptyList(), DistributionVector.Model.RANDOM, true);				
			}
			boolean allBCast = true;
			for(DistributionVector dv : nonColumnCandidates) {
				if (!dv.isBroadcast())
					allBCast = false;
			}
			return new DistributionVector(sc, Collections.<PEColumn>emptyList(), (allBCast ? DistributionVector.Model.BROADCAST : DistributionVector.Model.RANDOM), true);			
		}
	}
	
	@Override
	protected CatalogModificationExecutionStep buildStep(SchemaContext pc) throws PEException {
		PEView nv = getCreated().get();

		SQLCommand pushDown = null;
		if (nv.getMode() == ViewMode.ACTUAL) {
			GenericSQLCommand gsql = getGenericSQL(pc,false,false);
			pushDown = new SQLCommand(gsql.resolve(pc,null));
		}
		
		// I want to acquire locks on the underlying tables here so that we get a consistent metadata view
		return new ComplexDDLExecutionStep(ofDB,ofGroup,nv,Action.CREATE,
				new CreateViewOperation(pc,nv,orReplace,ofDB,buildMetadataQuery(nv.getViewDefinition(pc,null,true)),columnNames, pushDown));
	}

	
	private static class CreateViewOperation extends NestedOperationDDLCallback {

		private final PEView nascentDefinition;
		private SchemaContext context;
		private final SQLCommand littleQuery;
		private final PEDatabase onDatabase;
		private final List<UnqualifiedName> columnNames;
		private PEViewTable backingTable;
		private List<CatalogEntity> updates;
		private List<CatalogEntity> deletes;
		private final CacheInvalidationRecord record;
		private final SQLCommand pushDown;
		private final boolean createOrReplace;
		private SQLCommand emulatedDefinition;
		private Statement dropCommand;
		
		public CreateViewOperation(SchemaContext sc, PEView nv, boolean orReplace, PEDatabase onDB, ProjectingStatement firstStep, 
				List<UnqualifiedName> colNames,
				SQLCommand pd) {
			this.context = sc;
			this.nascentDefinition = nv;
			GenericSQLCommand gsql = firstStep.getGenericSQL(context, false,false);
			littleQuery = new SQLCommand(gsql.resolve(context,null));			
			this.columnNames = colNames;
			this.onDatabase = onDB;
			this.backingTable = null;
			this.updates = null;
			this.record = new CacheInvalidationRecord(nv.getCacheKey(),InvalidationScope.LOCAL);
			this.pushDown = pd;
			this.createOrReplace = orReplace;
			this.emulatedDefinition = null;
		}
		
		@Override
		public void beforeTxn(SSConnection conn, CatalogDAO c, WorkerGroup wg)
				throws PEException {
			this.updates = null;
			if (backingTable == null) {
				context.refresh(false);

				MysqlTextResultCollector tempRc = new MysqlTextResultCollector(true);

				WorkerExecuteRequest req = new WorkerExecuteRequest(conn.getTransactionalContext(), littleQuery).onDatabase(onDatabase);
				wg.execute(MappingSolution.AnyWorker, req, tempRc);

				List<ColumnMetadata> cmd = tempRc.getColumnSet().getColumnList(); 
			
				// create columns from the backing information
				List<TableComponent<?>> columns = new ArrayList<TableComponent<?>>();				

				if (cmd.size() != columnNames.size())
					throw new PEException("Invalid view computation: src has " + columnNames.size() + " columns but metadata query has " + cmd.size());
				for(int i = 0; i < columnNames.size(); i++) {
					UserColumn uc = new UserColumn(cmd.get(i));
					uc.setName(columnNames.get(i).getUnquotedName().get());
					columns.add(PEColumn.build(context, uc));
				}
				
				backingTable = new PEViewTable(context, nascentDefinition.getName(),columns,
						buildDistributionVector(context, nascentDefinition.getViewDefinition(context,null,false), nascentDefinition.getMode(), columns),
						(PEPersistentGroup) nascentDefinition.getViewDefinition(context,null,false).getSingleGroup(context),
						onDatabase,
						TableState.SHARED,
						nascentDefinition);
				backingTable.setDeclaration(context, backingTable);
				if (nascentDefinition.getMode() == ViewMode.EMULATE) {
					emulatedDefinition = new SQLCommand(backingTable.getDeclaration());
				}
			}			
		}

		@SuppressWarnings("unchecked")
		@Override
		public void inTxn(SSConnection conn, WorkerGroup wg) throws PEException {
			context.refresh(false);
			context.forceMutableSource();
			
			if (createOrReplace) {
				PEAbstractTable<?> existing = context.findTable(PEAbstractTable.getTableKey(onDatabase, nascentDefinition.getName()));
				if (existing != null && existing.isView()) {
					ViewMode vm = existing.asView().getView(context).getMode(); 
					if (vm == ViewMode.EMULATE) {
						// if it was emulated we would have tossed a table down
						dropCommand = new PEDropTableStatement(context, Collections.singletonList(new TableKey(existing,context.getNextTable())), Collections.EMPTY_LIST, true); 
					} else {
						dropCommand = new PEDropViewStatement(existing.asView(), true);
					}
					context.beginSaveContext();
					try {
						existing.persistTree(context);
						deletes = Functional.toList(context.getSaveContext().getObjects());
					} finally {
						context.endSaveContext();
					}
				}
			}
			
			context.beginSaveContext();
			try {
				backingTable.persistTree(context);
				updates = new ArrayList<CatalogEntity>(context.getSaveContext().getObjects());
			} finally {
				context.endSaveContext();
			}
		}

		@Override
		public List<CatalogEntity> getUpdatedObjects() throws PEException {
			return updates;
		}

		@Override
		public List<CatalogEntity> getDeletedObjects() throws PEException {
			if (deletes == null)
				return Collections.emptyList();
			else
				return deletes;
		}

		@Override
		public SQLCommand getCommand(CatalogDAO c) {
			return SQLCommand.EMPTY;
		}

		@Override
		public boolean canRetry(Throwable t) {
			return false;
		}

		@Override
		public boolean requiresFreshTxn() {
			return true;
		}

		@Override
		public String description() {
			return this.getClass().getSimpleName();
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return record;
		}

		@Override
		public boolean requiresWorkers() {
			return true;
		}

		@Override
		public void executeNested(SSConnection conn, WorkerGroup wg,
				DBResultConsumer resultConsumer) throws Throwable {
			List<SQLCommand> cmds = new ArrayList<SQLCommand>();
			if (dropCommand != null) {
				cmds.add(dropCommand.getGenericSQL(context,false,false).getSQLCommand());
			}
			cmds.add(pushDown != null ? pushDown : emulatedDefinition);
			for(SQLCommand sqlc : cmds) {
				QueryStepUpdateAllOperation qsuo = new QueryStepUpdateAllOperation(onDatabase,BroadcastDistributionModel.SINGLETON,sqlc);
				qsuo.execute(conn, wg, resultConsumer);
			}
		}
	}
	
}
