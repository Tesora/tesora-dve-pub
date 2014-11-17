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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.TemporaryTable;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.GroupDispatch;
import com.tesora.dve.db.NativeType;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.ExecutionState;
import com.tesora.dve.queryplan.QueryStepDDLNestedOperation.NestedOperationDDLCallback;
import com.tesora.dve.queryplan.QueryStepMultiTupleRedistOperation;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.TempTableDeclHints;
import com.tesora.dve.queryplan.TempTableGenerator;
import com.tesora.dve.resultset.ColumnInfo;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.messaging.ConditionalWorkerRequest;
import com.tesora.dve.server.messaging.ConditionalWorkerRequest.GuardFunction;
import com.tesora.dve.server.messaging.WorkerExecuteRequest;
import com.tesora.dve.server.messaging.WorkerRequest;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.NameAlias;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.ComplexPETable;
import com.tesora.dve.sql.schema.PEAbstractTable.TableCacheKey;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.modifiers.ColumnKeyModifier;
import com.tesora.dve.sql.schema.modifiers.ColumnModifier;
import com.tesora.dve.sql.schema.modifiers.ColumnModifierKind;
import com.tesora.dve.sql.schema.modifiers.DefaultValueModifier;
import com.tesora.dve.sql.schema.modifiers.StringTypeModifier;
import com.tesora.dve.sql.schema.modifiers.TypeModifier;
import com.tesora.dve.sql.schema.modifiers.TypeModifierKind;
import com.tesora.dve.sql.schema.types.BasicType;
import com.tesora.dve.sql.schema.types.TempColumnType;
import com.tesora.dve.sql.schema.types.Type;
import com.tesora.dve.sql.schema.validate.ValidateResult;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UnionStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.behaviors.DelegatingBehaviorConfiguration;
import com.tesora.dve.sql.transform.behaviors.FeaturePlanTransformerBehavior;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.transform.execution.ComplexDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.EmptyExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.IdentityConnectionValuesMap;
import com.tesora.dve.sql.transform.strategy.AdhocFeaturePlanner;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistributionFlags;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryPredicate;
import com.tesora.dve.worker.Worker;
import com.tesora.dve.worker.WorkerGroup;

public class PECreateTableAsSelectStatement extends PECreateTableStatement {

	private ProjectingStatement srcStmt;
	private ListOfPairs<PEColumn,Integer> projOffsets;
	private PEColumn unspecifiedAutoinc;
	private int specifiedAutoIncOffset;
	private InhibitTableDrop inhibitor;
	
	public PECreateTableAsSelectStatement(Persistable<PETable, UserTable> targ,
			Boolean ine, boolean exists, ProjectingStatement src, ListOfPairs<PEColumn,Integer> projectionOffsets) {
		super(targ, ine, exists);
		srcStmt = src;
		this.projOffsets = projectionOffsets;
		this.unspecifiedAutoinc = null;
		specifiedAutoIncOffset = -1;
		inhibitor = new InhibitTableDrop();
	}

	public ProjectingStatement getSourceStatement() {
		return srcStmt;
	}

	@Override
	public void plan(SchemaContext pc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		
		if (srcStmt instanceof UnionStatement) {
			// unsupported for now
			throw new PEException("create table as ... select ... union currently not supported");
		}
		
		// normalize the table decl as usual
		normalize(pc);
		if (alreadyExists) {
			es.append(new EmptyExecutionStep(0,"already exists - " + getSQL(pc)));
			return;
		}
		
		// if we can't do the one step plan on this, give up.  it's not clear right now how that would work.
		boolean immediate = !pc.isPersistent() ||
				Functional.all(related, new UnaryPredicate<DelayedFKDrop>() {

					@Override
					public boolean test(DelayedFKDrop object) {
						return object.getKeyIDs().isEmpty();
					}
					
				});
		if (!immediate)
			throw new PEException("create table as select with dangling fks in relaxed mode not supported yet");
		
		ExecutionSequence subes = new ExecutionSequence(null);
		maybeDeclareDatabase(pc,subes);
		
		SelectStatement select = (SelectStatement) CopyVisitor.copy(srcStmt);
		
//		SQLMode sqlMode = SchemaVariables.getSQLMode(pc);
		
		HashMap<PEColumn,Integer> columnOffsets = new HashMap<PEColumn,Integer>();
		for(Pair<PEColumn,Integer> p : projOffsets)
			columnOffsets.put(p.getFirst(),p.getSecond());
		for(PEColumn pec : getTable().getColumns(pc)) {
			Integer exists = columnOffsets.get(pec);
			if (exists == null) {
				if (pec.isAutoIncrement()) {
					unspecifiedAutoinc = pec;
					continue;
				}
				exists = select.getProjectionEdge().size();
				ExpressionNode value = null;
				ExpressionNode defaultValue = pec.getDefaultValue();
				if (defaultValue == null) {
					if (pec.isNullable())
						value = LiteralExpression.makeNullLiteral();
					// TODO: how does this iteract?
//					else if (!sqlMode.isStrictMode()) 
					else
						value = pec.getType().getZeroValueLiteral();					
				} else {
					value = (ExpressionNode) defaultValue.copy(null);
				}
				ExpressionAlias ea = new ExpressionAlias(value, new NameAlias(pec.getName().getUnqualified()),false);
				select.getProjectionEdge().add(ea);
				columnOffsets.put(pec,exists);
			} else if (pec.isAutoIncrement())
				specifiedAutoIncOffset = exists;
		}
		
		
		// we need to use the delegator here to get the right behavior
		ParserOptions opts = pc.getOptions();
		try {
			pc.setOptions(opts.setForceSessionPushdown());
			select.plan(pc, subes, new PlannerConfiguration(pc.getBehaviorConfiguration()));
		} finally {
			pc.setOptions(opts);
		}
		
		ArrayList<QueryStepOperation> steps = new ArrayList<QueryStepOperation>();
		subes.schedule(null, steps, null, pc, new IdentityConnectionValuesMap(pc.getValues()),null);
		int redistOffset = steps.size() - 1;

		boolean mustRebuildCTS = false;
		List<ValidateResult> results = getTable().validate(pc,false);
		for(ValidateResult vr : results) {
			if (vr.isError()) continue;
			if (vr.getSubject() instanceof PEForeignKey) {
				PEForeignKey pefk = (PEForeignKey) vr.getSubject();
				pefk.setPersisted(false);
				addIgnoredFKMessage(pc,vr);
				mustRebuildCTS = true;
			}
		}
		if (mustRebuildCTS) 
			// we need to rebuild the create table stmt
			getTable().setDeclaration(pc, getTable());
		CacheInvalidationRecord record = null;
		if (!getTable().isUserlandTemporaryTable()) {
			ListOfPairs<SchemaCacheKey<?>,InvalidationScope> clears = new ListOfPairs<SchemaCacheKey<?>,InvalidationScope>();
			clears.add(getTable().getCacheKey(),InvalidationScope.LOCAL);
			for(DelayedFKDrop dfd : related)
				clears.add(dfd.getTable().getCacheKey(),InvalidationScope.LOCAL);
			for(TableCacheKey tck : alsoClear)
				clears.add(tck, InvalidationScope.LOCAL);
			record = new CacheInvalidationRecord(clears);
		}

		CreateTableViaRedistCallback cb =
				new CreateTableViaRedistCallback(pc,(ComplexPETable)getTable(),steps,redistOffset,record);
		
		inhibitor.setCallback(cb);
		
		es.append(new ComplexDDLExecutionStep(getTable().getPEDatabase(pc),getTable().getStorageGroup(pc),getTable(),
				Action.CREATE, cb));		
	}

	private class PlannerConfiguration extends DelegatingBehaviorConfiguration {

		public PlannerConfiguration(BehaviorConfiguration target) {
			super(target);
		}

		@Override
		public FeaturePlanTransformerBehavior getPostPlanningTransformer(
				PlannerContext pc, DMLStatement original) {
			return new FinalStepTransformer(getTarget());
		}
		
	}
	
	private static final ColumnModifier notNullableModifier = new ColumnModifier(ColumnModifierKind.NOT_NULLABLE);
	private static final BigInteger maxIntValue = BigInteger.valueOf(4294967295L);
	private static final BigInteger minIntValue = BigInteger.valueOf(-2147483648L);
	
	public static PEColumn createColumnFromExpression(SchemaContext pc, ColumnInfo info, ExpressionNode expr) {
		ExpressionNode target = ExpressionUtils.getTarget(expr);
		// we can handle the following cases immediately:
		// constants:
		//   null - binary(0) default null
		//   integral - build the appropriate type set to not null default '0'
		//   string - build a varchar of the length, character set utf8 not null default '',
		//   floating pt - decimal of the appropriate type not null default '0.0'
		// columns:
		//   copy the def through
		// for all others we have to delay until the redist is available
		
		if (target instanceof LiteralExpression) {
			Type ctype = null;
			List<ColumnModifier> modifiers = new ArrayList<ColumnModifier>();
			LiteralExpression litex = (LiteralExpression) target;
			if (litex.isNullLiteral()) {
				ctype = BasicType.buildType("BINARY", 0, Collections.<TypeModifier> emptyList(),pc.getTypes());
				modifiers.add(new DefaultValueModifier(litex));
			} else if (litex.isStringLiteral()) {
				String str = litex.asString(pc.getValues());
				ctype = BasicType.buildType("VARCHAR",str.length(),
						Arrays.asList(new TypeModifier[] { new StringTypeModifier(TypeModifierKind.CHARSET,"utf8")}),pc.getTypes());
				modifiers.add(notNullableModifier);
				modifiers.add(new DefaultValueModifier(LiteralExpression.makeStringLiteral("")));
			} else if (litex.isFloatLiteral()) {
				String value = litex.toString(pc);
				BigDecimal bd = new BigDecimal(value);
				NativeType nt = null;
				try {
					nt = Singletons.require(HostService.class).getDBNative().findType("DECIMAL");
				} catch (PEException pe) {
					throw new SchemaException(Pass.FIRST, "Unable to load decimal type",pe);
				}
				ctype = BasicType.buildType(nt, 0, bd.precision(), bd.scale(), Collections.<TypeModifier> emptyList());
				modifiers.add(notNullableModifier);
				modifiers.add(new DefaultValueModifier(LiteralExpression.makeStringLiteral("0")));
			} else if (litex.isNumericLiteral()) {
				String value = litex.toString(pc);
				BigInteger bi = new BigInteger(value);
				String normalized = bi.toString();
				int displayWidth = normalized.length();
				boolean bigint = false;
				if (bi.compareTo(BigInteger.ZERO) < 0) {
					bigint = (bi.compareTo(minIntValue) < 0);
				} else {
					bigint = (bi.compareTo(maxIntValue) > 0);
				}
				if (bigint)
					ctype = BasicType.buildType("BIGINT", displayWidth, Collections.<TypeModifier> emptyList(), pc.getTypes());
				else
					ctype = BasicType.buildType("INT", displayWidth, Collections.<TypeModifier> emptyList(), pc.getTypes());
				modifiers.add(notNullableModifier);
				modifiers.add(new DefaultValueModifier(LiteralExpression.makeStringLiteral("0")));				
			} else {
				throw new SchemaException(Pass.SECOND, "Unhandled literal kind for create table as select: '" + litex.toString(pc) + "'");
			}
			return PEColumn.buildColumn(pc, new UnqualifiedName(info.getAlias()), ctype, modifiers, null, Collections.<ColumnKeyModifier> emptyList());
		} else if (target instanceof ColumnInstance) {
			ColumnInstance ci = (ColumnInstance) target;
			Column<?> c = ci.getColumn();
			if (c instanceof PEColumn) {
				PEColumn pec = ci.getPEColumn();
				PEColumn copy = (PEColumn) pec.copy(pc, null);
				if (copy.isAutoIncrement()) {
					copy.clearAutoIncrement();
					copy.setDefaultValue(LiteralExpression.makeStringLiteral("0"));
				} else if (pec.isPrimaryKeyPart()) {
					if (copy.getDefaultValue() == null)
						copy.setDefaultValue(LiteralExpression.makeStringLiteral("0"));
				}
				copy.normalize();
				copy.setName(new UnqualifiedName(info.getAlias()));
				return copy;
			}
		}

		// we have to defer, use a placeholder type
		PEColumn pec = new PEColumn(pc, new UnqualifiedName(info.getAlias()), TempColumnType.TEMP_TYPE);
		return pec;
	}

	private class FinalStepTransformer implements FeaturePlanTransformerBehavior {

		private final BehaviorConfiguration defaultBehavior;
		
		public FinalStepTransformer(BehaviorConfiguration superImpl) {
			this.defaultBehavior = superImpl;
		}
		
		
		@Override
		public FeatureStep transform(PlannerContext pc, DMLStatement stmt,
				FeatureStep existingPlan) throws PEException {
			// do our own redist first, then call the default
			// the last step in the plan will be select, so just create a redist step now
			// if the target table uses columns for distribution, we need to figure out the offsets

			ProjectingFeatureStep last = (ProjectingFeatureStep) existingPlan;
			SelectStatement ss = (SelectStatement) last.getPlannedStatement();
			
			HashMap<PEColumn, Integer> dvOffsets = new HashMap<PEColumn,Integer>();
			
			for(Pair<PEColumn,Integer> p : projOffsets) {
				ExpressionNode en = ss.getProjectionEdge().get(p.getSecond());
				PEColumn c = p.getFirst();
				if (c.isPartOfDistributionVector())
					dvOffsets.put(c, p.getSecond());
				if (en instanceof ExpressionAlias) {
					ExpressionAlias ea = (ExpressionAlias) en;
					ea.setAlias(c.getName().getUnqualified());
				} else {
					Edge<?,ExpressionNode> pedge = en.getParentEdge();
					ExpressionAlias ea = new ExpressionAlias(en, new NameAlias(c.getName().getUnqualified()),false);
					pedge.set(ea);
				}
			}
			
			List<Integer> dv = new ArrayList<Integer>();
			for(PEColumn pec : getTable().getDistributionVector(pc.getContext()).getColumns(pc.getContext())) {
				Integer offset = dvOffsets.get(pec);
				dv.add(offset);
			}
			
			FeatureStep out = new RedistFeatureStep(new AdhocFeaturePlanner(),
					last,TableKey.make(pc.getContext(),getTable(),0),
					getTable().getStorageGroup(pc.getContext()),
					dv,
					new RedistributionFlags().withRowCount(true)
						.withAutoIncColumn(unspecifiedAutoinc)
						.withExistingAutoInc(specifiedAutoIncOffset)
						.withTableGenerator(new CTATableGenerator(pc.getContext(),inhibitor)));
			
			return defaultBehavior.getPostPlanningTransformer(pc, stmt).transform(pc, stmt, out);
		}

		
	}
	

	private class CreateTableViaRedistCallback extends NestedOperationDDLCallback {

		private final List<QueryStepOperation> toExecute;
		private final int redistOffset;
		private final ComplexPETable target;
		
		private final CacheInvalidationRecord record;
		
		private List<CatalogEntity> updates;
		private List<CatalogEntity> deletes;
		
		private boolean redistCompleted = false;

		// create temporary table as select support
		private final SchemaContext context;
		int pincount = 0;

		
		public CreateTableViaRedistCallback(SchemaContext cntxt, ComplexPETable tab, List<QueryStepOperation> steps,
				int redistOffset,
				CacheInvalidationRecord record) {
			this.target = tab;
			this.toExecute = steps;
			this.updates = null;
			this.deletes = null;
			this.record = record;
			this.redistOffset = redistOffset;
			this.context = cntxt;
		}
		
		@Override
		public void inTxn(SSConnection conn, WorkerGroup wg) throws PEException {
			// the table would have been updated via the callback, so now we just build a new context
			// and persist out the updates
			deletes = Collections.emptyList();
			updates = new ArrayList<CatalogEntity>();
			if (target.isUserlandTemporaryTable()) {
				updates.add(new TemporaryTable(target.getName().getUnqualified().getUnquotedName().get(),
						target.getPEDatabase(context).getName().getUnqualified().getUnquotedName().get(),
						target.getEngine().getPersistent(),
						conn.getConnectionId()));
			} else {
				SchemaContext sc = SchemaContext.makeMutableIndependentContext(context);
				
				sc.beginSaveContext();
				try {
					target.persistTree(sc);
					sc.getSource().setLoaded(target, target.getCacheKey());
					for(DelayedFKDrop dfd : related) {
						dfd.getTable().persistTree(sc);
					}
					updates.addAll(sc.getSaveContext().getObjects());
				} finally {
					sc.endSaveContext();
				}
			}
			
		}

		@Override
		public List<CatalogEntity> getUpdatedObjects() throws PEException {
			return updates;
		}

		@Override
		public List<CatalogEntity> getDeletedObjects() throws PEException {
			return deletes;
		}

		@Override
		public boolean canRetry(Throwable t) {
			// we're going to say you can never retry these because of the prepare action
			return false;
		}

		@Override
		public boolean requiresFreshTxn() {
			return false;
		}

		@Override
		public String description() {
			return "create table as select";
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return record;
		}

		@Override
		public boolean requiresWorkers() {
			// TODO Auto-generated method stub
			return true;
		}

		@Override
		public void prepareNested(ExecutionState estate, CatalogDAO c,
				final WorkerGroup wg, DBResultConsumer resultConsumer)
				throws PEException {
			try {
				// we have to recreate slightly what QueryStep.executeOperation does -
				// we have to do the worker group management junk, but arrange to not 
				// return our given group (it was allocated in our parent step, which is the nested ddl step)
				int last = toExecute.size() - 1;
				for(int i = 0; i < toExecute.size(); i++) {
					QueryStepOperation qso = toExecute.get(i); 
					WorkerGroup cwg = wg;
					try {
						if (qso.requiresWorkers() && !qso.getStorageGroup().equals(wg.getGroup())) {
							cwg = estate.getConnection().getWorkerGroupAndPushContext(qso.getStorageGroup(),qso.getContextDatabase());
						}
						DBResultConsumer consumer = (i == last ? resultConsumer : DBEmptyTextResultConsumer.INSTANCE);
						if (redistOffset == i) {
							// this is always a multituple redist (for now) - indicate that the given wg is our preallocated wg
							QueryStepMultiTupleRedistOperation rd = (QueryStepMultiTupleRedistOperation) qso;
							rd.setPreallocatedTargetWorkerGroup(wg);
							qso.executeSelf(estate, cwg, consumer);
						} else {
							qso.executeSelf(estate, cwg, consumer);
						}						
					} finally {
						if (cwg != wg) {
							estate.getConnection().returnWorkerGroupAndPopContext(cwg);
						}
					}
				}
				redistCompleted = true;
			} catch (Throwable t) {
				throw new PEException(t);
			}
		}

		protected boolean isRedistComplete() {
			return redistCompleted;
		}

		@Override
		public void executeNested(ExecutionState estate, WorkerGroup wg,
				DBResultConsumer resultConsumer) throws Throwable {
			// nothing to do - this is handled elsewhere
		}
		
		public void onCommit(SSConnection conn, CatalogDAO c, WorkerGroup wg) {
			if (!target.isUserlandTemporaryTable()) return;
			pincount++;
			if (pincount == 1) {
				wg.markPinned();
				context.getTemporaryTableSchema().addTable(context, target);
				// if we already have it we won't do it again
				conn.acquireInflightTemporaryTablesLock();
			}
		}
		
		public void onRollback(SSConnection conn, CatalogDAO c, WorkerGroup wg) {
			if (!target.isUserlandTemporaryTable()) return;
			if (pincount == 1) {
				wg.clearPinned();
				pincount--;
				context.getTemporaryTableSchema().removeTable(context, target);
				if (context.getTemporaryTableSchema().isEmpty())
					conn.releaseInflightTemporaryTablesLock();
			}
		}

		
	}
	
	protected class CTATableGenerator extends TempTableGenerator {
		
		protected final SchemaContext context;
		protected final GuardFunction guard;
		
		public CTATableGenerator(SchemaContext cntxt, GuardFunction f) {
			this.context = cntxt;
			this.guard = f;
		}
		
		public UserTable createTableFromMetadata(WorkerGroup targetWG, 
				TempTableDeclHints tempHints, String tempTableName,
				PersistentDatabase database, ColumnSet metadata, DistributionModel tempDist) throws PEException {

			ComplexPETable target = (ComplexPETable) getTable();
			
			for(ColumnMetadata cm : metadata.getColumnList()) {
				UserColumn uc = new UserColumn(cm);
				if ( cm.usingAlias() ) 
					uc.setName(cm.getAliasName());
				PEColumn pec = PEColumn.build(context,uc);
				PEColumn existing = target.lookup(context,pec.getName());
				if (existing != null && existing.getType().equals(TempColumnType.TEMP_TYPE))
					existing.setType(pec.getType());
			}

			// refresh column type definitions
			target.setDeclaration(context, target);
			context.beginSaveContext();
			try {
				// need a different overload here
				return target.buildUserTableForRedist(context);
			} finally {
				context.endSaveContext();
			}
			
		}
		
		public String buildCreateTableStatement(UserTable theTable, boolean useSystemTempTable) throws PEException {
			String out = Singletons.require(HostService.class).getDBNative().getEmitter().emitCreateTableStatement(context, context.getValues(), getTable());
			return out;
		}
		
		public void addCleanupStep(SSConnection ssCon, UserTable theTable, PersistentDatabase database, WorkerGroup cleanupWG) {
			WorkerRequest wer = new WorkerExecuteRequest(
							ssCon.getNonTransactionalContext(), 
					UserTable.getDropTableStmt(ssCon, theTable.getName(), false)).onDatabase(database);
			cleanupWG.addCleanupStep(
					new ConditionalWorkerRequest(wer, guard));
			
		}		

		@Override
		public boolean requireSessVarsOnDDLGroup() {
			return true;
		}
	}

	private class InhibitTableDrop implements GuardFunction {
		
		private CreateTableViaRedistCallback cb;

		@Override
		public boolean proceed(Worker w, GroupDispatch consumer) {
			return !cb.isRedistComplete();
		}
		
		protected void setCallback(CreateTableViaRedistCallback cb) {
			this.cb = cb;
		}
		
	}
	
}
