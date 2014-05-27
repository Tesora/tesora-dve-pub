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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.db.NativeType;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepDDLGeneralOperation.DDLCallback;
import com.tesora.dve.queryplan.QueryStepDDLNestedOperation.NestedOperationDDLCallback;
import com.tesora.dve.resultset.ColumnInfo;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.NameAlias;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.NascentPETable;
import com.tesora.dve.sql.schema.PEAbstractTable.TableCacheKey;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SQLMode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaVariables;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
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
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.behaviors.DelegatingBehaviorConfiguration;
import com.tesora.dve.sql.transform.behaviors.FeaturePlanTransformerBehavior;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultPostPlanningFeaturePlanTransformer;
import com.tesora.dve.sql.transform.execution.ComplexDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.EmptyExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
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
import com.tesora.dve.worker.MysqlTextResultCollector;
import com.tesora.dve.worker.WorkerGroup;

public class PECreateTableAsSelectStatement extends PECreateTableStatement  {

	private ProjectingStatement srcStmt;
	private ListOfPairs<PEColumn,Integer> projOffsets;
	private PEColumn unspecifiedAutoinc;
	private int specifiedAutoIncOffset;
	
	public PECreateTableAsSelectStatement(Persistable<PETable, UserTable> targ,
			Boolean ine, boolean exists, ProjectingStatement src, ListOfPairs<PEColumn,Integer> projectionOffsets) {
		super(targ, ine, exists);
		srcStmt = src;
		this.projOffsets = projectionOffsets;
		this.unspecifiedAutoinc = null;
		specifiedAutoIncOffset = -1;
	}

	public ProjectingStatement getSourceStatement() {
		return srcStmt;
	}

	@Override
	public void plan(SchemaContext pc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		// normalize the table decl as usual
		normalize(pc);
		// we may just need a single step, but we may not - figure that out
		if (alreadyExists) {
			es.append(new EmptyExecutionStep(0,"already exists - " + getSQL(pc)));
			return;
		}
		// ignore the fk junk for right now.
		ExecutionSequence subes = new ExecutionSequence(null);
		maybeDeclareDatabase(pc,subes);
		
		// TODO:
		// make this test better
		SelectStatement select = (SelectStatement) CopyVisitor.copy(srcStmt);
		
		SQLMode sqlMode = SchemaVariables.getSQLMode(pc);
		
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
		
		ArrayList<QueryStep> steps = new ArrayList<QueryStep>();
		subes.schedule(null, steps, null, pc);
		
		es.append(new ComplexDDLExecutionStep(getTable().getPEDatabase(pc),getTable().getStorageGroup(pc),getTable(),
				Action.CREATE, new CreateTableViaRedistCallback((NascentPETable)getTable(),steps,
						new CacheInvalidationRecord(getTable().getCacheKey(),InvalidationScope.LOCAL))));
		
		
		/*
		boolean immediate = !pc.isPersistent() ||
				Functional.all(related, new UnaryPredicate<DelayedFKDrop>() {

					@Override
					public boolean test(DelayedFKDrop object) {
						return object.getKeyIDs().isEmpty();
					}
					
				});
		if (immediate) {
			oneStepPlan(pc,es);
		} else {
			manyStepPlan(pc,es);
		}
		*/		
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
				ctype = BasicType.buildType("BINARY", 0, Collections.<TypeModifier> emptyList());
				modifiers.add(new DefaultValueModifier(litex));
			} else if (litex.isStringLiteral()) {
				String str = litex.asString(pc);
				ctype = BasicType.buildType("VARCHAR",str.length(),
						Arrays.asList(new TypeModifier[] { new StringTypeModifier(TypeModifierKind.CHARSET,"utf8")}));
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
					ctype = BasicType.buildType("BIGINT", displayWidth, Collections.<TypeModifier> emptyList());
				else
					ctype = BasicType.buildType("INT", displayWidth, Collections.<TypeModifier> emptyList());
				modifiers.add(notNullableModifier);
				modifiers.add(new DefaultValueModifier(LiteralExpression.makeStringLiteral("0")));				
			} else {
				throw new SchemaException(Pass.SECOND, "Unhandled literal kind for create table as select: '" + litex.toString(pc) + "'");
			}
			return PEColumn.buildColumn(pc, new UnqualifiedName(info.getAlias()), ctype, modifiers, null, Collections.<ColumnKeyModifier> emptyList());
		} else if (target instanceof ColumnInstance) {
			ColumnInstance ci = (ColumnInstance) target;
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
		} else {
			// we have to defer, use a placeholder type
			PEColumn pec = new PEColumn(pc,new UnqualifiedName(info.getAlias()), TempColumnType.TEMP_TYPE);
			return pec;
		}
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
			
			for(Pair<PEColumn,Integer> p : projOffsets) {
				ExpressionNode en = ss.getProjectionEdge().get(p.getSecond());
				PEColumn c = p.getFirst();
				if (en instanceof ExpressionAlias) {
					ExpressionAlias ea = (ExpressionAlias) en;
					ea.setAlias(c.getName().getUnqualified());
				} else {
					Edge<?,ExpressionNode> pedge = en.getParentEdge();
					ExpressionAlias ea = new ExpressionAlias(en, new NameAlias(c.getName().getUnqualified()),false);
					pedge.set(ea);
				}
			}
			
			FeatureStep out = new RedistFeatureStep(new AdhocFeaturePlanner(),
					last,new TableKey(getTable(),0),
					getTable().getStorageGroup(pc.getContext()),
					Collections.<Integer> emptyList(),
					new RedistributionFlags().withRowCount(true)
						.withAutoIncColumn(unspecifiedAutoinc)
						.withExistingAutoInc(specifiedAutoIncOffset));
			
			return defaultBehavior.getPostPlanningTransformer(pc, stmt).transform(pc, stmt, out);
		}

		
	}
	

	private static class CreateTableViaRedistCallback implements NestedOperationDDLCallback {

		private final List<QueryStep> toExecute;
		private final NascentPETable nascent;
		
		private final CacheInvalidationRecord record;
		
		private List<CatalogEntity> updates;
		private List<CatalogEntity> deletes;
		
		public CreateTableViaRedistCallback(NascentPETable tab, List<QueryStep> steps, CacheInvalidationRecord record) {
			this.nascent = tab;
			this.toExecute = steps;
			this.updates = null;
			this.deletes = null;
			this.record = record;
		}
		
		@Override
		public void beforeTxn(SSConnection conn, CatalogDAO c, WorkerGroup wg)
				throws PEException {
		}

		@Override
		public void inTxn(SSConnection conn, WorkerGroup wg) throws PEException {
			// the table would have been updated via the callback, so now we just build a new context
			// and persist out the updates
			SchemaContext sc = SchemaContext.createContext(conn);
			sc.forceMutableSource();

			updates = new ArrayList<CatalogEntity>();
			deletes = Collections.emptyList();
			
			sc.beginSaveContext();
			try {
				nascent.persistTree(sc);
				updates.addAll(sc.getSaveContext().getObjects());
			} finally {
				sc.endSaveContext();
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
		public SQLCommand getCommand(CatalogDAO c) {
			return SQLCommand.EMPTY;
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
		public void postCommitAction(CatalogDAO c) throws PEException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void executeNested(SSConnection conn, WorkerGroup wg,
				DBResultConsumer resultConsumer) throws Throwable {
			// nothing
		}

		@Override
		public void prepareNested(SSConnection conn, CatalogDAO c,
				WorkerGroup wg, DBResultConsumer resultConsumer)
				throws PEException {
			try {
				for(Iterator<QueryStep> iter = toExecute.iterator(); iter.hasNext();) {
					QueryStep qs = iter.next();
					qs.getOperation().execute(conn, wg,  
							(iter.hasNext() ? new MysqlTextResultCollector() : resultConsumer));
				}
			} catch (Throwable t) {
				throw new PEException(t);
			}
		}
		
	}
	
}
