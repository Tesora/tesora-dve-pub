package com.tesora.dve.sql.transform.strategy;

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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.jg.JoinGraph;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.NameAlias;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TempTableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinSpecification;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.TempTableCreateOptions;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.SchemaMapper;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.FilterExecutionStep.LateBindingOperationFilter;
import com.tesora.dve.sql.transform.execution.LateBindingUpdateCountFilter.LateBindingUpdateCounter;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.MultiFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistributionFlags;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;

/**
 * Plans for UPDATE [IGNORE] which affect distribution vector columns,
 * otherwise push down to the persistent sites.
 * 
 * UPDATE:
 * convert the update to a select
 * for updated columns, in the select use the rhs
 * for nonupdated columns, just reference
 * convert the update to a delete
 * 
 * [1] select, redist to temp
 * [2] delete
 * [3] select temp, redist to table
 * 
 * UPDATE IGNORE:
 * [1] select all matching rows and redist to temp
 * [2] execute the update statement on the temp site
 * [3] delete the matching rows from the parent
 * [4] redist back to the parent
 * 
 * Of course, if the update doesn't touch unique columns or dist key columns, we can push it down
 * unless the update is not colocated.  In that case, we must build a lookup table prior to pushing it down.
 * The plan then is
 * [1] convert the update to a select.  put any dependent expressions on the projection, along with the uk columns
 *     of the updated table.
 * [2] redist the select back onto the pers group
 * [3] execute the update using the lookup table.
 */

public class UpdateRewriteTransformFactory extends TransformFactory {

	enum Variety {
		UNIQUE,
		DIST,
		COLOCATION,
		NESTED
	}
	
	private static void validateUpdateTableInstance(final PEAbstractTable<?> updateTable) {
		if (updateTable.isView()) {
			throw new SchemaException(Pass.PLANNER, "No support for updating views");
		}
	}

	private static Set<PEKey> getUniqueKeys(final SchemaContext sc, final PETable table) {
		final Set<PEKey> uniqueKeys = new HashSet<PEKey>();
		uniqueKeys.addAll(table.getUniqueKeys(sc));
		uniqueKeys.add(table.getPrimaryKey(sc));

		return uniqueKeys;
	}

	private static Set<PEKey> getKeysWithColumn(final Set<PEKey> keys, final PEColumn column) {
		final Set<PEKey> keysWithColumn = new HashSet<PEKey>();
		for (final PEKey key : keys) {
			if (key.containsColumn(column)) {
				keysWithColumn.add(key);
			}
		}

		return keysWithColumn;
	}

	private static List<Integer> keyColumnsToOffsets(final SchemaContext sc, final PEKey key) {
		final List<PEColumn> keyColumns = key.getColumns(sc);
		final List<Integer> keyColumnOffsets = new ArrayList<Integer>(keyColumns.size());
		for (final PEColumn keyColumn : keyColumns) {
			keyColumnOffsets.add(keyColumn.getPosition());
		}

		return keyColumnOffsets;
	}

	private static List<PEColumn> getColumnsAtOffsets(final List<PEColumn> columns, final List<Integer> offsets) {
		final List<PEColumn> colsAtOffsets = new ArrayList<PEColumn>(offsets.size());
		for (final Integer i : offsets) {
			colsAtOffsets.add(columns.get(i));
		}

		return colsAtOffsets;
	}

	public static ListOfPairs<ColumnKey, ExpressionNode> getUpdateExpressions(UpdateStatement us) throws PEException {
		ListOfPairs<ColumnKey, ExpressionNode> out = new ListOfPairs<ColumnKey, ExpressionNode>();
		for (ExpressionNode en : us.getUpdateExpressionsEdge().getMulti()) {
			if (!EngineConstant.FUNCTION.has(en, EngineConstant.EQUALS))
				throw new PEException("Malformed update statement: " + en + " is not a set expression");
			FunctionCall fc = (FunctionCall) en;
			if (fc.getParametersEdge().get(0) instanceof ColumnInstance) {
				out.add(((ColumnInstance)fc.getParametersEdge().get(0)).getColumnKey(), fc.getParametersEdge().get(1));
			} else {
				throw new PEException("Malformed update statement: set expression " + en + " lhs is not a column");
			}
		}
		return out;
	}

	public static TableKey getUpdateTables(final ListOfPairs<ColumnKey, ExpressionNode> updateExprs) {
		final ListSet<TableKey> updatedTables = new ListSet<TableKey>();
		for (final Pair<ColumnKey, ExpressionNode> ue : updateExprs) {
			updatedTables.add(ue.getFirst().getTableKey());
		}

		if (updatedTables.size() != 1) {
			throw new SchemaException(Pass.PLANNER, "Unsupported: multiple update tables");
		}

		return updatedTables.get(0);
	}

	private static ExpressionNode rebuildOrDecomposedWhereClause(final ListSet<ExpressionNode> whereClauseParts) {
		if (whereClauseParts.isEmpty()) {
			return null;
		}

		return ExpressionUtils.safeBuildOr(whereClauseParts);
	}

	private static List<Column<?>> getDistributionVectorColumns(final SchemaContext sc, final PETable table) {
		final DistributionVector dv = table.getDistributionVector(sc);
		if (dv.usesColumns(sc)) {
			return new ArrayList<Column<?>>(dv.getColumns(sc));
		}

		final PEKey primary = table.getPrimaryKey(sc);
		if (primary != null) {
			return new ArrayList<Column<?>>(primary.getColumns(sc));
		}

		return new ArrayList<Column<?>>(table.getColumns(sc));
	}

	private static final List<Integer> getColumnOffsets(final List<Column<?>> columns) {
		final List<Integer> offsets = new ArrayList<Integer>(columns.size());
		for (final Column<?> column : columns) {
			offsets.add(column.getPosition());
		}

		return offsets;
	}

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.UPDATE_DIST_VECT;
	}

	private static class RHSUpdateState {
		
		private int offset;
		private ConstantExpression constant;
		
		public RHSUpdateState(int offset) {
			this.offset = offset;
			this.constant = null;
		}
		
		public RHSUpdateState(ConstantExpression ce) {
			this.offset = -1;
			this.constant = ce;
		}
		
		public ExpressionNode getExpression(SelectStatement tempTableSelect) {
			if (this.offset > -1)
				return (ExpressionNode) ExpressionUtils.getTarget(tempTableSelect.getProjectionEdge().get(offset)).copy(null);
			else
				return (ExpressionNode) constant.copy(null);
		}
		
		public int getOffset() {
			return offset;
		}
		
		public ConstantExpression getConstant() {
			return constant;
		}
	}

	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext context)
			throws PEException {
		final UpdateStatement us = (UpdateStatement) stmt;
		final SchemaContext sc = context.getContext();
		
		PEForeignKey.doForeignKeyChecks(sc, us);

		// figure out which variety of update we have
		// our search order is:
		// unique key update, dist key update, partitions, nesting

		ListSet<TableKey> updatedTables = new ListSet<TableKey>();
		EnumSet<Variety> flags = EnumSet.noneOf(Variety.class);
		for(Pair<ColumnKey,ExpressionNode> p : getUpdateExpressions(us)) {
			updatedTables.add(p.getFirst().getTableKey());
			PEColumn pec = p.getFirst().getPEColumn();
			if (pec.isUniquePart() || pec.isPrimaryKeyPart())
				flags.add(Variety.UNIQUE);
			if (pec.isPartOfDistributionVector())
				flags.add(Variety.DIST);
		}
		if (updatedTables.get(0).getAbstractTable().isView())
			throw new SchemaException(Pass.PLANNER, "No support for updatable views");
		
		if (!updatedTables.get(0).getAbstractTable().getDistributionVector(sc).usesColumns(sc)) {
			flags.remove(Variety.DIST);
			flags.remove(Variety.UNIQUE);
		}
		
		if (flags.isEmpty()) {
			PEKey uk = updatedTables.get(0).getAbstractTable().asTable().getUniqueKey(sc);
			if (EngineConstant.NESTED.hasValue(us,sc)) {
				flags.add(uk == null ? Variety.DIST : Variety.NESTED);
			} else {
				JoinGraph jg = EngineConstant.PARTITIONS.getValue(us, sc);
				if (jg.requiresRedistribution()) {
					flags.add(uk == null ? Variety.DIST : Variety.COLOCATION);				
				}
			}
		}
		if (flags.isEmpty())
			return null;
		if (updatedTables.size() > 1) 
			throw new SchemaException(Pass.PLANNER, "Unsupported: update on multiple tables");
		
		PlannerContext childContext = context.withTransform(getFeaturePlannerID());
		
		if (flags.contains(Variety.DIST) || flags.contains(Variety.UNIQUE))
			return planComplexUpdate(childContext,us);
		else
			return planSimpleUpdate(childContext,us);
	}
	
	private FeatureStep planComplexUpdate(PlannerContext pc, final UpdateStatement us) throws PEException {
		UpdateStatement updateStmtCopy = CopyVisitor.copy(us);
		SchemaContext sc = pc.getContext();
		
		final ListOfPairs<ColumnKey, ExpressionNode> updateExpressions = getUpdateExpressions(updateStmtCopy);
		final TableKey btk = getUpdateTables(updateExpressions);
		final TableInstance bti = btk.toInstance();
		final PEAbstractTable<?> bta = bti.getAbstractTable();
		validateUpdateTableInstance(bta);
		final PETable updateTable = bta.asTable();

		final Set<PEKey> uniqueKeys = getUniqueKeys(sc, updateTable);

		final Map<Integer, ExpressionNode> updateExprsByColumnOffset = new HashMap<Integer, ExpressionNode>(updateExpressions.size());
		final Set<List<Integer>> uniqueKeyColumnOffsets = new HashSet<List<Integer>>(uniqueKeys.size());
		final ListSet<ExpressionNode> whereClauseParts = ExpressionUtils.decomposeOrClause(updateStmtCopy.getWhereClause());
		for (final Pair<ColumnKey, ExpressionNode> ue : updateExpressions) {
			final PEColumn expressionColumn = ue.getFirst().getPEColumn();
			final ExpressionNode updateExpression = ue.getSecond();
			final Integer columnOffset = expressionColumn.getPosition();
			updateExprsByColumnOffset.put(columnOffset, updateExpression);
			if (expressionColumn.isUniquePart() || expressionColumn.isPrimaryKeyPart()) {
				whereClauseParts.add(new FunctionCall(FunctionName.makeEquals(), (ExpressionNode)ue.getFirst().toInstance().copy(null), updateExpression));
				final Set<PEKey> columnKeys = getKeysWithColumn(uniqueKeys, expressionColumn);
				for (final PEKey key : columnKeys) {
					uniqueKeyColumnOffsets.add(keyColumnsToOffsets(sc, key));
				}
			}
		}
		final ExpressionNode whereClause = rebuildOrDecomposedWhereClause(whereClauseParts);

		/*
		 * Here we build a redistribution select with
		 * correct column references and mappings for
		 * all required columns.
		 */
		updateStmtCopy = CopyVisitor.copy(us);
		ListSet<ExpressionNode> projs = new ListSet<ExpressionNode>();
		Set<ColumnKey> projsColKeys = new HashSet<ColumnKey>();
		
		// we need all of the columns on the base table + all columns that are needed for rhs exprs
		for(PEColumn c : updateTable.getColumns(sc)) {
			ColumnInstance ci = new ColumnInstance(c,btk.toInstance());
			projs.add(ci);
			projsColKeys.add(new ColumnKey(ci));
		}
		// add in any columns which are needed by the update exprs which aren't in the update table
		ListOfPairs<Integer,RHSUpdateState> updateOffsets = new ListOfPairs<Integer,RHSUpdateState>();
		for (final Pair<ColumnKey, ExpressionNode> uex : getUpdateExpressions(updateStmtCopy)) {
			if (EngineConstant.CONSTANT.has(uex.getSecond())) {
				updateOffsets.add(uex.getFirst().getPEColumn().getPosition(), new RHSUpdateState((ConstantExpression)uex.getSecond()));
			} else {
				updateOffsets.add(uex.getFirst().getPEColumn().getPosition(), new RHSUpdateState(projs.size()));
				projs.add(uex.getSecond());
				projsColKeys.add(uex.getFirst());
			}
		}
		for (ColumnInstance ci : ColumnInstanceCollector.getColumnInstances(updateStmtCopy.getWhereClause())) {
			ColumnKey ck = new ColumnKey(ci);
			if (!projsColKeys.contains(ck))
				projs.add(ci);
		}

		SelectStatement select = new SelectStatement(new AliasInformation())
				.setTables(updateStmtCopy.getTables())
				.setProjection(projs)
				.setWhereClause(updateStmtCopy.getWhereClause());
		select.setOrderBy(updateStmtCopy.getOrderBys());
		select.setLimit(updateStmtCopy.getLimit());
		select.getDerivedInfo().take(updateStmtCopy.getDerivedInfo());
		SchemaMapper mapper = new SchemaMapper(updateStmtCopy.getMapper().getOriginals(), select, updateStmtCopy.getMapper().getCopyContext());
		select.setMapper(mapper);
		
		select.setWhereClause(whereClause);
		select.setLocking();
		
		final SelectStatement redistStmtCopy = CopyVisitor.copy(select);
		final DeleteStatement delete = new DeleteStatement().setTruncate(false);
		delete.setTables(redistStmtCopy.getTables()).setWhereClause(redistStmtCopy.getWhereClause());
		delete.getDerivedInfo().copyTake(redistStmtCopy.getDerivedInfo());
		delete.getDerivedInfo().addNestedStatements(redistStmtCopy.getDerivedInfo().getNestedQueries(redistStmtCopy.getTables()));
		delete.getDerivedInfo().addNestedStatements(redistStmtCopy.getDerivedInfo().getNestedQueries(redistStmtCopy.getWhereClause()));
		delete.setTargetDeletes(Collections.singletonList(btk.toInstance()));

		if (emitting()) {
			emit(select.getSQL(sc, ""));
			emit(delete.getSQL(sc, ""));
		}

		ProjectingFeatureStep selectStep = 
				(ProjectingFeatureStep) buildPlan(select,pc,DefaultFeaturePlannerFilter.INSTANCE);
		FeatureStep deleteStep =
				buildPlan(delete,pc,DefaultFeaturePlannerFilter.INSTANCE);
		
		final PEStorageGroup pesg = pc.getTempGroupManager().getGroup(true);
		final List<Integer> distVect = getColumnOffsets(getDistributionVectorColumns(sc, updateTable));

		// Build a temporary table containing updated rows from the target
		// table + all rows with potential uniqueness conflicts. All records
		// have to be available on a single site.
		
		RedistFeatureStep tempTableStep =
				selectStep.redist(pc, this,
						new TempTableCreateOptions(Model.BROADCAST, pesg)
							.distributeOn(distVect),
						null,
						null);
		final TempTable tt = tempTableStep.getTargetTempTable();
		final List<PEColumn> tempTableColumns = tt.getColumns(sc);
		for (final List<Integer> offsets : uniqueKeyColumnOffsets) {
			tt.addConstraint(sc, ConstraintType.UNIQUE, getColumnsAtOffsets(tempTableColumns, offsets));
		}

		SelectStatement tempSelect = tt.buildSelect(sc);
		
		// Build and plan the temp table update statement.
		final UpdateStatement tempTableUpdate = new UpdateStatement();
		tempTableUpdate.setIgnore(us.getIgnore());
		tempTableUpdate.setAliases(new AliasInformation());			
		tempTableUpdate.setTables(tempSelect.getTables());
		final TempTableInstance tti = new TempTableInstance(sc, tt);

		/*
		 * Replace the column names from the original update expressions
		 * with their temp table counterparts.
		 */
		final List<ExpressionNode> tempTableUpdateExpressions = new ArrayList<ExpressionNode>();
		for (final Pair<Integer,RHSUpdateState> ind : updateOffsets) {
			// the mapper is undoubtedly the worst thing I ever did.  seriously.
			ColumnInstance lhs = (ColumnInstance) ExpressionUtils.getTarget(tempSelect.getProjectionEdge().get(ind.getFirst()));
			ExpressionNode rhs = ind.getSecond().getExpression(tempSelect);
			tempTableUpdateExpressions.add(new FunctionCall(FunctionName.makeEquals(),(ExpressionNode)lhs.copy(null),rhs));
		}
		tempTableUpdate.setUpdateExpressions(tempTableUpdateExpressions);

		tempTableUpdate.setWhereClause(tempSelect.getMapper().copyForward(updateStmtCopy.getWhereClause()));
		
		tempTableUpdate.getDerivedInfo().addLocalTable(tti.getTableKey());
		FeatureStep tempTableUpdateStep =
				buildPlan(tempTableUpdate,pc,DefaultFeaturePlannerFilter.INSTANCE);

		// Finally, we redist back to the target table - but at this point we strip off all the extra columns we added
		ProjectingFeatureStep selectFromTempStep =
				tempTableStep.buildNewProjectingStep(pc, this, null, null);

		tempSelect = (SelectStatement) selectFromTempStep.getPlannedStatement();
		final List<PEColumn> targCols = updateTable.getColumns(sc);
		final List<ExpressionNode> newProj = new ArrayList<ExpressionNode>();
		List<PEColumn> cols = updateTable.getColumns(sc);
		for(int i = 0; i < cols.size(); i++) {
			ExpressionNode en = tempSelect.getProjectionEdge().get(i);
			if (en instanceof ExpressionAlias) {
				final ExpressionAlias ea = (ExpressionAlias) en;
				ea.setAlias(targCols.get(i).getName().getUnqualified());
				newProj.add(ea);
			} else {
				final ExpressionAlias ea = new ExpressionAlias(en, new NameAlias(targCols.get(i).getName().getUnqualified()), false);
				newProj.add(ea);
			}				
		}
		tempSelect.setProjection(newProj);
		
		final RedistFeatureStep backToSource =
				new RedistFeatureStep(this, selectFromTempStep, btk, updateTable.getStorageGroup(sc),
						Collections.<Integer> emptyList(),
						new RedistributionFlags().withInsertIgnore());
		
		MultiFeatureStep out = new MultiFeatureStep(this) {
			
			public void schedule(PlannerContext sc, ExecutionSequence es, Set<FeatureStep> scheduled) throws PEException {
				schedulePrefix(sc,es,scheduled);
				getSelfChildren().get(0).schedule(sc, es, scheduled);
				LateBindingOperationFilter.schedule(sc, getSelfChildren().get(1), es, scheduled, new LateBindingUpdateCounter());
				for(int i = 2; i < getSelfChildren().size(); i++)
					getSelfChildren().get(i).schedule(sc,es,scheduled);
			}
			
		};
		out.withDefangInvariants();
		out.addChild(tempTableStep);
		out.addChild(tempTableUpdateStep);
		out.addChild(deleteStep);
		out.addChild(backToSource);
		
		return out;
	}

	private FeatureStep planSimpleUpdate(final PlannerContext pc, final UpdateStatement us) throws PEException {
		final SchemaContext sc = pc.getContext();
		UpdateStatement copy = CopyVisitor.copy(us);
		final ListOfPairs<ColumnKey, ExpressionNode> updateExpressions = getUpdateExpressions(copy);
		TableKey updatedTable = getUpdateTables(updateExpressions);
		List<ExpressionNode> projection = new ArrayList<ExpressionNode>();
		List<ColumnKey> pks = new ArrayList<ColumnKey>();
		PEKey pk = updatedTable.getAbstractTable().asTable().getUniqueKey(sc);
		for(PEColumn pec : pk.getColumns(sc)) {
			ColumnKey ck = new ColumnKey(updatedTable,pec);
			pks.add(ck);
			projection.add(ck.toInstance());			
		}
				
		ListOfPairs<ColumnKey,RHSUpdateState> updateOffsets = new ListOfPairs<ColumnKey,RHSUpdateState>();
		for (final Pair<ColumnKey, ExpressionNode> uex : updateExpressions) {
			if (EngineConstant.CONSTANT.has(uex.getSecond())) {
				updateOffsets.add(uex.getFirst(), new RHSUpdateState((ConstantExpression)uex.getSecond()));
			} else {
				updateOffsets.add(uex.getFirst(), new RHSUpdateState(projection.size()));
				projection.add(uex.getSecond());
			}
		}		
		SelectStatement select = new SelectStatement(new AliasInformation())
				.setTables(copy.getTables())
				.setProjection(projection)
				.setWhereClause(copy.getWhereClause());
		select.setOrderBy(copy.getOrderBys());
		select.setLimit(copy.getLimit());
		select.getDerivedInfo().take(copy.getDerivedInfo());
		SchemaMapper mapper = new SchemaMapper(copy.getMapper().getOriginals(), select, copy.getMapper().getCopyContext());
		select.setMapper(mapper);
		select.setLocking();

		ProjectingFeatureStep selectStep =
				(ProjectingFeatureStep) buildPlan(select,pc,DefaultFeaturePlannerFilter.INSTANCE);
		
		// redist bcast
		TableKey updateTable = pks.get(0).getTableKey();
		PEStorageGroup targetGroup =  updateTable.getAbstractTable().getStorageGroup(sc);
		RedistFeatureStep lookupTableStep =
			selectStep.redist(pc, this,
					new TempTableCreateOptions(Model.BROADCAST, targetGroup),
					null, 
					null);

		SelectStatement lookupTable = lookupTableStep.buildNewSelect(pc);

		List<ExpressionNode> stripped= new ArrayList<ExpressionNode>();
		for(ExpressionNode en : lookupTable.getProjectionEdge()) {
			stripped.add(ExpressionUtils.getTarget(en));
		}
		
		FromTableReference ftr = new FromTableReference(updateTable.toInstance());
		List<ExpressionNode> eqs = new ArrayList<ExpressionNode>();
		for(int i = 0; i < pks.size(); i++) {
			eqs.add(new FunctionCall(FunctionName.makeEquals(),pks.get(i).toInstance(),stripped.get(i)));
		}
		JoinedTable jt = new JoinedTable(lookupTable.getBaseTables().get(0),ExpressionUtils.safeBuildAnd(eqs),JoinSpecification.INNER_JOIN);
		ftr.addJoinedTable(jt);

		List<ExpressionNode> mappedUpdateExprs = new ArrayList<ExpressionNode>();
		for(int i = 0; i < updateOffsets.size(); i++) {
			RHSUpdateState rstate = updateOffsets.get(i).getSecond();
			ExpressionNode rhs = null;
			if (rstate.getOffset() > -1) {
				rhs = stripped.get(rstate.getOffset());
			} else {
				rhs = (ExpressionNode) rstate.getConstant().copy(null);
			}
			mappedUpdateExprs.add(new FunctionCall(FunctionName.makeEquals(),
					updateOffsets.get(i).getFirst().toInstance(),rhs));
		}
		UpdateStatement uex = new UpdateStatement();
		uex.setTables(Collections.singletonList(ftr));
		uex.setUpdateExpressions(mappedUpdateExprs);
		uex.setAliases(new AliasInformation());
		uex.getDerivedInfo().addLocalTable(updateTable,lookupTable.getBaseTables().get(0).getTableKey());
		
		FeatureStep out =
				buildPlan(uex,pc,DefaultFeaturePlannerFilter.INSTANCE);
		out.addChild(lookupTableStep);

		return out;
	}

	
}
