package com.tesora.dve.sql.transform.strategy.triggers;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.TriggerValueHandler;
import com.tesora.dve.queryplan.TriggerValueHandlers;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.expression.TriggerTableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LateBindingConstantExpression;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PETableTriggerPlanningEventInfo;
import com.tesora.dve.sql.schema.TriggerEvent;
import com.tesora.dve.sql.schema.TriggerTime;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.SchemaMapper;
import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.UpdateRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;

public class UpdateTriggerPlanner extends TriggerPlanner {

	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext context)
			throws PEException {
		UpdateStatement us = (UpdateStatement) stmt;
		ListOfPairs<ColumnKey,ExpressionNode> updateExprs = UpdateRewriteTransformFactory.getUpdateExpressions(us);
		final TableKey updateTable = UpdateRewriteTransformFactory.getUpdateTables(updateExprs);
		PETableTriggerPlanningEventInfo triggerInfo = getTriggerInfo(context,updateTable, TriggerEvent.UPDATE);
		if (triggerInfo == null)
			return null;

		LinkedHashMap<ColumnKey,Integer> updateExprOffsets = new LinkedHashMap<ColumnKey,Integer>();
		LinkedHashMap<PEColumn,Integer> uniqueKeyOffsets = new LinkedHashMap<PEColumn,Integer>();
		
		Pair<TriggerValueHandlers,SelectStatement> srcSelect = buildTempTableSelect(context,us,updateTable,triggerInfo,updateExprOffsets, uniqueKeyOffsets);
		UpdateStatement uniqueKeyUpdate = buildUniqueKeyUpdate(context, updateTable, updateExprOffsets, uniqueKeyOffsets);

        return commonPlanning(context,updateTable,srcSelect.getSecond(),srcSelect.getFirst(),uniqueKeyUpdate,triggerInfo);		
	}

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.UPDATE_TRIGGER;
	}

	private Pair<TriggerValueHandlers,SelectStatement> buildTempTableSelect(PlannerContext pc, UpdateStatement us,  
			TableKey updatedTable, PETableTriggerPlanningEventInfo triggerInfo,
			Map<ColumnKey,Integer> updateExprOffsets, Map<PEColumn,Integer> ukOffsets) throws PEException {

		// figure out the unique key we're going to use
		PEKey uk = updatedTable.getAbstractTable().asTable().getUniqueKey(pc.getContext());
		if (uk == null) 
			throw new PEException("No support for updating a table with update triggers but no unique key");
		ListSet<PEColumn> ukColumns = new ListSet<PEColumn>(uk.getColumns(pc.getContext()));
		
		UpdateStatement copy = CopyVisitor.copy(us);
		ListOfPairs<ColumnKey,ExpressionNode> updateExprs = UpdateRewriteTransformFactory.getUpdateExpressions(copy);

		SelectStatement out = new SelectStatement(new AliasInformation())
		.setTables(copy.getTables())
		.setWhereClause(copy.getWhereClause());
		out.setOrderBy(copy.getOrderBys());
		out.setLimit(copy.getLimit());
		out.getDerivedInfo().take(copy.getDerivedInfo());
		SchemaMapper mapper = new SchemaMapper(copy.getMapper().getOriginals(), out, copy.getMapper().getCopyContext());
		out.setMapper(mapper);
		
		// key is the trigger column key, value is the original column key
		HashMap<ColumnKey,ColumnKey> updateKeyForwarding = new HashMap<ColumnKey,ColumnKey>();
		// key is orginal column key, value is update expression
		LinkedHashMap<ColumnKey,ExpressionNode> updateExprMap = new LinkedHashMap<ColumnKey,ExpressionNode>();
		for(Pair<ColumnKey,ExpressionNode> p : updateExprs) {
			ColumnKey triggerColumnKey = new ColumnKey(new TriggerTableKey(updatedTable.getTable(),-1,TriggerTime.AFTER),p.getFirst().getPEColumn());
			updateKeyForwarding.put(triggerColumnKey,p.getFirst());
			updateExprMap.put(p.getFirst(),p.getSecond());
		}
		List<ExpressionNode> proj = new ArrayList<ExpressionNode>();
		List<TriggerValueHandler> types = new ArrayList<TriggerValueHandler>();
		
		Collection<ColumnKey> triggerColumns = triggerInfo.getTriggerBodyColumns(pc.getContext());
		for(ColumnKey ck : triggerColumns) {
			int position = proj.size();
			boolean isKeyPart = false;
			TriggerTableKey ttk = (TriggerTableKey) ck.getTableKey();
			if (ttk.getTime() == TriggerTime.BEFORE) {
				isKeyPart = ukColumns.contains(ck.getPEColumn());
				proj.add(new ColumnInstance(ck.getPEColumn(),updatedTable.toInstance()));
				types.add(new TriggerValueHandler(ck.getPEColumn().getType()));
			} else {
				ColumnKey origCK = updateKeyForwarding.get(ck);
				if (origCK != null) {
					ExpressionNode en = updateExprMap.remove(origCK);
					updateExprOffsets.put(origCK,position);
					proj.add(en);
				} else {
					// not updated, so just take the before image
					isKeyPart = ukColumns.contains(ck.getPEColumn());
					proj.add(new ColumnInstance(ck.getPEColumn(),updatedTable.toInstance()));
				}
				types.add(new TriggerValueHandler(ck.getPEColumn().getType()));
			}
			if (isKeyPart) {
				Integer any = ukOffsets.get(ck.getPEColumn());
				if (any == null) 
					ukOffsets.put(ck.getPEColumn(), position);
			}
		}
		
		// proj now has the trigger columns, left to right
		// add any unreferenced update exprs
		for(Map.Entry<ColumnKey, ExpressionNode> me : updateExprMap.entrySet()) {
			updateExprOffsets.put(me.getKey(), proj.size());
			proj.add(me.getValue());
			types.add(new TriggerValueHandler(me.getKey().getPEColumn().getType()));
		}
		
		// finally, if there are any parts of the unique key which are missing, add those too
		for(PEColumn pec : ukColumns) {
			Integer any = ukOffsets.get(pec);
			if (any == null) {
				ukOffsets.put(pec, proj.size());
				proj.add(new ColumnInstance(pec,updatedTable.toInstance()));
				types.add(new TriggerValueHandler(pec.getType()));
			}
		}

		out.setProjection(proj);
		
		return new Pair<TriggerValueHandlers,SelectStatement>(new TriggerValueHandlers(types),out);
	}
	
	private UpdateStatement buildUniqueKeyUpdate(PlannerContext context, TableKey updatedTable, LinkedHashMap<ColumnKey,Integer> updateExprOffsets,
			LinkedHashMap<PEColumn,Integer> uniqueKeyOffsets) {
		// build a new table key for this thing
		List<ExpressionNode> updateExprs = new ArrayList<ExpressionNode>();
		for(Map.Entry<ColumnKey,Integer> me : updateExprOffsets.entrySet()) {
			FunctionCall eq = new FunctionCall(FunctionName.makeEquals(),me.getKey().toInstance(),
					new LateBindingConstantExpression(me.getValue(),me.getKey().getColumn().getType()));
			updateExprs.add(eq);
		}

		FromTableReference ftr = new FromTableReference(updatedTable.toInstance());
		UpdateStatement out = new UpdateStatement(Collections.singletonList(ftr),
				updateExprs,
				buildUniqueWhereClause(updatedTable,uniqueKeyOffsets),
				Collections.<SortingSpecification> emptyList(),
				null,
				new AliasInformation(),
				null);
		return out;
	}	
}
