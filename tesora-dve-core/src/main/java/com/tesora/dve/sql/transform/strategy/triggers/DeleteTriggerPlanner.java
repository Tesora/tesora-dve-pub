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
import java.util.LinkedHashMap;
import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.PETableTriggerPlanningEventInfo;
import com.tesora.dve.sql.schema.TriggerEvent;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.SchemaMapper;
import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

public class DeleteTriggerPlanner extends TriggerPlanner {

	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext context)
			throws PEException {
		DeleteStatement ds = (DeleteStatement) stmt;
		List<TableKey> deleteTabs = Functional.apply(ds.getTargetDeletes(), new UnaryFunction<TableKey,TableInstance>() {

			@Override
			public TableKey evaluate(TableInstance object) {
				return object.getTableKey();
			}
			
		});
		ListSet<PETable> triggered = getTriggeredTables(context,deleteTabs,TriggerEvent.DELETE);
		if (triggered.isEmpty()) return null;
		if (triggered.size() > 1) 
			throw new SchemaException(Pass.PLANNER,"Too many triggered tables");

		PETable subject = triggered.get(0);
		
		TableKey deleteKey = deleteTabs.get(0);
		
		PETableTriggerPlanningEventInfo triggerInfo = (PETableTriggerPlanningEventInfo) subject.getTriggers(context.getContext(), TriggerEvent.DELETE);
		
		LinkedHashMap<PEColumn,Integer> uniqueKeyOffsets = new LinkedHashMap<PEColumn,Integer>();
		
		SelectStatement srcSelect = buildTempTableSelect(context,ds,deleteKey,triggerInfo,uniqueKeyOffsets);
		
		DeleteStatement targetDelete = buildUniqueKeyDelete(context,deleteKey,uniqueKeyOffsets);

		return commonPlanning(context,deleteKey,srcSelect,targetDelete,triggerInfo); 
	}

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.DELETE_TRIGGER;
	}

	private SelectStatement buildTempTableSelect(PlannerContext context, DeleteStatement ds, TableKey triggerTable, 
			PETableTriggerPlanningEventInfo triggerInfo,
			LinkedHashMap<PEColumn,Integer> uniqueKeyOffsets) throws PEException {
		
		PEKey uk = triggerTable.getAbstractTable().asTable().getUniqueKey(context.getContext());
		if (uk == null)
			throw new PEException("No support for deleting a table with delete triggers but no unique key");
		
		ListSet<PEColumn> ukColumns = new ListSet<PEColumn>(uk.getColumns(context.getContext()));
		
		DeleteStatement copy = CopyVisitor.copy(ds);
		
		SelectStatement out = new SelectStatement(new AliasInformation())
			.setTables(copy.getTables())
			.setWhereClause(copy.getWhereClause());
		out.setOrderBy(copy.getOrderBys());
		out.setLimit(copy.getLimit());
		out.getDerivedInfo().take(copy.getDerivedInfo());
		SchemaMapper mapper = new SchemaMapper(copy.getMapper().getOriginals(), out, copy.getMapper().getCopyContext());
		out.setMapper(mapper);
		
		
		List<ExpressionNode> proj = new ArrayList<ExpressionNode>();
		Collection<ColumnKey> triggerColumns = triggerInfo.getTriggerBodyColumns(context.getContext());
		
		for(ColumnKey ck : triggerColumns) {
			int position = proj.size();
			// these can only be OLD columns
			proj.add(new ColumnInstance(ck.getPEColumn(),triggerTable.toInstance()));
			if (ukColumns.contains(ck.getPEColumn())) {
				Integer any = uniqueKeyOffsets.get(ck.getPEColumn());
				if (any == null)
					uniqueKeyOffsets.put(ck.getPEColumn(),position);
			}
		}
		
		for(PEColumn pec : ukColumns) {
			Integer any = uniqueKeyOffsets.get(pec);
			if (any == null) {
				uniqueKeyOffsets.put(pec, proj.size());
				proj.add(new ColumnInstance(pec,triggerTable.toInstance()));
			}
		}
		
		out.setProjection(proj);
		
		return out;
	}
	
	private DeleteStatement buildUniqueKeyDelete(PlannerContext context, TableKey deleteTable,
			LinkedHashMap<PEColumn,Integer> uniqueKeyOffsets) {
		FromTableReference ftr = new FromTableReference(deleteTable.toInstance());
		DeleteStatement out = new DeleteStatement(Collections.<TableInstance> emptyList(),
				Collections.singletonList(ftr),
				buildUniqueWhereClause(deleteTable,uniqueKeyOffsets),
				Collections.<SortingSpecification> emptyList(),
				null,
				false,
				new AliasInformation(),
				null);
		return out;
	}
	
}
