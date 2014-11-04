package com.tesora.dve.sql.transform.strategy.insert;

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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.TransformException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.node.expression.DelegatingLiteralExpression;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.LateSortedInsert;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.schema.TriggerEvent;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.InsertStatement;
import com.tesora.dve.sql.statement.dml.ReplaceIntoValuesStatement;
import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.TransformFactory;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.InsertValuesFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.LateSortedInsertFeatureStep;
import com.tesora.dve.sql.util.ListOfPairs;

public class InsertIntoValuesPlanner extends TransformFactory {

	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext context)
			throws PEException {
		if (!(stmt instanceof InsertIntoValuesStatement))
			return null;

		InsertIntoValuesStatement iivs = (InsertIntoValuesStatement) stmt;
		
		checkForIllegalInsert(context,iivs, iivs instanceof ReplaceIntoValuesStatement);

		assertValidDupKey(context.getContext(),iivs);

		// first time planning, get the value manager to allocate values now - well, unless this is prepare - in which case not so much
		// also, we don't do it if we only have late binding constants
		if (!context.getContext().getOptions().isPrepare() && !context.getContext().getOptions().isTriggerPlanning()) 
			context.getContext().getValueManager().handleAutoincrementValues(context.getContext());
		
		return buildInsertIntoValuesFeatureStep(context,this,iivs);
	}

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.INSERT_VALUES;
	}

	public static void checkForIllegalInsert(PlannerContext pc, InsertStatement is, boolean notrigs) throws PEException {
		Table<?> table = is.getTableInstance().getTable();
		if (table.isInfoSchema())
			throw new PEException("Cannot insert into info schema table " + table.getName());
		PEAbstractTable<?> peat = is.getTableInstance().getAbstractTable();
		if (peat.isView())
			throw new PEException("No support for updatable views");
		if (notrigs && peat.asTable().hasTrigger(pc.getContext(), TriggerEvent.INSERT))
			throw new PEException("No support for trigger execution");
	}
	
	public static FeatureStep buildInsertIntoValuesFeatureStep(PlannerContext context, 
			FeaturePlanner planner, InsertIntoValuesStatement iivs) throws PEException {
		boolean requiresReferenceTimestamp = iivs.getDerivedInfo().doSetTimestampVariable();

		TableInstance intoTI = iivs.getPrimaryTable(); 
		TableKey tk = intoTI.getTableKey();
		DistributionVector dv = intoTI.getAbstractTable().getDistributionVector(context.getContext());

		FeatureStep out = null;
		
		// since we've already normalized the auto inc ids are already filled in
		if (dv.isBroadcast()) {
			// will redist to every site, no sense in even bothering with the sorting, breaking up
			// well, we still have to if this is an on dup key insert - because we need to figure out what
			// the id value is so we can set the last inserted id right
			@SuppressWarnings("unchecked")
			DistributionKey dk = new DistributionKey(tk, Collections.EMPTY_LIST, null);
			out = new InsertValuesFeatureStep(iivs,planner,tk.getAbstractTable().asTable().getStorageGroup(context.getContext()),
					dk, requiresReferenceTimestamp,iivs);
		} else if (dv.getDistributedWhollyOnTenantColumn(context.getContext()) != null 
				&& (context.getContext().getPolicyContext().isSchemaTenant() || context.getContext().getPolicyContext().isDataTenant())) {
			// so the whole table is distributed on tenant column - so just shove the whole thing down - but first
			// build a dist key
			PEColumn tenantColumn = intoTI.getAbstractTable().getTenantColumn(context.getContext());
			ListOfPairs<PEColumn,ConstantExpression> values = new ListOfPairs<PEColumn,ConstantExpression>();
			values.add(tenantColumn,context.getContext().getPolicyContext().getTenantIDLiteral(true));
			DistributionKey dk = new DistributionKey(tk, Collections.singletonList(tenantColumn), values);
			out = new InsertValuesFeatureStep(iivs,planner,tk.getAbstractTable().asTable().getStorageGroup(context.getContext()),
					dk, requiresReferenceTimestamp, iivs);
		} else {
		
			ListOfPairs<List<ExpressionNode>, DistributionKey> parts = preparePlanInsert(context, iivs);

			if (intoTI.getAbstractTable().getStorageGroup(context.getContext()).isSingleSiteGroup() || parts.size() == 1) {
				// push the whole thing down, and just use the first dist key
				out = new InsertValuesFeatureStep(iivs,planner,tk.getAbstractTable().asTable().getStorageGroup(context.getContext()),
						parts.get(0).getSecond(),requiresReferenceTimestamp, iivs);

			} else {
				final LateSortedInsert lsi = new LateSortedInsert(iivs, parts);
				context.getContext().getValueManager().registerLateSortedInsert(lsi);
				if (!context.getContext().getOptions().isPrepare())
					context.getContext().getValueManager().handleLateSortedInsert(context.getContext());

				out = new LateSortedInsertFeatureStep(iivs,planner,
						tk.getAbstractTable().asTable().getStorageGroup(context.getContext()),
						requiresReferenceTimestamp);
			}

		}
			
		return out;		
	}
	
	protected void assertValidDupKey(SchemaContext sc, InsertIntoValuesStatement iivs) {
		// we're only going to support key = key for now, and only for nonrandom tables
		// when the user says key = key, they mean that the inserted key value should win
		if (iivs.getOnDuplicateKeyEdge().size() < 1)
			return;
		DistributionVector dv = iivs.getTable().getDistributionVector(sc); 
		if (dv.isRandom())
			throw new SchemaException(Pass.NORMALIZE,"No support for on duplicate key for random distributed tables");
		// we used to have a restriction that there couldn't be more than one tuple, but I don't believe we need that
		// since we only accept basically id=id and the use of on dup key means we don't set the update count anyhow,
		// it looks like that is a silly restriction
		// if (nvals > 1)
		//	throw new PEException("No support for multivalue insert with on duplicate key");
		// last test - we only support id = id and VALUES(id).  check for that
		for(ExpressionNode expr : iivs.getOnDuplicateKeyEdge()) {
			if (EngineConstant.FUNCTION.has(expr, EngineConstant.EQUALS)) {
				FunctionCall eq = (FunctionCall) expr;
				ExpressionNode lhs = eq.getParametersEdge().get(0);
				ExpressionNode rhs = eq.getParametersEdge().get(1);
				
				if (!EngineConstant.COLUMN.has(lhs)) {
					throw new SchemaException(Pass.NORMALIZE,"No support for on duplicate key update of a non-column");
				}
				
				if (dv.contains(sc, ((ColumnInstance) lhs).getPEColumn()) && !((ColumnInstance) lhs).isSchemaEqual(rhs)) {
					throw new SchemaException(Pass.NORMALIZE,"No support for on duplicate key update of distribution column");
				}
				
				if (!EngineConstant.COLUMN.has(rhs) && !EngineConstant.FUNCTION.has(rhs) && !EngineConstant.CONSTANT.has(rhs)) {
					throw new SchemaException(Pass.NORMALIZE,"Unsupported right-hand side of on duplicate key update");
				}
				
				return;
			} else {
				// not an equals function
				break;
			}
			
		}
		// something more complex
		throw new SchemaException(Pass.NORMALIZE,"No support for on duplicate key insert value expression not of form id = id");
	}

	public static ListOfPairs<List<ExpressionNode>, DistributionKey> preparePlanInsert(PlannerContext pc, InsertIntoValuesStatement iivs) {
		SchemaContext sc = pc.getContext();
		PETable intoTable = iivs.getPrimaryTable().getAbstractTable().asTable();
		List<PEColumn> distTemplate = intoTable.getDistributionVector(sc).getDistributionTemplate(sc);
		// the user may have specified the columns in a random order, go figure out the (0 offset) position of each column in the dist template
		Map<Column<?>, Integer> columnOffset = new HashMap<Column<?>, Integer>();
		for(int i = 0; i < iivs.getColumnSpecificationEdge().size(); i++) {
			ExpressionNode e = iivs.getColumnSpecificationEdge().get(i);
			if (e instanceof ColumnInstance) {
				ColumnInstance cr = (ColumnInstance)e;
				if (distTemplate.contains(cr.getColumn()))
					columnOffset.put(cr.getColumn(), new Integer(i));
			} else {
				throw new TransformException(Pass.PLANNER,"Unrecognized expression type in insert colum specification: " + e.getClass().getName());
			}
		}
		ListOfPairs<List<ExpressionNode>, DistributionKey> ret = new ListOfPairs<List<ExpressionNode>, DistributionKey>();
		for(List<ExpressionNode> rowValues : iivs.getValues()) {
			for(ExpressionNode en : rowValues)
				en.setParent(null);
			// columns in hand, I can pull out the dv vector
			ListOfPairs<PEColumn,ConstantExpression> values = new ListOfPairs<PEColumn,ConstantExpression>();
			for(Map.Entry<Column<?>, Integer> offset : columnOffset.entrySet()) {
				ExpressionNode e = rowValues.get(offset.getValue());
				if (e instanceof ConstantExpression) {
					ConstantExpression ce = (ConstantExpression) e;
					// this should be interpreted as the sql type of the column
					if (e instanceof LiteralExpression) {
						LiteralExpression le = (LiteralExpression)e;
						if (le instanceof DelegatingLiteralExpression) {
							DelegatingLiteralExpression dle = (DelegatingLiteralExpression) le;
							sc.getValueManager().setLiteralType(dle, offset.getKey().getType());
						}
					}
					values.add((PEColumn)offset.getKey(), ce);
				} else {
					throw new TransformException(Pass.PLANNER, "Nonliteral insert value: '" + e.getClass().getName());
				}
			}
			DistributionKey dk = intoTable.getDistributionVector(sc).buildDistKey(sc, iivs.getPrimaryTable().getTableKey(), values);
			ret.add(rowValues,dk);
		}
		return ret;
	}

}
