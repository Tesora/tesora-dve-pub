// OS_STATUS: public
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.NameAlias;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TempTableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.InsertStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.ReplaceIntoSelectStatement;
import com.tesora.dve.sql.statement.dml.ReplaceIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.execution.CreateTempTableExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.FilterExecutionStep.LateBindingOperationFilter;
import com.tesora.dve.sql.transform.execution.LateBindingUpdateCountFilter.LateBindingUpdateCountAccumulator;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.MultiFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.NonQueryFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryFunction;

/*
 * Applies for replace into ... 
 * The general idea is:
 * [1] delete any old matching rows.  a row matches if any unique key matches any of the values.
 * [2] insert the new rows
 * 
 * The update count is the sum of the number of rows inserted and deleted.
 */
public class ReplaceIntoTransformFactory extends TransformFactory {

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.REPLACE_INTO;
	}
	
	private static LinkedHashMap<PEColumn,Integer> buildTableOffsets(SchemaContext sc, PETable theTable) {
		LinkedHashMap<PEColumn,Integer> tableOffsets = new LinkedHashMap<PEColumn,Integer>();
		for(PEKey pek : theTable.getUniqueKeys(sc)) {
			for(PEColumn c : pek.getColumns(sc)) {
				Integer offset = tableOffsets.get(c);
				if (offset != null) continue;
				offset = tableOffsets.size();
				tableOffsets.put(c, offset);
			}
		}
		return tableOffsets;
	}
	
	private static TempTable buildDeleteLookupTable(SchemaContext sc, PETable theTable, LinkedHashMap<PEColumn,Integer> tableOffsets) throws PEException {
		// now, using tableOffsets, build a new bunch of columns
		List<PEColumn> newCols = new ArrayList<PEColumn>();
		CopyContext ccForTable = new CopyContext("replaceIntoDeleteTempTable");
		for(PEColumn oc : tableOffsets.keySet()) {
			PEColumn nc = (PEColumn) oc.copy(sc,ccForTable);
			newCols.add(nc);
		}
		// tableOffsets will be the hint for the temp table
		TempTable deleteLookupTable = TempTable.buildAdHoc(sc, theTable.getPEDatabase(sc),
				newCols, DistributionVector.Model.BROADCAST, Collections.<PEColumn> emptyList(), theTable.getStorageGroup(sc),
				true);
		return deleteLookupTable;
	}
	
	private static DeleteStatement buildDeleteStatement(SchemaContext sc, PETable theTable, TempTable deleteLookupTable) {
		// I need two new table instances - a new one for the lookup table, and one for the original table
		TableInstance deleteLookupTableInstance = new TempTableInstance(sc, deleteLookupTable, new UnqualifiedName("ritit"));
		TableInstance theTableInstance = new TableInstance(theTable, theTable.getName(),
				new UnqualifiedName("tt"), sc.getNextTable(), false);
		
		ArrayList<FromTableReference> fromClauses = new ArrayList<FromTableReference>();
		fromClauses.add(new FromTableReference(theTableInstance));
		fromClauses.add(new FromTableReference(deleteLookupTableInstance));
		List<ExpressionNode> orClauses = new ArrayList<ExpressionNode>();
		// build the where clause
		for(PEKey uk : theTable.getUniqueKeys(sc)) {
			List<ExpressionNode> andClauses = new ArrayList<ExpressionNode>();
			for(PEColumn dc : uk.getColumns(sc)) {
				PEColumn lc = dc.getIn(sc, deleteLookupTable);
				if (lc == null) {
					// for the insert into select case, may not have the columns (i.e. autoinc)
					andClauses.clear();
					break;
				}
				// build the two column instances and the eq
				andClauses.add(new FunctionCall(FunctionName.makeEquals(), 
						new ColumnInstance(dc,theTableInstance), 
						new ColumnInstance(lc, deleteLookupTableInstance)));
			}
			if (andClauses.isEmpty()) continue;
			ExpressionNode anded = ExpressionUtils.safeBuildAnd(andClauses);
			anded.setGrouped();
			orClauses.add(anded);
		}
		if (orClauses.isEmpty()) return null;
		ExpressionNode wc = ExpressionUtils.safeBuildOr(orClauses);
		DeleteStatement ds = new DeleteStatement(Collections.singletonList(theTableInstance), fromClauses, wc, 
				null, null, false, new AliasInformation(),null);
		ds.getDerivedInfo().addLocalTable(theTableInstance.getTableKey());
		ds.getDerivedInfo().addLocalTable(deleteLookupTableInstance.getTableKey());
		return ds;
	}
	
	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext ipc)
			throws PEException {
		if (!(stmt instanceof InsertStatement))
			return null;
		InsertStatement is = (InsertStatement) stmt;
		if (!is.isReplace())
			return null;
		PlannerContext context = ipc.withTransform(getFeaturePlannerID());
		if (stmt instanceof ReplaceIntoValuesStatement) {
			return planValuesStatement(context, (ReplaceIntoValuesStatement)stmt);
		} else if (stmt instanceof ReplaceIntoSelectStatement) {
			return planSelectStatement(context,(ReplaceIntoSelectStatement)stmt);
		} else {
			throw new SchemaException(Pass.PLANNER, "Invalid replace statement kind: " + stmt.getClass().getSimpleName());
		}
	}
	
	private FeatureStep planValuesStatement(PlannerContext pc, ReplaceIntoValuesStatement stmt) throws PEException {
		SchemaContext sc = pc.getContext();
		// make a copy since will destroy in the process of building the delete 
		ReplaceIntoValuesStatement deleteCopy = CopyVisitor.copy(stmt);
		// offsets of columns in the insert specification
		Map<PEColumn,Integer> insertOffsets = new HashMap<PEColumn,Integer>();
		for(int i = 0; i < deleteCopy.getColumnSpecificationEdge().size(); i++) {
			ColumnInstance ci = (ColumnInstance) deleteCopy.getColumnSpecificationEdge().get(i);
			insertOffsets.put(ci.getPEColumn(), i);
		}
		PETable theTable = deleteCopy.getTable().asTable();
		LinkedHashMap<PEColumn,Integer> tableOffsets = buildTableOffsets(sc,theTable);
		final TempTable deleteLookupTable = buildDeleteLookupTable(sc,theTable,tableOffsets);
		List<Integer> keyOffsets = Functional.toList(tableOffsets.values());
		// the columns have the same names and offsets given by tableOffsets, so we just
		// need to build an insert statement of the tuples to populate the lookup table
		// and then a delete statement joined against the lookup table
		// but first, make a copy of the original stmt - we're going to steal all the tuples from it
		// for the insert
		ReplaceIntoValuesStatement insertCopy = CopyVisitor.copy(stmt);
		List<List<ExpressionNode>> keyTuples = new ArrayList<List<ExpressionNode>>();
		for(MultiEdge<InsertIntoValuesStatement,ExpressionNode> rc : insertCopy.getValuesEdge()) {
			List<ExpressionNode> tuple = rc.getMulti();
			ArrayList<ExpressionNode> keyTuple = new ArrayList<ExpressionNode>();
			keyTuples.add(keyTuple);
			for(Integer i : keyOffsets) 
				keyTuple.add(tuple.get(i));
		}
		final TableInstance lookupTablePopInstance = new TempTableInstance(sc, deleteLookupTable, new UnqualifiedName("ritit"));
		List<ExpressionNode> tempColSpec = Functional.apply(deleteLookupTable.getColumns(sc),new UnaryFunction<ExpressionNode, PEColumn>() {

			@Override
			public ExpressionNode evaluate(PEColumn object) {
				return new ColumnInstance(object, lookupTablePopInstance);
			}
			
		});
		final InsertIntoValuesStatement popInsert = new InsertIntoValuesStatement(lookupTablePopInstance,
				tempColSpec,
				keyTuples,
				null,
				new AliasInformation(),
				null);
		popInsert.getDerivedInfo().addLocalTable(lookupTablePopInstance.getTableKey());
		popInsert.setHiddenUpdateCount();
		
		DeleteStatement ds = buildDeleteStatement(sc, theTable, deleteLookupTable);
		ReplaceIntoValuesStatement finalInsertCopy = CopyVisitor.copy(stmt);
		final InsertIntoValuesStatement finalInsert = new InsertIntoValuesStatement(finalInsertCopy.getTableInstance(),finalInsertCopy.getColumnSpecification(),
				finalInsertCopy.getValues(), null, finalInsertCopy.getAliases(), null);
		finalInsert.getDerivedInfo().take(insertCopy.getDerivedInfo());

		FeatureStep deleteStep = null;
		if (ds != null)
			deleteStep = buildPlan(ds,pc,DefaultFeaturePlannerFilter.INSTANCE);
		
		FeatureStep out = new NonQueryFeatureStep(this, finalInsert, deleteCopy.getTableInstance().getTableKey(), 
				deleteCopy.getTable().getStorageGroup(sc), null) {

			public void schedule(PlannerContext sc, ExecutionSequence es, Set<FeatureStep> scheduled) throws PEException {
				if (!getSelfChildren().isEmpty()) {
					// first off create lookup table
					es.append(new CreateTempTableExecutionStep(
							getDatabase(sc),
							deleteLookupTable.getStorageGroup(sc.getContext()), 
							deleteLookupTable));
					// next we schedule the pop insert
					popInsert.plan(sc.getContext(),es);
					// this is where our magic happens
					LateBindingOperationFilter.schedule(sc, getSelfChildren().get(0), es, scheduled, new LateBindingUpdateCountAccumulator());
				}
				finalInsert.plan(sc.getContext(), es);
			}
			
			@Override
			protected void scheduleSelf(PlannerContext sc, ExecutionSequence es)
					throws PEException {
				// TODO Auto-generated method stub
				
			}
			
		}.withDefangInvariants();
		if (deleteStep != null)
			out.addChild(deleteStep);
		
		return out;
	}

	private LinkedHashMap<PEColumn,Integer> buildInsertIntoSelectTableOffsets(SchemaContext sc, ReplaceIntoSelectStatement riss) throws PEException {
		ProjectingStatement ps = riss.getSource();
		if (!(ps instanceof SelectStatement)) {
			// the issue is setting the aliases on the resulting select statement
			throw new PEException("No current support for replace into ... union");
		}
		
		// figure out whether to omit the autoinc column
		int autoIncOffset = -1;
		PEColumn autoIncColumn = null;
		for(int i = 0; i < riss.getColumnSpecificationEdge().size(); i++) {
			ColumnInstance ci = (ColumnInstance) riss.getColumnSpecificationEdge().get(i);
			PEColumn pec = ci.getPEColumn();
			if (pec.isAutoIncrement()) {
				autoIncOffset = i;
				autoIncColumn = pec;
				break;
			}
		}
		boolean omitAutoIncColumn = false;
		if (autoIncOffset == riss.getColumnSpecificationEdge().size() - 1) {
			// at the end - verify that the size of the colspec does not match the ps proj length
			if (riss.getColumnSpecificationEdge().size() > ps.getProjections().get(0).size()) {
				omitAutoIncColumn = true;
			}
		}
		
		final PETable theTable = riss.getTable().asTable();
		LinkedHashMap<PEColumn,Integer> tableOffsets = buildTableOffsets(sc,theTable);
		if (omitAutoIncColumn) {
			tableOffsets.remove(autoIncColumn);
		}
		
		return tableOffsets;
	}
	
	private Pair<TempTable,Integer> buildFirstTempTable(PlannerContext pc, ReplaceIntoSelectStatement orig, ProjectingFeatureStep pfs) throws PEException {
		CopyContext ccForTable = new CopyContext("replaceIntoResultsTempTable");
		List<PEColumn> foundColumns = new ArrayList<PEColumn>();
		int distOn = -1;
		int childLength = orig.getSource().getProjections().get(0).size();
		for(int i = 0; i < orig.getColumnSpecificationEdge().size(); i++) {
			ColumnInstance ci = (ColumnInstance) orig.getColumnSpecificationEdge().get(i);
			PEColumn pec = ci.getPEColumn();
			if (i < childLength) {
				PEColumn nc = (PEColumn)pec.copy(pc.getContext(),ccForTable);
				foundColumns.add(nc);
				if (distOn == -1 && nc.getType().isIntegralType())
					distOn = i;
			}
		}
		if (distOn == -1) distOn = 0;
		SelectStatement ss = (SelectStatement) pfs.getPlannedStatement();
		for(int i = 0; i < ss.getProjectionEdge().size(); i++) {
			if (i >= foundColumns.size()) continue;
			ExpressionNode en = ss.getProjectionEdge().get(i);
			if (en instanceof ExpressionAlias) {
				ExpressionAlias ea = (ExpressionAlias) en;
				ea.setAlias(foundColumns.get(i).getName().getUnqualified());
			}
		}
		PEStorageGroup tempGroup = pc.getTempGroupManager().getGroup(pfs.getCost().getGroupScore());
		// now we build a new temp table with the found columns 
		TempTable tempTab = TempTable.buildAdHoc(pc.getContext(), orig.getTable().asTable().getPEDatabase(pc.getContext()),
				foundColumns, DistributionVector.Model.STATIC, Collections.singletonList(foundColumns.get(distOn)),
				tempGroup,true);
		tempTab.forceDefinitions(pc.getContext());
		return new Pair<TempTable,Integer>(tempTab,distOn);
	}
	
	private InsertIntoSelectStatement buildLookupPopulation(PlannerContext pc, RedistFeatureStep tempTab, TempTable lookupTable) throws PEException {
		TempTable resultsTab = tempTab.getTargetTempTable();
		SelectStatement ss = tempTab.buildNewSelect(pc);
		TableInstance resultsTabInstance = ss.getTablesEdge().get(0).getBaseTable();
		TempTableInstance lookupTableInstance = new TempTableInstance(pc.getContext(), lookupTable, new UnqualifiedName("ltt"));
		List<ExpressionNode> colSpec = new ArrayList<ExpressionNode>();
		List<ExpressionNode> proj = new ArrayList<ExpressionNode>();
		for(PEColumn pec : lookupTable.getColumns(pc.getContext())) {
			colSpec.add(new ColumnInstance(pec, lookupTableInstance));
			PEColumn tc = pec.getIn(pc.getContext(), resultsTab);
			if (tc == null)
				throw new PEException("Lost column between lookup table and results table");
			ColumnInstance ci = new ColumnInstance(tc,resultsTabInstance);
			ExpressionAlias ea = new ExpressionAlias(ci, new NameAlias(pec.getName().getUnqualified()), false);				
			proj.add(ea);				
		}
		ss.setProjection(proj);
		InsertIntoSelectStatement iiss = new InsertIntoSelectStatement(
				lookupTableInstance,
				colSpec,
				ss,
				false,
				null,
				new AliasInformation(),null);
		iiss.getDerivedInfo().addLocalTable(lookupTableInstance.getTableKey());
		iiss.getDerivedInfo().addNestedStatements(Collections.singleton((ProjectingStatement)ss));
				
		return iiss;
		
	}
	
	private InsertIntoSelectStatement buildFinalInsert(PlannerContext pc, RedistFeatureStep tempTab, PETable theTable) throws PEException {
		TempTable resultsTab = tempTab.getTargetTempTable();
		// the columns in resultsTab have the same names as those in the target table, so 
		// build the insert into select and let the insert into select planner deal with any autoinc
		TableInstance targetTable = new TableInstance(theTable,null,new UnqualifiedName("fins"),pc.getContext().getNextTable(),false);
		TempTableInstance srcTable = new TempTableInstance(pc.getContext(), resultsTab, new UnqualifiedName("finsrc"));
		List<ExpressionNode> colSpec = new ArrayList<ExpressionNode>();
		List<ExpressionNode> proj = new ArrayList<ExpressionNode>();
		for(PEColumn c : resultsTab.getColumns(pc.getContext())) {
			proj.add(new ExpressionAlias(new ColumnInstance(c,srcTable), new NameAlias(c.getName().getUnqualified()), false));
			PEColumn tc = c.getIn(pc.getContext(), theTable);
			if (tc == null)
				throw new PEException("Unable to build final insert into select, missing target column " + c.getName().getSQL());
			colSpec.add(new ColumnInstance(tc,targetTable));
		}
		SelectStatement src = new SelectStatement(new AliasInformation());
		src.setTables(srcTable);
		src.setProjection(proj);
		src.getDerivedInfo().addLocalTable(srcTable.getTableKey());
		
		InsertIntoSelectStatement iiss = 
				new InsertIntoSelectStatement(
					targetTable,colSpec,src,
					false, null, new AliasInformation(), null);
		iiss.getDerivedInfo().addLocalTable(targetTable.getTableKey());
		iiss.getDerivedInfo().addNestedStatements(Collections.singleton((ProjectingStatement)src));
		return iiss;

	}
	
	private FeatureStep planSelectStatement(PlannerContext pc, ReplaceIntoSelectStatement riss) throws PEException {
		SchemaContext sc = pc.getContext();
		ReplaceIntoSelectStatement selectCopy = CopyVisitor.copy(riss);
		LinkedHashMap<PEColumn,Integer> tableOffsets = buildInsertIntoSelectTableOffsets(sc,selectCopy);
		final PETable theTable = riss.getTable().asTable();
		final TempTable deleteLookupTable = buildDeleteLookupTable(sc,theTable, tableOffsets);
		DeleteStatement ds = buildDeleteStatement(sc, theTable, deleteLookupTable);
		
		if (ds == null) {
			// we can just schedule this as an ordinary insert into select
			InsertIntoSelectStatement iiss = new InsertIntoSelectStatement(
					selectCopy.getTableInstance(),
					selectCopy.getColumnSpecification(),
					selectCopy.getSource(),
					selectCopy.isNestedGrouped(),
					null,
					selectCopy.getAliases(),
					null);
			return buildPlan(iiss,pc,DefaultFeaturePlannerFilter.INSTANCE);
		}
		
		// original select plan
		ProjectingFeatureStep pfs =
				(ProjectingFeatureStep) buildPlan(selectCopy.getSource(), pc, DefaultFeaturePlannerFilter.INSTANCE);
		FeatureStep dfs =
				buildPlan(ds, pc, DefaultFeaturePlannerFilter.INSTANCE);
		
		Pair<TempTable, Integer> t1 = buildFirstTempTable(pc,riss,pfs);
		
		final TempTable selectResultsTable = t1.getFirst();
		
		RedistFeatureStep redistributed = 
				new RedistFeatureStep(this,pfs,new TableKey(selectResultsTable,0), selectResultsTable.getStorageGroup(sc),
						Collections.singletonList(t1.getSecond()), null);
		
		InsertIntoSelectStatement lookupPop =
				buildLookupPopulation(pc,redistributed,deleteLookupTable);
		
		InsertIntoSelectStatement actualInsert =
				buildFinalInsert(pc,redistributed, theTable);
		
		// child 2 is the redist from the temp table to the delete lookup table
		FeatureStep lookupPopStep =
				buildPlan(lookupPop, pc, DefaultFeaturePlannerFilter.INSTANCE);
		FeatureStep actualInsertStep =
				buildPlan(actualInsert, pc, DefaultFeaturePlannerFilter.INSTANCE);

		// we have a specific scheduling order - first we build the temp table referenced in child 0
		// then we schedule child 0 (the select that is redist'd into that temp table)
		// then we create the lookup temp table
		// then we schedule child 2 to populate the lookup table
		// then we schedule child 1 to delete from the target table using the lookup table
		// then we schedule child 3 to do the actual insert

		FeatureStep out = new MultiFeatureStep(this) {
			
			@Override
			public void schedule(PlannerContext sc, ExecutionSequence es, Set<FeatureStep> scheduled) throws PEException {
				schedulePrefix(sc,es,scheduled);
				es.append(new CreateTempTableExecutionStep(theTable.getDatabase(sc.getContext()),
						selectResultsTable.getStorageGroup(sc.getContext()),
						selectResultsTable));
				getSelfChildren().get(0).schedule(sc, es, scheduled);
				es.append(new CreateTempTableExecutionStep(theTable.getDatabase(sc.getContext()),
						deleteLookupTable.getStorageGroup(sc.getContext()),
						deleteLookupTable));
				getSelfChildren().get(2).schedule(sc, es, scheduled);
				LateBindingOperationFilter.schedule(sc, getSelfChildren().get(1), es, scheduled, new LateBindingUpdateCountAccumulator());
				getSelfChildren().get(3).schedule(sc, es, scheduled);

			}
			
		};
		out.withDefangInvariants();
		out.addChild(redistributed);
		out.addChild(dfs);
		out.addChild(lookupPopStep);
		out.addChild(actualInsertStep);

		return out;
	}

	
}
