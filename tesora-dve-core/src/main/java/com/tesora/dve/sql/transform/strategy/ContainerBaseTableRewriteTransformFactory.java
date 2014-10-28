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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.errmap.AvailableErrors;
import com.tesora.dve.errmap.ErrorInfo;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepDDLNestedOperation.NestedOperationDDLCallback;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.ContainerDistributionVector;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEContainer;
import com.tesora.dve.sql.schema.PEContainerTenant;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.transform.DiscriminantCollector;
import com.tesora.dve.sql.transform.KeyCollector.AndedParts;
import com.tesora.dve.sql.transform.KeyCollector.EqualityPart;
import com.tesora.dve.sql.transform.KeyCollector.OredParts;
import com.tesora.dve.sql.transform.KeyCollector.Part;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.execution.ComplexDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.NonDMLFeatureStep;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.worker.WorkerGroup;

/*
 * This transform is primarily interested with capturing changes to discriminant values and updating the base table accordingly.
 */
public class ContainerBaseTableRewriteTransformFactory extends TransformFactory {

	private boolean applies(SchemaContext sc, DMLStatement stmt)
			throws PEException {
		if (stmt instanceof UpdateStatement) {
			UpdateStatement us = (UpdateStatement) stmt;
			for(Iterator<ExpressionNode> iter = us.getUpdateExpressionsEdge().iterator(); iter.hasNext();) {
				FunctionCall fc = (FunctionCall) iter.next();
				ColumnInstance lhs = (ColumnInstance) fc.getParametersEdge().get(0);
				if (lhs.getPEColumn().isPartOfContainerDistributionVector())
					throw new SchemaException(new ErrorInfo(AvailableErrors.INVALID_CONTAINER_DISCRIMINANT_COLUMN_UPDATE,
							lhs.getPEColumn().getName().getUnquotedName().get(),
							lhs.getPEColumn().getTable().getName().getUnquotedName().get()));
			}
			return false;
		} else if (stmt instanceof DeleteStatement) {
			DeleteStatement ds = (DeleteStatement) stmt;
			PETable pet = ds.getTargetDeleteEdge().get().getAbstractTable().asTable();
			if (pet.isContainerBaseTable(sc)) {
				List<Part> parts = DiscriminantCollector.getDiscriminants(sc, ds.getWhereClause());
				if (parts == null || parts.isEmpty())
					throw new SchemaException(new ErrorInfo(AvailableErrors.INVALID_CONTAINER_DELETE,
							pet.getName().getUnquotedName().get()));
				else {
					List<SchemaCacheKey<PEContainerTenant>> matchingTenants = convert(sc, parts);
					stmt.getBlock().store(ContainerBaseTableRewriteTransformFactory.class, matchingTenants);
					return true;
				}
			}
			return false;
		}
		return false;
	}

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.CONTAINER_BASE_TABLE;
	}

	private static class DeleteContainerTenantsCallback extends NestedOperationDDLCallback {

		private List<QueryStepOperation> steps;
		private List<SchemaCacheKey<PEContainerTenant>> tenants;
		private List<CatalogEntity> deleted;
		
		public DeleteContainerTenantsCallback(List<QueryStepOperation> steps, List<SchemaCacheKey<PEContainerTenant>> tenants) {
			this.steps = steps;
			this.tenants = tenants;
		}
		
		@Override
		public List<CatalogEntity> getDeletedObjects() throws PEException {
			return deleted;
		}
		
		@Override
		public void inTxn(SSConnection conn, WorkerGroup wg) throws PEException {
			final SchemaContext sc = SchemaContext.createContext(conn);
			sc.forceMutableSource();
			List<PEContainerTenant> loaded = Functional.apply(tenants, new UnaryFunction<PEContainerTenant, SchemaCacheKey<PEContainerTenant>>() {

				@Override
				public PEContainerTenant evaluate(
						SchemaCacheKey<PEContainerTenant> object) {
					return sc.getSource().find(sc, object);
				}
				
			});
			sc.beginSaveContext();
			try {
				List<CatalogEntity> out = Functional.apply(loaded, new UnaryFunction<CatalogEntity, PEContainerTenant>() {

					@Override
					public CatalogEntity evaluate(PEContainerTenant object) {
						return object.getPersistent(sc);
					}

				});
				deleted = out;
			} finally {
				sc.endSaveContext();
			}
		}

		@Override
		public boolean canRetry(Throwable t) {
			return false;
		}

		@Override
		public String description() {
			return null;
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			ListOfPairs<SchemaCacheKey<?>,InvalidationScope> clears = new ListOfPairs<SchemaCacheKey<?>,InvalidationScope>();
			for(SchemaCacheKey<PEContainerTenant> sck : tenants) {
				clears.add(sck, InvalidationScope.CASCADE);
			}
			return new CacheInvalidationRecord(clears);
		}

		@Override
		public void executeNested(SSConnection conn, WorkerGroup wg, DBResultConsumer resultConsumer)
				throws Throwable {
			for(int i = 0; i < steps.size(); i++) {
				QueryStepOperation qso = steps.get(i);
				qso.executeSelf(conn, wg, resultConsumer);
			}			
		}

		@Override
		public void beforeTxn(SSConnection ssConn, CatalogDAO c, WorkerGroup wg) throws PEException {
			deleted = null;
		}

		@Override
		public boolean requiresFreshTxn() {
			return false;
		}

		@Override
		public boolean requiresWorkers() {
			return true;
		}

	}
	
	private static List<SchemaCacheKey<PEContainerTenant>> convert(SchemaContext sc, List<Part> parts) {
		ListSet<SchemaCacheKey<PEContainerTenant>> out = new ListSet<SchemaCacheKey<PEContainerTenant>>();
		for(Part p : parts) {
			out.addAll(convert(sc, p));
		}
		return out;
	}
	
	private static List<SchemaCacheKey<PEContainerTenant>> convert(SchemaContext sc, Part p) {
		ListSet<SchemaCacheKey<PEContainerTenant>> out = new ListSet<SchemaCacheKey<PEContainerTenant>>();
		if (p instanceof OredParts) {
			OredParts op = (OredParts) p;
			for(Part sp : op.getParts()) {
				out.addAll(convert(sc, sp));
			}
		} else if (p instanceof EqualityPart) {
			EqualityPart ep = (EqualityPart) p;
			PEColumn pec = ep.getColumn().getPEColumn();
			ConstantExpression value = ep.getLiteral();
			HashMap<PEColumn,ConstantExpression> single = new HashMap<PEColumn,ConstantExpression>();
			single.put(pec,value);
			out.add(convert(sc,single));
		} else if (p instanceof AndedParts) {
			AndedParts ap = (AndedParts) p;
			HashMap<PEColumn,ConstantExpression> multi = new HashMap<PEColumn,ConstantExpression>();
			for(Part sp : ap.getParts()) {
				EqualityPart ep = (EqualityPart) sp;
				multi.put(ep.getColumn().getPEColumn(), ep.getLiteral());
			}
			out.add(convert(sc,multi));
		}
		return out;
	}
	
	private static SchemaCacheKey<PEContainerTenant> convert(SchemaContext sc, Map<PEColumn, ConstantExpression> unorderedValues) { 
		PETable ofTab = unorderedValues.keySet().iterator().next().getTable().asTable(); 
		ContainerDistributionVector cdv = (ContainerDistributionVector) ofTab.getDistributionVector(sc);
		PEContainer cont = cdv.getContainer(sc);
		List<PEColumn> discColOrder = cont.getDiscriminantColumns(sc);
		List<Pair<PEColumn,LiteralExpression>> ordered = new ArrayList<Pair<PEColumn,LiteralExpression>>();
		for(PEColumn pec : discColOrder) {
			ConstantExpression ce = unorderedValues.get(pec);
			if (ce == null)
				throw new SchemaException(Pass.PLANNER, "Malformed discriminant key");
			ordered.add(new Pair<PEColumn,LiteralExpression>(pec,(LiteralExpression)ce));
		}
		String discValue = PEContainerTenant.buildDiscriminantValue(sc, ordered);
		return PEContainerTenant.getContainerTenantKey(cont, discValue);		
	}

	private List<SchemaCacheKey<PEContainerTenant>> getPertinentTenants(PlannerContext pc, DMLStatement stmt) throws PEException {
		SchemaContext sc = pc.getContext();
		if (stmt instanceof UpdateStatement) {
			UpdateStatement us = (UpdateStatement) stmt;
			for(Iterator<ExpressionNode> iter = us.getUpdateExpressionsEdge().iterator(); iter.hasNext();) {
				FunctionCall fc = (FunctionCall) iter.next();
				ColumnInstance lhs = (ColumnInstance) fc.getParametersEdge().get(0);
				if (lhs.getPEColumn().isPartOfContainerDistributionVector())
					throw new SchemaException(Pass.PLANNER, "Invalid update: discriminant column " 
							+ lhs.getPEColumn().getName().getSQL() 
							+ " of container base table " + lhs.getPEColumn().getTable().getName().getSQL() + " cannot be updated");
			}
		} else if (stmt instanceof DeleteStatement) {
			DeleteStatement ds = (DeleteStatement) stmt;
			PETable pet = ds.getTargetDeleteEdge().get().getAbstractTable().asTable();
			if (pet.isContainerBaseTable(sc)) {
				List<Part> parts = DiscriminantCollector.getDiscriminants(sc, ds.getWhereClause());
				if (parts == null || parts.isEmpty())
					throw new SchemaException(Pass.PLANNER, "Invalid delete on container base table "
							+ pet.getName().getSQL() 
							+ ".  Not restricted by discriminant columns");
				else {
					List<SchemaCacheKey<PEContainerTenant>> matchingTenants = convert(sc, parts);
					return matchingTenants;
				}
			}
		}
		return null;
	}
	

	
	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext pc)
			throws PEException {
		if (!applies(pc.getContext(),stmt))
			return null;
		final List<SchemaCacheKey<PEContainerTenant>> tenants =
				getPertinentTenants(pc,stmt);
		if (tenants == null)
			return null;

		final FeatureStep childStep = buildPlan(stmt, pc.withTransform(getFeaturePlannerID()), 
				DefaultFeaturePlannerFilter.INSTANCE);

		FeatureStep out = new NonDMLFeatureStep(this,childStep.getSourceGroup()) {

			public void schedule(PlannerContext sc, ExecutionSequence es, Set<FeatureStep> scheduled) throws PEException {
				schedulePrefix(sc,es,scheduled);
				if (es.getPlan() != null)
					es.getPlan().setCacheable(false);
				ExecutionSequence subseq = new ExecutionSequence(null);
				getSelfChildren().get(0).schedule(sc, subseq, scheduled);
				ArrayList<QueryStepOperation> substeps = new ArrayList<QueryStepOperation>();
				subseq.schedule(null, substeps, null, sc.getContext());
				Database<?> db = childStep.getDatabase(sc);
				
				es.append(new ComplexDDLExecutionStep((PEDatabase)db,
						childStep.getSourceGroup(),null,
						Action.DROP,
						new DeleteContainerTenantsCallback(substeps,tenants)));
			}
			
			@Override
			protected void scheduleSelf(PlannerContext sc, ExecutionSequence es)
					throws PEException {
				throw new PEException("Uncalled");
			}
			
		};
		out.addChild(childStep);
		
		return out;
	}

	
	
}
