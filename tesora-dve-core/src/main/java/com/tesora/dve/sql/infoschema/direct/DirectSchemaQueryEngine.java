package com.tesora.dve.sql.infoschema.direct;

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
import java.util.Map;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.engine.LogicalQuery;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ProjectingExecutionStep;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.ViewRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;

public class DirectSchemaQueryEngine {

	public static FeatureStep buildStep(SchemaContext sc, LogicalQuery lq, ProjectionInfo pi, FeaturePlanner planner) {
		/*
		 * 	public ProjectingFeatureStep(PlannerContext pc, FeaturePlanner planner, ProjectingStatement statement, ExecutionCost cost, 
		 * 	PEStorageGroup group, DistributionKey dk, Database<?> db, DistributionVector vector) {

		 */
		SelectStatement toExecute = lq.getQuery();
		
		final String sql = toExecute.getSQL(sc, EmitOptions.NONE.addCatalog(), false);
		
		// look up the system group now - we have to use the actual item
		final PEPersistentGroup sg = sc.findStorageGroup(new UnqualifiedName(PEConstants.SYSTEM_GROUP_NAME));
		final Database pdb = toExecute.getDatabase(sc);
		
		return new ProjectingFeatureStep(null, planner, lq.getQuery(), new ExecutionCost(true,true,null,-1),
				sg,null,pdb,null) {
			
			@Override
			public void scheduleSelf(PlannerContext pc, ExecutionSequence es)
					throws PEException {
				
				ProjectingExecutionStep pes =
						ProjectingExecutionStep.build(
								pdb, sg,
								sql);
				es.append(pes);		
			}

		};
	}
	
	public static DirectLogicalQuery convertDown(SchemaContext sc, ViewQuery vq) {
		SelectStatement in = vq.getQuery();
		SelectStatement copy = CopyVisitor.copy(in);
		System.out.println("Before conversion: " + copy.getSQL(sc));
		Converter forwarder = new Converter(sc);
		forwarder.traverse(copy);
		for(Map.Entry<TableKey, TableKey> me : forwarder.getForwardedTableKeys().entrySet()) {
			copy.getDerivedInfo().removeLocalTable(me.getKey().getTable());
			copy.getDerivedInfo().addLocalTable(me.getValue());
		}
		System.out.println("After conversion: " + copy.getSQL(sc));
		// and now we can explode it
		ViewRewriteTransformFactory.applyViewRewrites(sc, copy);
		System.out.println("After explode: "+ copy.getSQL(sc));
		return new DirectLogicalQuery(vq,copy,Collections.EMPTY_MAP);
	}
	
	private static class Converter extends Traversal {
		
		final SchemaContext context;
		final Map<TableKey,TableKey> forwarding;
		
		public Converter(SchemaContext sc) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			this.context = sc;
			this.forwarding = new HashMap<TableKey,TableKey>();
		}

		public Map<TableKey,TableKey> getForwardedTableKeys() {
			return forwarding;
		}

		@Override
		public LanguageNode action(LanguageNode in) {
			if (in instanceof TableInstance) {
				return map((TableInstance)in);
			} else if (in instanceof ColumnInstance) {
				ColumnInstance ci = (ColumnInstance) in;
				DirectInformationSchemaColumn vc = (DirectInformationSchemaColumn) ci.getColumn();
				PEColumn backing = vc.getColumn();
				return new ColumnInstance(ci.getSpecifiedAs(),backing,map(ci.getTableInstance()));
			}
			return in;
		}
		
		private TableInstance map(TableInstance in) {
			if (in.getTable().isInfoSchema()) {
				// I already know these will be view based info schema tables
				DirectInformationSchemaTable infoTab = (DirectInformationSchemaTable) in.getTable();
				TableInstance out =  new TableInstance(infoTab.getBackingView(),in.getSpecifiedAs(context),in.getAlias(),in.getNode(),false);
				if (!forwarding.containsKey(in.getTableKey()))
					forwarding.put(in.getTableKey(),out.getTableKey());
				return out;
			}
			return in;
		}

	}
	
}
