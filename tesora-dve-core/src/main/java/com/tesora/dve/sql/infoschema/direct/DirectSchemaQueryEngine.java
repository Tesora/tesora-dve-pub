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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.engine.LogicalQuery;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ProjectingExecutionStep;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.InformationSchemaRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.ViewRewriteTransformFactory;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;

public class DirectSchemaQueryEngine {

	// shamefully copied
	static final boolean emit = Boolean.getBoolean("parser.debug");
	private static final Logger logger = Logger.getLogger(DirectSchemaQueryEngine.class);
	
	private static boolean canLog() { return emit || logger.isDebugEnabled(); }
	
	public static void log(String in) {
		if (emit)
			System.out.println(in);
		if (logger.isDebugEnabled())
			logger.debug(in);
	}

	// convenience
	public static FeatureStep buildStep(SchemaContext sc, ViewQuery vq, ProjectionInfo pi) {
		DirectLogicalQuery dlq = convertDown(sc,vq);
		return buildStep(sc,dlq,new InformationSchemaRewriteTransformFactory(),pi);
	}
	
	
	public static FeatureStep buildStep(SchemaContext sc, LogicalQuery lq, FeaturePlanner planner, final ProjectionInfo pi) {
		SelectStatement toExecute = lq.getQuery();
		
		final GenericSQLCommand gsql = toExecute.getGenericSQL(sc, Singletons.require(HostService.class).getDBNative().getEmitter(), EmitOptions.NONE.addCatalog());
				
		// look up the system group now - we have to use the actual item
		final PEPersistentGroup sg = sc.findStorageGroup(new UnqualifiedName(PEConstants.SYSTEM_GROUP_NAME));
		
		if (canLog()) {
			log("execute on " + sg.getName().getSQL());
			List<String> lines = new ArrayList<String>();
			gsql.display(sc, false, "  ", lines);
			for(String s : lines)
				log(s);
			if (pi != null) {
				for(int i = 1; i <= pi.getWidth(); i++) {
					log("[" + i + "]: " + pi.getColumnInfo(i));
				}
			} else {
				log("no projection info");
			}
		}
		
		return new ProjectingFeatureStep(null, planner, toExecute, new ExecutionCost(true,true,null,-1),
				sg,null,null,null) {
			
			@Override
			public void scheduleSelf(PlannerContext pc, ExecutionSequence es)
					throws PEException {
				
				ProjectingExecutionStep pes =
						ProjectingExecutionStep.build(
								null, sg,
							gsql);
				if (pi != null)
					pes.setProjectionOverride(pi);
				es.append(pes);		
			}

		};
	}
	
	public static DirectLogicalQuery convertDown(SchemaContext sc, ViewQuery vq) {
		SelectStatement in = vq.getQuery();
		SelectStatement copy = CopyVisitor.copy(in);
		if (canLog())
			log("Before conversion: " + copy.getSQL(sc));		
		Converter forwarder = new Converter(sc);
		forwarder.traverse(copy);
		for(Map.Entry<TableKey, TableKey> me : forwarder.getForwardedTableKeys().entrySet()) {
			copy.getDerivedInfo().removeLocalTable(me.getKey().getTable());
			copy.getDerivedInfo().addLocalTable(me.getValue());
		}
		if (canLog())
			log("After conversion: " + copy.getSQL(sc));
		// and now we can explode it
		ViewRewriteTransformFactory.applyViewRewrites(sc, copy);
		Map<String,Object> params = vq.getParams();
		if (params == null) params = new HashMap<String,Object>();
		Long anyTenant = sc.getPolicyContext().getTenantID(false);
		if (sc.getPolicyContext().isContainerContext()) {
			if (anyTenant == null) anyTenant = -1L; // global tenant
		}
		params.put(ViewInformationSchemaTable.tenantVariable,anyTenant);
		params.put(ViewInformationSchemaTable.sessid,new Long(sc.getConnection().getConnectionId()));
		new VariableConverter(params).traverse(copy);
		if (canLog())
			log("After explode: "+ copy.getSQL(sc));
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
				if (ci.getColumn() instanceof DirectInformationSchemaColumn) {
					DirectInformationSchemaColumn vc = (DirectInformationSchemaColumn) ci.getColumn();
					PEColumn backing = vc.getColumn();
					return new ColumnInstance(ci.getSpecifiedAs(),backing,map(ci.getTableInstance()));
				}
			}
			return in;
		}
		
		private TableInstance map(TableInstance in) {
			if (in.getTable().isInfoSchema()) {
				// I already know these will be view based info schema tables
				ViewInformationSchemaTable infoTab = (ViewInformationSchemaTable) in.getTable();
				TableInstance out =  new TableInstance(infoTab.getBackingView(),in.getSpecifiedAs(context),in.getAlias(),in.getNode(),false);
				if (!forwarding.containsKey(in.getTableKey()))
					forwarding.put(in.getTableKey(),out.getTableKey());
				return out;
			}
			return in;
		}

	}

	private static class VariableConverter extends Traversal {
		
		final Map<String,Object> params;
		
		public VariableConverter(Map<String,Object> params) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			this.params = params;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (in instanceof VariableInstance) {
				VariableInstance vi = (VariableInstance) in;
				String name = vi.getVariableName().getUnquotedName().get();
				Object repl = params.get(name);
				if (repl == null)
					return LiteralExpression.makeNullLiteral();
				if (repl instanceof String)
					return LiteralExpression.makeStringLiteral((String)repl);
				else
					return LiteralExpression.makeAutoIncrLiteral((Long)repl);
			}
			return in;
		}
	}
	
	
}
