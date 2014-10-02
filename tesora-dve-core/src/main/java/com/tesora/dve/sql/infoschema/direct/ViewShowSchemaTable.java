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
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.InfoView;
import com.tesora.dve.sql.infoschema.InformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.ShowOptions;
import com.tesora.dve.sql.infoschema.ShowSchemaBehavior;
import com.tesora.dve.sql.infoschema.engine.LogicalSchemaQueryEngine;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.NameAlias;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.VariableInstance;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEViewTable;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.mt.IPETenant;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.SchemaQueryStatement;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.transform.VariableInstanceCollector;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.ResizableArray;
import com.tesora.dve.sql.util.UnaryFunction;

public class ViewShowSchemaTable extends ViewInformationSchemaTable implements ShowSchemaBehavior {

	private final TemporaryTableHandler tempTableHandler;

	protected boolean[] scopes = null;
	private static final String[] scopeNames = new String[] { "dbn", "tn" };

	
	public ViewShowSchemaTable(SchemaContext sc, InfoView view,
			PEViewTable viewTab, UnqualifiedName viewName, UnqualifiedName pluralViewName,
			boolean privileged, boolean extension,
			List<DirectColumnGenerator> columnGenerators,
			TemporaryTableHandler tempTableHandler) {
		super(sc, view, viewTab, viewName, pluralViewName, privileged, extension, columnGenerators);
		this.tempTableHandler = tempTableHandler;
		List<VariableInstance> vars = VariableInstanceCollector.getVariables(viewTab.getView(sc).getViewDefinition(sc, viewTab, false));
		if (!vars.isEmpty()) {
			for(VariableInstance vi : vars) {
				String name = vi.getVariableName().getUnquotedName().get();
				for(int i = 0; i < scopeNames.length; i++) {
					if (name.startsWith(scopeNames[i])) {
						if (scopes == null)
							scopes = new boolean[] { false, false };
						scopes[i] = true;
					}
				}
			}
		}

	}

	@Override
	public Statement buildShowPlural(SchemaContext sc, List<Name> scoping,
			ExpressionNode likeExpr, ExpressionNode whereExpr,
			ShowOptions options) {
		if (tempTableHandler != null) {
			Statement out = buildTempTableResults(sc,scoping,likeExpr,whereExpr,options);
			if (out != null)
				return out;
		}
		assertPrivilege(sc);
		TableInstance ti = null;
		if (whereExpr != null) {
			ListSet<ColumnInstance> cols = ColumnInstanceCollector.getColumnInstances(whereExpr);
			ti = cols.get(0).getTableInstance();
		} else {
			ti = new TableInstance(this,new UnqualifiedName("a"),new UnqualifiedName("a"),sc.getNextTable(),false);
		}
		HashMap<String,Object> params = new HashMap<String,Object>();
		SelectStatement ss = buildSkeleton(sc,ti,options,params);
		if (whereExpr != null)
			ss.setWhereClause(whereExpr);
		else if (likeExpr != null) {
			FunctionCall fc = new FunctionCall(FunctionName.makeLike(),new ColumnInstance(getIdentColumn().getName(),getIdentColumn(),ti),
					likeExpr);
			ss.setWhereClause(fc);
		}
		ResizableArray<DirectInformationSchemaColumn> obcs = getOrderByColumns();
		if (obcs.size() > 0) {
			List<SortingSpecification> sorts = new ArrayList<SortingSpecification>();
			for(int i = 0; i < obcs.size(); i++) {
				DirectInformationSchemaColumn col = obcs.get(i);
				if (col == null) continue;
				ColumnInstance ci = new ColumnInstance(col.getName(), col, ti);
				SortingSpecification sort = new SortingSpecification(ci,true);
				sort.setOrdering(true);
				sorts.add(sort);
			}
			ss.setOrderBy(sorts);
		}
			
		normalizeScoping(sc,scoping,params);
		ProjectionInfo pi = ss.getProjectionMetadata(sc);
		ViewQuery vq = new ViewQuery(ss,params,ss.getBaseTables().get(0));
		return new DirectInfoSchemaStatement(DirectSchemaQueryEngine.buildStep(sc, vq, pi));
	}

	@Override
	public Statement buildUniqueStatement(SchemaContext sc, Name objectName, ShowOptions opts) {
		assertPrivilege(sc);
		TableInstance ti = new TableInstance(this,new UnqualifiedName("a"),new UnqualifiedName("a"),sc.getNextTable(),false);
		Map<String,Object> params = new HashMap<String,Object>();
		SelectStatement ss = buildSkeleton(sc,ti,null,params);
		FunctionCall fc = new FunctionCall(FunctionName.makeEquals(),new ColumnInstance(getIdentColumn().getName(),getIdentColumn(),ti),
				LiteralExpression.makeStringLiteral(objectName.getUnquotedName().get()));
		ss.setWhereClause(fc);
		normalizeScoping(sc,null,params);
		ProjectionInfo pi = ss.getProjectionMetadata(sc);
		ViewQuery vq = new ViewQuery(ss,params,ss.getBaseTables().get(0));
		return new DirectInfoSchemaStatement(DirectSchemaQueryEngine.buildStep(sc, vq, pi));
	}

	@Override
	public List<CatalogEntity> getLikeSelectEntities(SchemaContext sc,
			String likeExpr, List<Name> scoping, ShowOptions options,
			Boolean overrideRequiresPrivilegeValue) {
		throw new InformationSchemaException("No entities available for direct tables");
	}

	public void assertPrivilege(SchemaContext sc) {
		if (!requiresPriviledge()) return;
		if (!sc.getPolicyContext().isRoot())
			throw new InformationSchemaException("You do not have permission to show " + getPluralName().get());
	}

	protected SelectStatement buildSkeleton(SchemaContext sc, TableInstance ti, ShowOptions options, Map<String,Object> params) {
		AliasInformation ai = new AliasInformation();
		ai.addAlias("a");
		List<ExpressionNode> proj = buildProjection(sc, ti, useExtensions(sc), sc.getPolicyContext().isRoot(), ai, options);
		
		SelectStatement ss = new SelectStatement(ai)
			.setTables(ti).setProjection(proj);
		ss.getDerivedInfo().addLocalTable(ti.getTableKey());
		
		if (useExtensions(sc)) {
			params.put(ViewInformationSchemaTable.metadataExtensions, 1L);
		}
		if (options != null && options.isIfNotExists())
			params.put("ine",1L);

		return ss;
	}
	
	protected List<ExpressionNode> buildProjection(SchemaContext sc, TableInstance ti, boolean useExtensions, boolean hasPriviledge, AliasInformation aliases, ShowOptions opts) {
		List<DirectInformationSchemaColumn> projCols = getProjectionColumns(useExtensions,hasPriviledge, (opts != null && opts.isFull()));
		ArrayList<ExpressionNode> proj = new ArrayList<ExpressionNode>();
		for(DirectInformationSchemaColumn c : projCols) {
			ColumnInstance ci = new ColumnInstance(c,ti);
			ExpressionAlias ea = new ExpressionAlias(ci, new NameAlias(c.getName().getUnqualified()), true); 
			aliases.addAlias(ea.getAlias().get());
			proj.add(ea);
		}
		
		return proj;
	}

	
	public interface TemporaryTableHandler {
	
		public List<ResultRow> buildResults(SchemaContext sc, TableInstance matching, ShowOptions opts, String likeExpr);

	}
	
	protected Statement buildTempTableResults(SchemaContext sc, List<Name> scoping,
			ExpressionNode likeExpr, ExpressionNode whereExpr,
			ShowOptions options) {
		if (!sc.getTemporaryTableSchema().isEmpty()) {
			Pair<String,String> scope = inferScope(sc,scoping);
			QualifiedName qn = new QualifiedName(new UnqualifiedName(scope.getFirst()), new UnqualifiedName(scope.getSecond()));
			TableInstance matching = sc.getTemporaryTableSchema().buildInstance(sc, qn);
			if (matching != null) {
				List<DirectInformationSchemaColumn> projCols = 
						getProjectionColumns(useExtensions(sc), sc.getPolicyContext().isRoot(), (options != null && options.isFull()));
				List<List<InformationSchemaColumn>> proj = 
						Functional.apply(projCols, new UnaryFunction<List<InformationSchemaColumn>,DirectInformationSchemaColumn>() {

							@Override
							public List<InformationSchemaColumn> evaluate(
									DirectInformationSchemaColumn object) {
								return Collections.singletonList((InformationSchemaColumn)object);
							}
							
						});
				ColumnSet cs = LogicalSchemaQueryEngine.buildProjectionMetadata(sc,proj,null,null);
				IntermediateResultSet irs = new IntermediateResultSet(cs,
						tempTableHandler.buildResults(sc, matching, options, (likeExpr == null ? null : (String)((LiteralExpression)likeExpr).getValue(sc))));
				return new SchemaQueryStatement(false,getName().get(),irs);
			}
		}
		return null;

	}
	
	private Pair<String,String> inferScope(SchemaContext sc, List<Name> scoping) {
		if (scoping.size() > 2)
			throw new InformationSchemaException("Overly qualified " + getPluralName() + " statement");
		if (scoping.isEmpty())
			throw new InformationSchemaException("Underly qualified " + getPluralName() + " statement");
		return decomposeScope(sc,scoping);
	}
		
	protected Pair<String,String> decomposeScope(SchemaContext sc, List<Name> scoping) {
		Name tablen = scoping.get(0);
		Name dbn = (scoping.size() > 1 ? scoping.get(1) : null);
		if (dbn == null && sc != null) dbn = sc.getCurrentDatabase().getName();
		String tableName = (tablen == null ? null : tablen.get());
		String dbName = (dbn == null ? null : dbn.get());
		return new Pair<String,String>(dbName,tableName);		
	}

	private Name getDatabaseName(SchemaContext sc) {
		IPETenant ten = sc.getCurrentTenant().get(sc);
		if (ten == null)
			return sc.getCurrentDatabase().getName();
		if (ten.getTenantID() == null || ten.getUniqueIdentifier() == null)
			return sc.getCurrentDatabase().getName();
		return new UnqualifiedName(ten.getUniqueIdentifier());
	}
	
	public void normalizeScoping(SchemaContext sc, List<Name> given, Map<String,Object> params) {
		if (scopes == null) return;
		Name[] actualScoping = new Name[] { null, null };
		if (scopes[0]) {
			// we require a database
			if (given == null || given.isEmpty()) {
				actualScoping[0] = getDatabaseName(sc); 
			} else if (given.size() < 2) {
				if (!scopes[1])
					actualScoping[0] = given.get(0);
				else
					actualScoping[0] = getDatabaseName(sc);
			} else {
				actualScoping[0] = given.get(0);
			}				
		}
		if (scopes[1]) {
			if (given == null || given.isEmpty())
				throw new InformationSchemaException("Underly qualified " + getName() + " statement");
			else if (given.size() == 2) {
				actualScoping[1] = given.get(1);
			} else {
				actualScoping[1] = given.get(0);
			}
		}
		for(int i = 0; i < scopes.length; i++) {
			if (scopes[i]) {
				params.put(scopeNames[i],actualScoping[i].getUnquotedName().get());
			}
		}
	}

}
