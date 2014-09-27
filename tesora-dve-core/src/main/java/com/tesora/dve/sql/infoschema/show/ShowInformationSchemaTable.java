package com.tesora.dve.sql.infoschema.show;

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
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.infoschema.InformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.ShowOptions;
import com.tesora.dve.sql.infoschema.ShowSchemaBehavior;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.computed.BackedComputedInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.computed.ComputedInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.computed.ComputedInformationSchemaTable;
import com.tesora.dve.sql.infoschema.engine.LogicalSchemaQueryEngine;
import com.tesora.dve.sql.infoschema.engine.NamedParameter;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.SchemaQueryStatement;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;

public class ShowInformationSchemaTable extends ComputedInformationSchemaTable implements ShowSchemaBehavior {

	public ShowInformationSchemaTable(
			LogicalInformationSchemaTable basedOn, UnqualifiedName viewName,
			UnqualifiedName pluralViewName, boolean isPriviledged, boolean isExtension) {
		super(InfoView.SHOW, basedOn, viewName, pluralViewName, isPriviledged, isExtension);
	}
	
	protected List<ExpressionNode> buildProjection(SchemaContext sc, TableInstance ti, boolean useExtensions, boolean hasPriviledge, AliasInformation aliases, ShowOptions opts) {
		List<ComputedInformationSchemaColumn> projCols = getProjectionColumns(useExtensions,hasPriviledge);
		ArrayList<ExpressionNode> proj = new ArrayList<ExpressionNode>();
		for(ComputedInformationSchemaColumn c : projCols) {
			ColumnInstance ci = new ColumnInstance(c,ti);
			ExpressionAlias ea = new ExpressionAlias(ci, ci.buildAlias(sc), true);
			aliases.addAlias(ea.getAlias().get());
			proj.add(ea);
		}
		
		return proj;
	}

	public Statement buildUniqueStatement(SchemaContext sc, Name objectName, ShowOptions opts) {
		return new SchemaQueryStatement(isExtension(),getName().get(),executeUniqueSelect(sc,objectName, opts));
	}
	
	public Statement buildShowPlural(SchemaContext sc, List<Name> scoping, ExpressionNode likeExpr, ExpressionNode whereExpr, ShowOptions options) {
		if (whereExpr != null) {
			return new SchemaQueryStatement(isExtension(),getName().get(),
					executeWhereSelect(sc, whereExpr, scoping, options));
		} else if (likeExpr != null) {
			LiteralExpression litex = (LiteralExpression) likeExpr;
			return new SchemaQueryStatement(isExtension(), getName().get(),
					executeLikeSelect(sc, (String)litex.getValue(sc), scoping, options));
		} else {
			return new SchemaQueryStatement(isExtension(), getName().get(),
					executeLikeSelect(sc, null, scoping, options));
		}
	}
	
	public IntermediateResultSet executeUniqueSelect(SchemaContext sc, Name onName, ShowOptions opts) {
		return LogicalSchemaQueryEngine.buildResultSet(sc, buildUniqueSelect(sc, onName, opts),null);
	}

    public ViewQuery buildUniqueSelect(SchemaContext sc, Name onName, ShowOptions opts) {
		if (requiresPriviledge() && !sc.getPolicyContext().isRoot())
			throw new SchemaException(Pass.SECOND, "You do not have permission to show " + getName().get());
		if (onName.isQualified())
			throw new SchemaException(Pass.PLANNER, "No support for qualified show");

		Column identCol = getIdentColumn();
		Object paramValue = null;
		if (!identCol.getType().isStringType()) {
			paramValue = Integer.parseInt(onName.get());
		} else {
			paramValue = onName.get();
		}
		
		TableInstance ti = new TableInstance(this,null,new UnqualifiedName("a"),sc.getNextTable(),false);
		AliasInformation ai = new AliasInformation();
		ai.addAlias("a");
		List<ExpressionNode> proj = buildProjection(sc, ti, useExtensions(sc), sc.getPolicyContext().isRoot(), ai, null);
		
		SelectStatement ss = new SelectStatement(ai)
			.setTables(ti)
			.setProjection(proj);
		
		FunctionCall fc = new FunctionCall(FunctionName.makeEquals(),new ColumnInstance(null,identCol,ti), new NamedParameter(new UnqualifiedName("exactName")));
		ss.setWhereClause(fc);

		HashMap<String,Object> params = new HashMap<String, Object>();
		params.put("exactName",paramValue);
		
		return addAdditionalFiltering(new ViewQuery(ss,params,ti));
	}

	public IntermediateResultSet executeLikeSelect(SchemaContext sc, String likeExpr, List<Name> scoping, ShowOptions options) {
		return LogicalSchemaQueryEngine.buildResultSet(sc, buildLikeSelect(sc, likeExpr, scoping, options),null);
	}

	public List<CatalogEntity> getLikeSelectEntities(SchemaContext sc, String likeExpr, List<Name> scoping, ShowOptions options) {
		return getLikeSelectEntities(sc, likeExpr, scoping, options, null);
	}
	
	public List<CatalogEntity> getLikeSelectEntities(SchemaContext sc, String likeExpr, List<Name> scoping, ShowOptions options, Boolean overrideRequiresPrivilegeValue) {
		return LogicalSchemaQueryEngine.buildCatalogEntities(sc, buildLikeSelect(sc,likeExpr,scoping, options, overrideRequiresPrivilegeValue)).getEntities();
	}
	
	public ViewQuery buildLikeSelect(SchemaContext sc, String likeExpr, List<Name> scoping, ShowOptions options) {
		return buildLikeSelect(sc, likeExpr, scoping, options, null);
	}
	
	/**
	 * @param sc
	 * @param likeExpr
	 * @param scoping
	 * @param options
	 * @param overrideRequiresPrivilegeValue
	 * @return
	 */
	public ViewQuery buildLikeSelect(SchemaContext sc, String likeExpr, List<Name> scoping, ShowOptions options, Boolean overrideRequiresPrivilegeValue) {
		if ((overrideRequiresPrivilegeValue != null) ? overrideRequiresPrivilegeValue : (requiresPriviledge() && !sc.getPolicyContext().isRoot()))
			throw new SchemaException(Pass.SECOND, "You do not have permission to show " + getPluralName().get());

		Column ident = getIdentColumn();
		TableInstance ti = new TableInstance(this,null,new UnqualifiedName("a"),(sc == null ? 0 : sc.getNextTable()),false);
		AliasInformation ai = new AliasInformation();
		ai.addAlias("a");
		List<ExpressionNode> proj = buildProjection(sc, ti,useExtensions(sc),(sc == null ? true :sc.getPolicyContext().isRoot()),ai, options);

		SelectStatement ss = new SelectStatement(ai)
			.setTables(ti).setProjection(proj);
		ss.getDerivedInfo().addLocalTable(ti.getTableKey());
		
		HashMap<String,Object> params = new HashMap<String, Object>();
		if (likeExpr != null) {
			FunctionCall fc = new FunctionCall(FunctionName.makeLike(),new ColumnInstance(null,ident,ti), 
					new NamedParameter(new UnqualifiedName("likeExpr")));
			ss.setWhereClause(fc);
			params.put("likeExpr",likeExpr);			
		}
		addSorting(ss, ti);
		handleScope(sc, ss, params, scoping);

		return addAdditionalFiltering(new ViewQuery(ss,params,ti,overrideRequiresPrivilegeValue));
	}

	public IntermediateResultSet executeWhereSelect(SchemaContext sc, ExpressionNode wc, List<Name> scoping, ShowOptions options) {
		return LogicalSchemaQueryEngine.buildResultSet(sc, buildWhereSelect(sc, wc, scoping, options),null);
	}

	public ViewQuery buildWhereSelect(SchemaContext sc, ExpressionNode wc, List<Name> scoping, ShowOptions options) {
		if (wc == null)
			return buildLikeSelect(sc, null, scoping, options);
		if (requiresPriviledge() && !sc.getPolicyContext().isRoot())
			throw new SchemaException(Pass.SECOND, "You do not have permission to show " + getPluralName().get());
		ListSet<ColumnInstance> cols = ColumnInstanceCollector.getColumnInstances(wc);
		TableInstance ti = cols.get(0).getTableInstance();
		
		derefEntities(wc);

		AliasInformation ai = new AliasInformation();
		ai.addAlias(ti.getAlias().getUnquotedName().get());
		List<ExpressionNode> proj = buildProjection(sc, ti,useExtensions(sc),sc.getPolicyContext().isRoot(),ai, options);
		SelectStatement ss = new SelectStatement(ai)
			.setTables(ti)
			.setProjection(proj)
			.setWhereClause(wc);
		
		addSorting(ss,ti);
		HashMap<String,Object> params = new HashMap<String,Object>();
		handleScope(sc, ss, params, scoping);
		
		return addAdditionalFiltering(new ViewQuery(ss,params,ti));
	}

	/**
	 * @param sc
	 * @param ss
	 * @param params
	 * @param scoping
	 */
	protected void handleScope(SchemaContext sc, SelectStatement ss, Map<String,Object> params, List<Name> scoping) {
	}
	
	protected Pair<String,String> decomposeScope(SchemaContext sc, List<Name> scoping) {
		Name tablen = scoping.get(0);
		Name dbn = (scoping.size() > 1 ? scoping.get(1) : null);
		if (dbn == null && sc != null) dbn = sc.getCurrentDatabase().getName();
		String tableName = (tablen == null ? null : tablen.get());
		String dbName = (dbn == null ? null : dbn.get());
		return new Pair<String,String>(dbName,tableName);		
	}
	
	protected ViewQuery addAdditionalFiltering(ViewQuery vq) {
		return vq;
	}
	
	// for pass through views
	protected ComputedInformationSchemaColumn passthroughView(LogicalInformationSchemaTable basis) {
		ComputedInformationSchemaColumn nameColumn = null;
		for(LogicalInformationSchemaColumn lisc : basis.getColumns(null)) {
			if (lisc == basis.getNameColumn()) {
				nameColumn = new BackedComputedInformationSchemaColumn(InfoView.SHOW, lisc, lisc.getName().getUnqualified()) {
					@Override
					public boolean isIdentColumn() { return true; }
					@Override
					public boolean isOrderByColumn() { return true; }
				}; 
				addColumn(null,nameColumn);
			} else {
				addColumn(null,new BackedComputedInformationSchemaColumn(InfoView.SHOW, lisc, lisc.getName().getUnqualified()));
			}
		}
		return nameColumn;
	}
	
	@SuppressWarnings("unchecked")
	protected IntermediateResultSet buildEmptyResultSet(SchemaContext sc) {
		ArrayList<List<InformationSchemaColumn>> proj = new ArrayList<List<InformationSchemaColumn>>();
		for(InformationSchemaColumn c : getColumns(sc)) {
			proj.add(Collections.singletonList(c));
		}
		ColumnSet cs = LogicalSchemaQueryEngine.buildProjectionMetadata(sc,proj,null);
		return new IntermediateResultSet(cs, Collections.EMPTY_LIST);
	}
	
}
