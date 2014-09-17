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
import java.util.TreeMap;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.errmap.DVEErrors;
import com.tesora.dve.errmap.ErrorInfo;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.infoschema.InformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.ShowSchemaBehavior;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.engine.LogicalCatalogQuery;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.infoschema.show.ShowOptions;
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

public class DirectShowSchemaTable extends DirectInformationSchemaTable implements ShowSchemaBehavior {

	// the ordering is database, table
	// thus, if we only care about database, there is only one scoped column
	private List<VariableInstance> scopedColumns;
	private final TemporaryTableHandler tempTableHandler;
	
	
	private static final String scope = "scope";
	
	public DirectShowSchemaTable(SchemaContext sc, InfoView view,
			PEViewTable viewTab, UnqualifiedName viewName, UnqualifiedName pluralViewName,
			boolean privileged, boolean extension,
			List<DirectColumnGenerator> columnGenerators,
			TemporaryTableHandler tempTableHandler) {
		super(sc, view, viewTab, viewName, pluralViewName, privileged, extension, columnGenerators);
		List<VariableInstance> vars = VariableInstanceCollector.getVariables(viewTab.getView(sc).getViewDefinition(sc, viewTab, false));
		if (!vars.isEmpty()) {
			// vars are in an expression of the form column = @var<n>, so we yank of the <n> and store the 
			TreeMap<Integer,VariableInstance> byOffset = new TreeMap<Integer,VariableInstance>();
			for(VariableInstance vi : vars) {
				String name = vi.getVariableName().getUnquotedName().get();
				String suffix = name.substring(scope.length());
				Integer offset = Integer.parseInt(suffix);
				byOffset.put(offset, vi);
			}
			scopedColumns = Functional.toList(byOffset.values());
		} else {
			scopedColumns = Collections.EMPTY_LIST;
		}
		this.tempTableHandler = tempTableHandler;
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
		SelectStatement ss = buildSkeleton(sc,ti,options);
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
			
		Map<String,Object> params = normalizeScoping(sc,scoping);
		ProjectionInfo pi = ss.getProjectionMetadata(sc);
		ViewQuery vq = new ViewQuery(ss,params,ss.getBaseTables().get(0));
		return new DirectInfoSchemaStatement(DirectSchemaQueryEngine.buildStep(sc, vq, pi));
	}

	@Override
	public Statement buildUniqueStatement(SchemaContext sc, Name objectName) {
		assertPrivilege(sc);
		TableInstance ti = new TableInstance(this,new UnqualifiedName("a"),new UnqualifiedName("a"),sc.getNextTable(),false);
		SelectStatement ss = buildSkeleton(sc,ti,null);
		FunctionCall fc = new FunctionCall(FunctionName.makeEquals(),new ColumnInstance(getIdentColumn().getName(),getIdentColumn(),ti),
				LiteralExpression.makeStringLiteral(objectName.getUnquotedName().get()));
		ss.setWhereClause(fc);
		ProjectionInfo pi = ss.getProjectionMetadata(sc);
		ViewQuery vq = new ViewQuery(ss,Collections.EMPTY_MAP,ss.getBaseTables().get(0));
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

	protected SelectStatement buildSkeleton(SchemaContext sc, TableInstance ti, ShowOptions options) {
		AliasInformation ai = new AliasInformation();
		ai.addAlias("a");
		List<ExpressionNode> proj = buildProjection(sc, ti, useExtensions(sc), sc.getPolicyContext().isRoot(), ai, options);
		
		SelectStatement ss = new SelectStatement(ai)
			.setTables(ti).setProjection(proj);
		ss.getDerivedInfo().addLocalTable(ti.getTableKey());
		return ss;
	}
	
	protected List<ExpressionNode> buildProjection(SchemaContext sc, TableInstance ti, boolean useExtensions, boolean hasPriviledge, AliasInformation aliases, ShowOptions opts) {
		List<DirectInformationSchemaColumn> projCols = getProjectionColumns(useExtensions,hasPriviledge);
		ArrayList<ExpressionNode> proj = new ArrayList<ExpressionNode>();
		for(DirectInformationSchemaColumn c : projCols) {
			ColumnInstance ci = new ColumnInstance(c,ti);
			ExpressionAlias ea = new ExpressionAlias(ci, new NameAlias(c.getName().getUnqualified()), true); 
			aliases.addAlias(ea.getAlias().get());
			proj.add(ea);
		}
		
		return proj;
	}

	public Map<String,Object> normalizeScoping(SchemaContext sc, List<Name> given) {
		if (scopedColumns.isEmpty()) return Collections.EMPTY_MAP;
		List<Name> actualScoping = given;
		if (given == null || given.isEmpty()) {
			// always
			if (sc.getCurrentDatabase(false) == null)
				throw new SchemaException(new ErrorInfo(DVEErrors.NO_DATABASE_SELECTED));
			// always add db
			actualScoping.add(sc.getCurrentDatabase(false).getName());
		} else if (scopedColumns.size() > given.size()) {
			// prefix the db
			if (sc.getCurrentDatabase(false) == null)
				throw new SchemaException(new ErrorInfo(DVEErrors.NO_DATABASE_SELECTED));
			actualScoping.add(0,sc.getCurrentDatabase(false).getName());
		}
		HashMap<String,Object> out = new HashMap<String,Object>();
		for(int i = 0; i < scopedColumns.size(); i++)
			out.put(scopedColumns.get(i).getVariableName().getUnquotedName().get(),
					actualScoping.get(i).getUnquotedName().get());
		return out;
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
				List<List<InformationSchemaColumn>> proj = 
						Functional.apply(getColumns(sc), new UnaryFunction<List<InformationSchemaColumn>,InformationSchemaColumn>() {

							@Override
							public List<InformationSchemaColumn> evaluate(
									InformationSchemaColumn object) {
								return Collections.singletonList(object);
							}
							
						});
				ColumnSet cs = LogicalCatalogQuery.buildProjectionMetadata(sc,proj,null,null);
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

}
