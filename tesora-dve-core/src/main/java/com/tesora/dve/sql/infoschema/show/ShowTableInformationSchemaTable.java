// OS_STATUS: public
package com.tesora.dve.sql.infoschema.show;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.infoschema.AbstractInformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.engine.NamedParameter;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;

public class ShowTableInformationSchemaTable extends ShowInformationSchemaTable {

	// we use, but don't expose, the database
	InformationSchemaColumnView database;
	
	public ShowTableInformationSchemaTable(
			LogicalInformationSchemaTable basedOn, UnqualifiedName viewName,
			UnqualifiedName pluralViewName, boolean isPriviledged, boolean isExtension) {
		super(basedOn, viewName, pluralViewName, isPriviledged, isExtension);
		database = null;
	}

	@Override
	protected void validate(SchemaView ofView) {
		super.validate(ofView);
		database = buildDatabaseColumn(ofView,getLogicalTable(),this);
	}

	public static InformationSchemaColumnView buildDatabaseColumn(SchemaView ofView, LogicalInformationSchemaTable tableTable, ShowInformationSchemaTable owner) {
		InformationSchemaColumnView database = 
			new InformationSchemaColumnView(InfoView.SHOW, tableTable.lookup("database"), new UnqualifiedName("database"));
		database.setTable(owner);
		// not correct
		database.setPosition(100);
		database.prepare(ofView, owner, null);
		database.freeze();
		return database;
	}
	
	private List<Name> buildScoping(List<Name> given, SchemaContext sc) {
		List<Name> actualScoping = given;
		if ((given == null || given.isEmpty())) {
			if (sc.getCurrentDatabase(false) == null)
				throw new SchemaException(Pass.PLANNER, "Unable to show tables (unqualified) with no current database");
			actualScoping = new ArrayList<Name>();
			actualScoping.add(sc.getCurrentDatabase(false).getName());
		}
		return actualScoping;
	}

	@Override
	public ViewQuery buildWhereSelect(SchemaContext sc, ExpressionNode wc, List<Name> scoping, ShowOptions options) {
		return super.buildWhereSelect(sc, wc, buildScoping(scoping,sc),options);
	}
	
	@Override
	public ViewQuery buildLikeSelect(SchemaContext sc, String likeExpr, List<Name> scoping, ShowOptions options) {
		return super.buildLikeSelect(sc, likeExpr, buildScoping(scoping,sc),options);
	}

	@Override
	public ViewQuery buildUniqueSelect(SchemaContext sc, Name onName) {
		QualifiedName actual = null;
		if (onName.isQualified())
			actual = (QualifiedName)onName;
		else {
			// qualify to the current database, else error
			actual =(QualifiedName)onName.postfix(sc.getCurrentDatabase(true).getName().getUnqualified());
		}
		List<UnqualifiedName> parts = actual.getParts();
		List<Name> scoping = Functional.apply(parts, new UnaryFunction<Name,UnqualifiedName>() {

			@Override
			public Name evaluate(UnqualifiedName object) {
				return object;
			}
			
		});
		// remove the last item - that will be our like expression
		Name last = scoping.remove(scoping.size() - 1);
		return buildLikeSelect(sc,last.get(),scoping,null);
	}
	
	@Override
	protected void handleScope(SchemaContext sc, SelectStatement ss, Map<String,Object> params, List<Name> scoping) {
		if (scoping.size() > 1)
			throw new SchemaException(Pass.SECOND, "Overly qualified show tables statement");
		TableInstance ti = ss.getBaseTables().get(0);
		Name dbn = scoping.get(0);
		ExpressionNode wc = ss.getWhereClause();
		List<ExpressionNode> decomp = ExpressionUtils.decomposeAndClause(wc);
		FunctionCall tnc = new FunctionCall(FunctionName.makeEquals(),database.buildNameTest(ti),new NamedParameter(new UnqualifiedName("encdb")));
		decomp.add(tnc);
		params.put("encdb",dbn.get());
		ss.setWhereClause(ExpressionUtils.safeBuildAnd(decomp));		
	}
	
	@Override
	public IntermediateResultSet executeWhereSelect(SchemaContext sc,
			ExpressionNode wc, List<Name> scoping, ShowOptions options) {
		IntermediateResultSet irs = super.executeWhereSelect(sc, wc, scoping,
				options);

		return irs;

	}

	@Override
	public IntermediateResultSet executeLikeSelect(SchemaContext sc,
			String likeExpr, List<Name> scoping, ShowOptions options) {
		IntermediateResultSet irs = super.executeLikeSelect(sc, likeExpr,
				scoping, options);

		return irs;

	}

	// take into account additional filtering
	protected List<ExpressionNode> buildProjection(SchemaContext sc, TableInstance ti, boolean useExtensions, boolean hasPriviledge, AliasInformation aliases, ShowOptions opts) {
		List<AbstractInformationSchemaColumnView> projCols = getProjectionColumns(useExtensions,hasPriviledge);
		ArrayList<ExpressionNode> proj = new ArrayList<ExpressionNode>();
		for(AbstractInformationSchemaColumnView c : projCols) {
			if (c.getName().get().equals(ShowSchema.Table.TYPE) && (opts == null || !opts.isFull())) 
				continue;
			ColumnInstance ci = new ColumnInstance(c,ti);
			ExpressionAlias ea = new ExpressionAlias(ci, ci.buildAlias(sc), true);
			aliases.addAlias(ea.getAlias().get());
			proj.add(ea);
		}
		
		return proj;
	}


	
}
