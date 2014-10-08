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
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.infoschema.InfoView;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.ShowOptions;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.SchemaQueryStatement;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryPredicate;

public class DirectShowStatusInformation extends DirectShowSchemaTable {

	public DirectShowStatusInformation(SchemaContext sc, 
			List<PEColumn> cols,List<DirectColumnGenerator> columnGenerators) {
		super(sc, InfoView.SHOW, cols, new UnqualifiedName("status"), null, false, false,
				columnGenerators);
	}



	@Override
	public Statement buildShowPlural(SchemaContext sc, List<Name> scoping,
			ExpressionNode likeExprNode, ExpressionNode whereExpr,
			ShowOptions options) {
		if (whereExpr != null)
			throw new InformationSchemaException("show table status where ... not supported");
		String likeExpr = null;
		if (likeExprNode != null) {
			likeExpr = (String)((LiteralExpression)likeExprNode).getValue(sc);
		}
		List<ResultRow> rows = new ArrayList<ResultRow>();

		List<Pair<String, String>> sortedVarList;
		try {
			sortedVarList = getCurrentStatusValues(sc);
		} catch (PEException e) {
			throw new SchemaException(Pass.SECOND, "Exception occurred retrieveing values for Status variables", e);
		}
				
		Collections.sort(sortedVarList, new Comparator<Pair<String,String>>(){
			@Override
			public int compare(Pair<String, String> pair1, Pair<String, String> pair2) {
				return pair1.getFirst().compareTo(pair2.getFirst());
			}
		});
				
		UnaryPredicate<Pair<String,String>> pred = buildPredicate(likeExpr);
		for( Pair<String,String> varPair : sortedVarList) {
			if (!pred.test(varPair)) continue;

			ResultRow row = new ResultRow();
			row.addResultColumn(varPair.getFirst());
			row.addResultColumn(varPair.getSecond());
			rows.add(row);
		}

		return new SchemaQueryStatement(false,getName().get(),new IntermediateResultSet(buildColumnSet(), rows ));
	}

	
	
	@Override
	public Statement buildUniqueStatement(SchemaContext sc, Name objectName,
			ShowOptions opts) {
		// TODO Auto-generated method stub
		return null;
	}

	private List<Pair<String,String>> getCurrentStatusValues(SchemaContext sc) throws PEException {
        return Singletons.require(HostService.class).getStatusVariables(sc.getCatalog().getDAO());
	}

	private ColumnSet buildColumnSet() {
		ColumnSet cs = new ColumnSet();
		for(DirectInformationSchemaColumn isc : columns) {
			PEColumn pec = isc.getColumn();
			cs.addColumn(pec.getName().getUnqualified().get(),pec.getType().getSize(),pec.getType().getTypeName(), pec.getType().getDataType());
		}
		return cs;
	}

	private UnaryPredicate<Pair<String,String>> buildPredicate(String likeExpr) {
		if (likeExpr == null || "".equals(likeExpr.trim())) {
			return new UnaryPredicate<Pair<String,String>>() {

				@Override
				public boolean test(Pair<String,String> object) {
					return true;
				}
			};
		}
		
		final Pattern p = PEStringUtils.buildSQLPattern(likeExpr);
		return new UnaryPredicate<Pair<String,String>>() {

			@Override
			public boolean test(Pair<String,String> object) {
				Matcher m = p.matcher(object.getFirst().toLowerCase(Locale.ENGLISH));
				return m.matches();
			}
		};
	}

}
