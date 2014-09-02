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
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.AbstractInformationSchema;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.computed.BackedComputedInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.logical.StatusLogicalInformationSchemaTable;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryPredicate;

public class StatusInformationSchemaTable extends ShowInformationSchemaTable {
	protected StatusLogicalInformationSchemaTable logicalTable = null;

	public StatusInformationSchemaTable(StatusLogicalInformationSchemaTable logicalTable) {
		super(null, logicalTable.getName().getUnqualified(), logicalTable.getName().getUnqualified(), false, false);
		
		this.logicalTable = logicalTable;
		addColumn(null,new BackedComputedInformationSchemaColumn(InfoView.SHOW, logicalTable.lookup(StatusLogicalInformationSchemaTable.NAME_COL_NAME), new UnqualifiedName(StatusLogicalInformationSchemaTable.NAME_COL_NAME)));
		addColumn(null,new BackedComputedInformationSchemaColumn(InfoView.SHOW, logicalTable.lookup(StatusLogicalInformationSchemaTable.VALUE_COL_NAME), new UnqualifiedName(StatusLogicalInformationSchemaTable.VALUE_COL_NAME)));
	}
	
	@Override
	public IntermediateResultSet executeWhereSelect(SchemaContext sc, ExpressionNode wc, List<Name> scoping,
			ShowOptions options) {
		throw new SchemaException(Pass.SECOND, "WHERE clause not supported on SHOW STATUS");
	}

	@Override
	public IntermediateResultSet executeLikeSelect(SchemaContext sc, String likeExpr, List<Name> scoping,
			ShowOptions options) {
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
		
		return new IntermediateResultSet(buildColumnSet(), rows );
	}

	private List<Pair<String,String>> getCurrentStatusValues(SchemaContext sc) throws PEException {
        return Singletons.require(HostService.class).getStatusVariables(sc.getCatalog().getDAO());
	}
	
	@Override
	protected void validate(AbstractInformationSchema ofView) {
	}

	private ColumnSet buildColumnSet() {
		LogicalInformationSchemaColumn name_lc = logicalTable.lookup(StatusLogicalInformationSchemaTable.NAME_COL_NAME);
		LogicalInformationSchemaColumn value_lc = logicalTable.lookup(StatusLogicalInformationSchemaTable.VALUE_COL_NAME);
		
		ColumnSet cs = new ColumnSet();
		cs.addColumn(name_lc.getName().getUnqualified().get(), name_lc.getDataSize(), name_lc.getType().getTypeName(), name_lc.getDataType());
		cs.addColumn(value_lc.getName().getUnqualified().get(), value_lc.getDataSize(), value_lc.getType().getTypeName(), value_lc.getDataType());

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