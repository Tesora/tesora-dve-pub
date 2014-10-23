package com.tesora.dve.sql.node;

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

import java.util.EnumSet;

public enum EdgeName implements IEdgeName {

	TABLES("from_clause"),
	PROJECTION("projection"),
	DELETE_TABLE("target_delete"),
	WHERECLAUSE("where_clause"),
	ORDERBY("orderby_clause"),
	GROUPBY("groupby_clause"),
	UPDATE_EXPRS("update_expressions"),
	LIMIT("limit_clause"),
	HAVING("having_clause"),
	INSERT_COLUMN_SPEC("column_spec"),
	INSERT_DUPKEY("dupkey"),
	UNION_FROM("union_from"),
	UNION_TO("union_to"),

	
	CASE_TESTCLAUSE("case_test"),
	CASE_ELSECLAUSE("case_else"),
	CASE_WHENCLAUSE("case_when"),

	EXPRESSION_ALIAS_TARGET("expr_alias_target"),
	ALIAS_REFERENCE("alias_reference"),
	EXPRESSION_SET_VALUE("expr_set_value"),
	FUNCTION_PARAMS("fun_params"),
	INTERVAL_TARGET("interval_target"),
	SUBQUERY("subquery"),
	JOIN_BASE("join_base"),
	JOIN_JOINS("join_joins"),
	JOIN_TO("join_to"),
	JOIN_ON("join_on"),
	FROM_TARGET("from_target"),
	WHEN_TEST("when_test"),
	WHEN_RESULT("when_result"),
	SORT_TARGET("sort_target"),
	LIMIT_OFFSET("limit_offset"),
	LIMIT_ROWCOUNT("limit_rowcount"),
	
	// compound statement edges
	COMPOUND_STATEMENTS("compound_statements"),
	CASE_STMT_TESTCLAUSE("case_stmt_test"),
	CASE_STMT_ELSECLAUSE("case_stmt_else"),
	CASE_STMT_WHENCLAUSE("case_stmt_when"),
	
	STMT_WHEN_TEST("stmt_when_test"),
	STMT_WHEN_RESULT("stmt_when_result"),
	
	// this is for the insert tuples, and is used ONLY in the bad char parameterization.
	// DO NOT USE for anything else.
	INSERT_MULTIVALUE("donotselect");
	
	private final String name;
	
	private EdgeName(String n) {
		name = n;
	}
	
	@Override
	public String getName() {
		return name;
	}
	
	@Override
	public IEdgeName getBase() {
		return this;
	}
	
	@Override
	public boolean matches(IEdgeName in) {
		if (in.isOffset()) return false;
		return this == in;
	}

	@Override
	public boolean any(EnumSet<EdgeName> set) {
		return set.contains(this);
	}
	
	@Override
	public boolean baseMatches(IEdgeName in) {
		return this == in.getBase();
	}
	
	@Override
	public boolean isOffset() {
		return false;
	}
	
	@Override
	public OffsetEdgeName makeOffset(int i) {
		return new OffsetEdgeName(this, i);
	}
	
	@Override
	public String toString() {
		return name;
	}
}
