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

import java.util.Collection;

import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.JoinSpecification;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.util.ListSet;

/**
 * The NATURAL [LEFT] JOIN of two tables is defined to be semantically
 * equivalent to an INNER JOIN or a LEFT JOIN with a USING clause that names all
 * columns that exist in both tables.
 * 
 * @see http://dev.mysql.com/doc/refman/5.6/en/join.html
 */
public class NaturalJoinRewriter {

	public static ListSet<JoinedTable> collectNaturalJoins(final Collection<JoinedTable> joins) {
		final ListSet<JoinedTable> naturalJoins = new ListSet<JoinedTable>();
		for (final JoinedTable join : joins) {
			if (join.getJoinType().isNaturalJoin()) {
				naturalJoins.add(join);
			}
		}

		return naturalJoins;
	}

	public static void rewriteToInnerJoin(final SchemaContext sc, final TableInstance base, final JoinedTable join) {
		final TableInstance target = join.getJoinedToTable();

		join.setUsingColSpec(collectNaturalJoinColumnNames(sc, base, target));

		final JoinSpecification originalJoinKind = join.getJoinType();
		if (originalJoinKind.isInnerJoin()) {
			join.setJoinType(JoinSpecification.INNER_JOIN);
		} else if (originalJoinKind.isLeftOuterJoin()) {
			join.setJoinType(JoinSpecification.LEFT_OUTER_JOIN);
		} else {
			throw new SchemaException(Pass.FIRST, "No natural join rewrite available for '" + originalJoinKind + "' join kind.");
		}
	}

	private static ListSet<Name> collectNaturalJoinColumnNames(final SchemaContext sc, final TableInstance base,
			final TableInstance target) {
		final ListSet<Name> lhsTabCols = getColumnNames(sc, base);
		final ListSet<Name> rhsTabCols = getColumnNames(sc, target);

		lhsTabCols.retainAll(rhsTabCols);

		return lhsTabCols;
	}

	private static ListSet<Name> getColumnNames(final SchemaContext sc, final TableInstance table) {
		final ListSet<Name> columnNames = new ListSet<Name>();
		for (final PEColumn column : table.getAbstractTable().getColumns(sc)) {
			final UnqualifiedName columnName = column.getName().getUnqualified();
			columnNames.add(columnName);
		}

		return columnNames;
	}

}
