// OS_STATUS: public
package com.tesora.dve.tools.analyzer.stats;

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

import java.util.List;

import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.Table;

public interface StatsVisitor {

	/**
	 * This will always be called before any other methods for a particular stmt
	 * however it may be called again before endStmt for a particular stmt if
	 * there are nested queries.
	 */
	public void beginStmt(StatementAnalysis<?> s);

	/**
	 * A column from the where clause of the form c = <literal>, or c = ?.
	 */
	public void onIdentColumn(Column<?> c, int freq);

	public void onJoin(EquijoinInfo joinInfo, int freq);

	/**
	 * Where c is on the lhs of an update expression in an update statement.
	 */
	public void onUpdate(PEColumn c, int freq);

	public void onInsertValues(PETable tab, int ntuples, int freq);

	/**
	 * In the following, the table list is all the tables in the stmt excluding
	 * tables in nested queries.
	 */
	public void onSelect(List<Table<?>> tables, int freq);

	public void onUpdate(List<PETable> tables, int freq);

	public void onDelete(List<PETable> tables, int freq);

	public void onInsertIntoSelect(PETable table, int freq);

	public void onUnion(int freq);

	/**
	 * Where c shows up (directly or indirectly) in a group by expression.
	 */
	public void onGroupBy(Column<?> c, int freq);

	/**
	 * Where c shows up (directly or indirectly) in an order by expression.
	 */
	public void onOrderBy(PETable tab, int freq);

	/**
	 * This will always be called last.
	 */
	public void endStmt(StatementAnalysis<?> s);

}
