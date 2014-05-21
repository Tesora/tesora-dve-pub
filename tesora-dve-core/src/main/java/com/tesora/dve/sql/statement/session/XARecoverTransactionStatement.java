package com.tesora.dve.sql.statement.session;

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
import java.util.List;

import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.statement.ddl.SchemaQueryStatement;

public class XARecoverTransactionStatement extends SchemaQueryStatement {

	public XARecoverTransactionStatement() {
		super(false, "", buildEmptyResultSet());
	}
	
	
	public static IntermediateResultSet buildEmptyResultSet() {
		ColumnSet cs = new ColumnSet();
		cs.addColumn("formatID", 21, "bigint", java.sql.Types.BIGINT);
		cs.addColumn("gtrid_length",21, "bigint", java.sql.Types.BIGINT);
		cs.addColumn("bqual_length",21, "bigint", java.sql.Types.BIGINT);
		cs.addColumn("data", 128, "varchar", java.sql.Types.VARCHAR);

		List<ResultRow> rows = new ArrayList<ResultRow>();
		return new IntermediateResultSet(cs, rows);
	}


}
