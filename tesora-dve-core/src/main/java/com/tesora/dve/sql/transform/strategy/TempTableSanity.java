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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.util.ListSet;

// ensure that temp tables are not used before they are scheduled
public class TempTableSanity {

	private ListSet<TempTable> scheduled;
	
	public TempTableSanity() {
		scheduled = new ListSet<TempTable>();
	}

	public void assertSane(SchemaContext sc, DMLStatement dmls) throws PEException {
		for(TableKey tk : dmls.getDerivedInfo().getAllTableKeys()) {
			if (!(tk.getTable() instanceof PETable)) continue;
			PETable pet = (PETable) tk.getTable();
			if (!pet.isTempTable()) continue;
			if (!scheduled.contains(pet)) {
				throw new PEException("Internal error: use of temp table " + pet.getName(sc) + " before definition");
			} else if (!pet.hasDatabase(sc)) {
				throw new PEException("Internal error: temp table " + pet.getName(sc) + " has no database");
			}
		}
	}
	
	public void onTempTableDefinition(TempTable tt) {
		scheduled.add(tt);
	}
	
}
