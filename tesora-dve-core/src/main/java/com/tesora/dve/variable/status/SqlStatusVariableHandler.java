// OS_STATUS: public
package com.tesora.dve.variable.status;

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

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.sql.parser.SqlStatistics;
import com.tesora.dve.sql.statement.StatementType;

public class SqlStatusVariableHandler extends StatusVariableHandler {

	@Override
	public String getValue(CatalogDAO c, String name) throws PENotFoundException {
		if ("Queries".equalsIgnoreCase(name) || "Questions".equalsIgnoreCase(name))
			return Long.toString(SqlStatistics.getTotalValue());

		try {
			return Long.toString(SqlStatistics.getCurrentValue(StatementType.valueOf(defaultValue)));
		} catch (PEException e) {
			throw new PENotFoundException("Unable to find value for status variable " + name, e);
		}
	}
	
	@Override
	public void reset(CatalogDAO c, String name) throws PENotFoundException {
		if ("Queries".equalsIgnoreCase(name) || "Questions".equalsIgnoreCase(name)) {
			SqlStatistics.resetAllValues();
			return;
		}

		try {
			SqlStatistics.resetCurrentValue(StatementType.valueOf(defaultValue));
		} catch (PEException e) {
			throw new PENotFoundException("Unable to find value for status variable " + name, e);
		}
	}
}
