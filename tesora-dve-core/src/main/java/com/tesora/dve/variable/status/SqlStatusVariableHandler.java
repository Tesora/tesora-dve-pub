// OS_STATUS: public
package com.tesora.dve.variable.status;

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
