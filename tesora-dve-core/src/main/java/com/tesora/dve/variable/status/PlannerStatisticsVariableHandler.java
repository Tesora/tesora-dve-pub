// OS_STATUS: public
package com.tesora.dve.variable.status;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;
import com.tesora.dve.sql.PlannerStatisticType;
import com.tesora.dve.sql.PlannerStatistics;

public class PlannerStatisticsVariableHandler extends StatusVariableHandler {

	@Override
	public String getValue(CatalogDAO c, String name) throws PEException {
		try {
			return Long.toString(PlannerStatistics.getCurrentValue(PlannerStatisticType.valueOf(defaultValue)));
		} catch (PEException e) {
			throw new PENotFoundException("Unable to find value for status variable " + name, e);
		}
	}

	@Override
	public void reset(CatalogDAO c, String name) throws PEException {
		try {
			PlannerStatistics.resetCurrentValue(PlannerStatisticType.valueOf(defaultValue));
		} catch (PEException e) {
			throw new PENotFoundException("Unable to reset status variable " + name, e);
		}
	}
}
