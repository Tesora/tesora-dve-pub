// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import com.tesora.dve.common.catalog.RawPlan;
import com.tesora.dve.sql.schema.PERawPlan;

public class PEDropRawPlanStatement extends
		PEDropStatement<PERawPlan, RawPlan> {

	public PEDropRawPlanStatement(
			PERawPlan perp,
			Boolean ifExists) {
		super(PERawPlan.class, ifExists, true, perp, "RAW PLAN");
	}

}
