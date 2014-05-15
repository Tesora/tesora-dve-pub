// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.featureplan;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.strategy.PlannerContext;

public abstract class NonQueryFeatureStep extends FeatureStep {

	private final DMLStatement statement;
	private final TableKey table;
	

	public NonQueryFeatureStep(FeaturePlanner planner, DMLStatement statement, TableKey tableKey, PEStorageGroup srcGroup, DistributionKey key) {
		super(planner, srcGroup, key);
		this.statement = statement;
		this.table = tableKey;
	}

	@Override
	public DMLStatement getPlannedStatement() {
		return statement;
	}

	public TableKey getTable() {
		return table;
	}
	
	@Override
	public Database<?> getDatabase(PlannerContext pc) {
		return table.getAbstractTable().getDatabase(pc.getContext());
	}
}
