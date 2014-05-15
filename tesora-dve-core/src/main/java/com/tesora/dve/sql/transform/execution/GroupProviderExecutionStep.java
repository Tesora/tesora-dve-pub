// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepDDLOperation;
import com.tesora.dve.queryplan.QueryStepGroupProviderDDLOperation;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.PEProvider;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.worker.SiteManagerCommand;

public class GroupProviderExecutionStep extends
		SimpleDDLExecutionStep {

	private SiteManagerCommand smc;
	
	public GroupProviderExecutionStep(PEProvider provider,
			SiteManagerCommand smc,
			Action act, List<CatalogEntity> deleteList,
			List<CatalogEntity> entityList) {
		super(null, null, provider, act, SQLCommand.EMPTY, deleteList, entityList, null);
		this.smc = smc;
	}

	@Override
	protected QueryStepDDLOperation buildOperation(SchemaContext sc) throws PEException {
		return new QueryStepGroupProviderDDLOperation(smc);
	}

	@Override
	public void getSQL(SchemaContext sc, List<String> buf, EmitOptions opts) {
	}
	
}
