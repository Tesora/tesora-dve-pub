package com.tesora.dve.sql.transform.execution;

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

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepDDLOperation;
import com.tesora.dve.queryplan.QueryStepGroupProviderDDLOperation;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.ConnectionValues;
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
	protected QueryStepDDLOperation buildOperation(SchemaContext sc, ConnectionValues cv) throws PEException {
		return new QueryStepGroupProviderDDLOperation(getStorageGroup(sc,cv),smc);
	}

	@Override
	public void getSQL(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing, List<String> buf, EmitOptions opts) {
	}
	
}
