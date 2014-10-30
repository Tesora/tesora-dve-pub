package com.tesora.dve.distribution;

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

import java.util.HashSet;
import java.util.Set;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.StorageGroupGeneration;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.db.DBResultConsumer.RowCountAdjuster;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.ExecutionState;
import com.tesora.dve.queryplan.QueryStepMultiTupleRedistOperation;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

@Entity
@DiscriminatorValue(BroadcastDistributionModel.MODEL_NAME)
public class BroadcastDistributionModel extends DistributionModel {
	
	private static final long serialVersionUID = 1L;
	public final static String MODEL_NAME = "Broadcast";
	
	public final static BroadcastDistributionModel SINGLETON = new BroadcastDistributionModel();

	public BroadcastDistributionModel() {
		super(MODEL_NAME);
	}

	@Override
	public MappingSolution mapKeyForInsert(CatalogDAO c, StorageGroup wg,
			IKeyValue key) throws PEException {
		return MappingSolution.AllWorkersSerialized;
	}

	@Override
	public MappingSolution mapKeyForUpdate(CatalogDAO c, StorageGroup wg,
			IKeyValue key) throws PEException {
		return MappingSolution.AllWorkersSerialized;
	}

	@Override
	public MappingSolution mapKeyForQuery(CatalogDAO c, StorageGroup sg, IKeyValue key, DistKeyOpType operation)
			throws PEException {
		MappingSolution mappingSolution;
		
		if (operation.equals(DistKeyOpType.SELECT_FOR_UPDATE))
			mappingSolution = MappingSolution.AnyWorkerSerialized;
		else
			mappingSolution = MappingSolution.AnyWorker;
		
		return mappingSolution;
	}

	@Override
	public MappingSolution mapForQuery(WorkerGroup wg, SQLCommand command) throws PEException {
		MappingSolution mappingSolution;
		
 		if (command.isForUpdateStatement())
			mappingSolution = MappingSolution.AnyWorkerSerialized;
		else
			mappingSolution = MappingSolution.AnyWorker.lockMapping(wg); 
		
		return mappingSolution;
	}

	@Override
	public void prepareGenerationAddition(ExecutionState estate, WorkerGroup wg, UserTable userTable, StorageGroupGeneration newGen) throws PEException {
		Set<PersistentSite> netNewSites = new HashSet<PersistentSite>(newGen.getStorageSites());
		netNewSites.removeAll(wg.getStorageSites());
		PersistentGroup netNewSG = new PersistentGroup(netNewSites);
	
		// if the new generation contains no net new sites, we don't need to create/populate the table
		if ( netNewSG.sizeForProvisioning() != 0 ) {
			// Create a new userTable from the old where the persistent group is just the new generation
			UserTable redistTable = userTable.shallowClone();
			redistTable.setPersistentGroup(netNewSG);
	
			QueryStepMultiTupleRedistOperation qso =
					new QueryStepMultiTupleRedistOperation(netNewSG, userTable.getDatabase(),
							new SQLCommand(estate.getConnection(), "select * from " + userTable.getNameAsIdentifier()),
							BroadcastDistributionModel.SINGLETON
							).toUserTable(netNewSG, redistTable);
			qso.executeSelf(estate, wg, DBEmptyTextResultConsumer.INSTANCE);
		}
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}

	@Override
	public RowCountAdjuster getInsertAdjuster() {
		return new RowCountAdjuster() {
			@Override
			public long adjust(long rawRowCount, int siteCount) {
				return rawRowCount / siteCount;
			}
		};
	}

	@Override
	public RowCountAdjuster getUpdateAdjuster() {
		return getInsertAdjuster();
	}
}
