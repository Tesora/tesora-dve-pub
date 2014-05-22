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

import java.util.concurrent.Callable;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

@Entity
@DiscriminatorValue(RandomDistributionModel.MODEL_NAME)
public class RandomDistributionModel extends DistributionModel {

	private static final long serialVersionUID = 1L;
	public final static String MODEL_NAME = "Random";
	public static final DistributionModel SINGLETON = new RandomDistributionModel();

	static final Cache<Integer, PersistentGroup> groupCache = CacheBuilder.newBuilder()
			.softValues().build();

	
	public RandomDistributionModel() {
		super(MODEL_NAME);
	}
	
	@Override
	public MappingSolution mapKeyForInsert(CatalogDAO c, StorageGroup sg, IKeyValue key) throws PEException {
		MappingSolution sitesForInsert;
		int id = key.getPersistentGroupId();
		PersistentGroup pg = null;
		if (id > 0)
			pg = getPersistentGroup(c,key.getPersistentGroupId());
		else
			pg = (PersistentGroup) key.getPersistentGroup();
		synchronized (pg) {
			sitesForInsert = new WorkerGroup.MappingSolution(pg.anySiteInLatestGeneration());
		}
		return sitesForInsert;
	}

//	@Override
//	public MappingSolution mapKeyForUpdate(CatalogDAO c, StorageGroup wg, IKeyValue key) throws PEException {
//		return MappingSolution.AllWorkersSerialized;
//	}
//
	@Override
	public MappingSolution mapKeyForQuery(CatalogDAO c, StorageGroup wg, IKeyValue key, DistKeyOpType operation)
			throws PEException {
		return operation.equals(DistKeyOpType.SELECT_FOR_UPDATE) ? MappingSolution.AllWorkersSerialized : MappingSolution.AllWorkers;
	}

	@Override
	public MappingSolution mapForQuery(WorkerGroup wg, SQLCommand command) throws PEException {
		return command.isForUpdateStatement() ? MappingSolution.AllWorkersSerialized : MappingSolution.AllWorkers;
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}
	
	private static PersistentGroup getPersistentGroup(final CatalogDAO c, final int groupid) throws PEException {
		try {
			return groupCache.get(groupid, new Callable<PersistentGroup>() {

				@Override
				public PersistentGroup call() throws Exception {
					PersistentGroup pg = c.findByKey(PersistentGroup.class, groupid);
					// make sure this is loaded
					pg.anySiteInLatestGeneration();
					return pg;
				}
				
			});
		} catch (Throwable t) {
			throw new PEException("Unable to lazily cache persistent group",t);
		}
	}
	
	public static void clearCache() {
		groupCache.invalidateAll();
	}
}
