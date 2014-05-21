// OS_STATUS: public
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

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.worker.WorkerGroup;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

@Entity
@DiscriminatorValue(StaticDistributionModel.MODEL_NAME)
public class StaticDistributionModel extends DistributionModel {

	static Logger logger = Logger.getLogger(StaticDistributionModel.class);

	private static final long serialVersionUID = 1L;
	public final static String MODEL_NAME = "Static";
	
	public final static StaticDistributionModel SINGLETON = new StaticDistributionModel(); 

	public StaticDistributionModel() {
		super(MODEL_NAME);
	}

	@Override
	public WorkerGroup.MappingSolution mapKeyForInsert(CatalogDAO c, StorageGroup sg, IKeyValue key) throws PEException {
		return new MappingSolution(mapKeyToSite(key, sg.sizeForProvisioning()));
	}
	
	@Override
	public WorkerGroup.MappingSolution mapKeyForUpdate(CatalogDAO c, StorageGroup sg, IKeyValue key) throws PEException {
		return new MappingSolution(mapKeyToSite(key, sg.sizeForProvisioning()));
	}
	
	@Override
	public WorkerGroup.MappingSolution mapKeyForQuery(CatalogDAO c, StorageGroup sg, IKeyValue key, DistKeyOpType op) throws PEException {
		return new MappingSolution(mapKeyToSite(key, sg.sizeForProvisioning()));
	}
	
	private StorageSite mapKeyToSite(IKeyValue key, int groupSize) throws PELockedException {
		return new DynamicSitePlaceHolder(Math.abs(key.hashCode()) % groupSize, groupSize);
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}
}
