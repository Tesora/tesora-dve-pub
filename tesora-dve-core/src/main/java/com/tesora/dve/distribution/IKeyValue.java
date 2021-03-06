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

import java.util.Map;

import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.ConnectionValues;

public interface IKeyValue {

	public int getUserTableId();
	
	// if the table is range distributed, the id of the range
	public Integer getRangeId();
	
	public String getQualifiedTableName();

	public int compare(IKeyValue other) throws PEException;

	public int compare(RangeLimit rangeLimit) throws PEException;

	public StorageGroup getPersistentGroup();
	
	public int getPersistentGroupId();
	
	public DistributionModel getDistributionModel();
	
	public DistributionModel getContainerDistributionModel();

	@Override
	public boolean equals(Object o);

	@Override
	public int hashCode();
	
	public Map<String,? extends IColumnDatum> getValues();
	
	public IKeyValue rebind(ConnectionValues values) throws PEException;
}
