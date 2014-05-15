// OS_STATUS: public
package com.tesora.dve.distribution;

import java.util.Map;

import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.exceptions.PEException;

public interface IKeyValue {

	public int getUserTableId();
	
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
}
