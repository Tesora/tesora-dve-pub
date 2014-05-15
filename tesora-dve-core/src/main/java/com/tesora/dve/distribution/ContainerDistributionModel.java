// OS_STATUS: public
package com.tesora.dve.distribution;


import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.SchemaContext.DistKeyOpType;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

@Entity
@DiscriminatorValue(ContainerDistributionModel.MODEL_NAME)
public class ContainerDistributionModel extends DistributionModel {

	private static final long serialVersionUID = 1L;
	public final static String MODEL_NAME = "Container";

	public final static ContainerDistributionModel SINGLETON = new ContainerDistributionModel(); 

	public ContainerDistributionModel() {
		super(MODEL_NAME);
	}

	@Override
	public MappingSolution mapKeyForInsert(CatalogDAO c, StorageGroup sg, IKeyValue key) throws PEException {
		return getRealDistributionModel(key).mapKeyForInsert(c, sg, key);
	}
	
	@Override
	public MappingSolution mapKeyForUpdate(CatalogDAO c, StorageGroup sg, IKeyValue key) throws PEException {
		return getRealDistributionModel(key).mapKeyForUpdate(c, sg, key);
	}

	@Override
	public MappingSolution mapKeyForQuery(CatalogDAO c, StorageGroup sg, IKeyValue key, DistKeyOpType type) throws PEException {
		return getRealDistributionModel(key).mapKeyForQuery(c, sg, key, type);
	}

	private DistributionModel getRealDistributionModel(IKeyValue kv) throws PEException {
		DistributionModel delegate = kv.getContainerDistributionModel();
		if (delegate == null)
			throw new PEException("Invalid key value.  " + kv.getQualifiedTableName() + " is container distributed but has no backing distribution.");
		return delegate;
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}
	
}
