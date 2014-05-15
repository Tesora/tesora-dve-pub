// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;

public abstract class CatalogModificationExecutionStep extends ExecutionStep {

	public enum Action {
		CREATE, DROP, ALTER
	}
	
	protected Persistable<?,?> rootEntity;
	protected Action action;

	protected Long updateCountOverride = null;
	
	public CatalogModificationExecutionStep(PEDatabase db, PEStorageGroup tsg, Persistable<?,?> root,
			Action act) {
		super(db, tsg, ExecutionType.DDL);
		this.rootEntity = root;
		this.action = act;
	}

	public Action getAction() {
		return action;
	}
		
	@Override
	public StorageGroup getStorageGroup(SchemaContext sc) {
		if (sg == null) return null;
		return sg.getPersistent(sc);
	}

	@Override
	public abstract CacheInvalidationRecord getCacheInvalidation(SchemaContext sc);

	@Override
	public Long getUpdateCount(SchemaContext sc) {
		if (updateCountOverride != null) 
			return updateCountOverride;
		return super.getUpdateCount(sc);
	}

	public void setUpdateCountOverride(Long v) {
		updateCountOverride = v;
	}
	
}
