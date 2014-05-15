// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.PESiteInstance;
import com.tesora.dve.sql.schema.PEStorageSite;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;

public class PEAlterPersistentSite extends PEAlterStatement<PEStorageSite> {

	Boolean addOperation = null;
	String haType = null;
	PESiteInstance master = null;
	List<PESiteInstance> siteInstances = null;
	
	public PEAlterPersistentSite(PEStorageSite target,
			boolean peOnly) {
		super(target, peOnly);
	}

	public PEAlterPersistentSite(PEStorageSite pess,
			String haType) {
		this(pess, true);
		
		this.haType = haType;
	}

	public PEAlterPersistentSite(PEStorageSite pess,
			PESiteInstance master) {
		this(pess, true);
		
		this.master = master;
	}

	public PEAlterPersistentSite(PEStorageSite pess,
			boolean addOperation, List<PESiteInstance> siteInstances) {
		this(pess, true);
		
		this.addOperation = addOperation;
		this.siteInstances = siteInstances;
	}

	@Override
	protected PEStorageSite modify(SchemaContext sc, PEStorageSite backing) throws PEException {
		// set the new values
		if (haType != null) {
			backing.setHAType(haType);
		}
		
		if (master != null) {
			backing.setMaster(master);
		}
		
		if (addOperation != null) {
			for(PESiteInstance si : siteInstances) {
				if (addOperation) {
					backing.addSiteInstance(si);
				} else {
					backing.removeSiteInstance(si);
				}
			}
		}
		return backing;
	}

	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		return CacheInvalidationRecord.GLOBAL;
	}

}
