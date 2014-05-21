// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

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
