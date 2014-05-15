// OS_STATUS: public
package com.tesora.dve.sql.schema.mt;

import com.tesora.dve.common.catalog.ITenant;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.SchemaContext;

public interface IPETenant {

	public Long getTenantID();
	
	public boolean isGlobalTenant();
	
	public ITenant getPersistentTenant(SchemaContext sc) throws PEException;
	
	public String getUniqueIdentifier();
	
}
