// OS_STATUS: public
package com.tesora.dve.sql.infoschema.persist;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.SimplePersistedEntity;

public class CatalogTableEntity extends SimplePersistedEntity {

	public CatalogTableEntity(CatalogSchema s, CatalogDatabaseEntity cde, String name, int modelid, int storageid, String engine) throws PEException {
		super(s.getTable());
		addRequires(cde);
		preValue("persistent_group_id",storageid);
		preValue("distribution_model_id",modelid);
		preValue("name",name);
		preValue("engine",engine);
		preValue("table_type","SYSTEM VIEW");
	}
}
