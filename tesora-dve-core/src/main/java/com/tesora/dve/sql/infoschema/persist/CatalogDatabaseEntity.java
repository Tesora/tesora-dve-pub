// OS_STATUS: public
package com.tesora.dve.sql.infoschema.persist;

import com.tesora.dve.common.catalog.FKMode;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.SimplePersistedEntity;

public class CatalogDatabaseEntity extends SimplePersistedEntity {

	public CatalogDatabaseEntity(CatalogSchema schema, String name, int groupid, String charSet, String collation) throws PEException {
		super(schema.getDatabase());
		preValue("default_group_id",groupid);
		preValue("default_character_set_name",charSet);
		preValue("default_collation_name",collation);
		preValue("name",name);
		preValue("multitenant_mode",MultitenantMode.OFF.getPersistentValue());
		preValue("fk_mode",FKMode.STRICT.getPersistentValue());
	}
}
