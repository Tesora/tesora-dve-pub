// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

import java.util.HashMap;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.parser.TranslatorUtils;
import com.tesora.dve.sql.schema.PEStorageSite;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;

public class PEDropStorageSiteStatement extends
		PEDropStatement<PEStorageSite, PersistentSite> {

	public PEDropStorageSiteStatement( 
			Persistable<PEStorageSite, PersistentSite> targ) {
		super(PEStorageSite.class, null, true, targ, TranslatorUtils.PERSISTENT_SITE_TAG);
	}

	public void ensureUnreferenced(SchemaContext pc) {
		PEStorageSite pess = getTarget().get();
		try {
			pc.beginSaveContext();
			PersistentSite ss = null;
			try {
				pess.persistTree(pc,true);
				ss = pess.getPersistent(pc);
			} finally {
				pc.endSaveContext();
			}
			HashMap<String,Object> params = new HashMap<String,Object>();
			params.put("ss",ss);
			String q = "select gen.storageGroup from StorageGroupGeneration gen where :ss member of gen.groupMembers";
			List<CatalogEntity> any = pc.getCatalog().query(q,params);
			
				//.query("select gen.storageGroup from StorageGroupGeneration gen where gen.groupMembers = :ss", params);
			if (!any.isEmpty()) {
				PersistentGroup pg = (PersistentGroup)any.get(0);
				throw new SchemaException(Pass.PLANNER,"Unable to drop persistent site " + pess.getName().get() + " because used by group " + pg.getName());
			}
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to compute reference set for site " + pess.getName().get());
		}
	}
}
