// OS_STATUS: public
package com.tesora.dve.common.catalog;

import javax.persistence.EntityManager;

import com.tesora.dve.common.catalog.CatalogDAO;

public class CatalogDAOAccessor {
	
	public static EntityManager getEntityManager(CatalogDAO c) {
		return c.em.get();
	}

}
