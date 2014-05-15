// OS_STATUS: public
package com.tesora.dve.upgrade.versions;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.exceptions.PEException;

public abstract class SimpleCatalogVersion extends BasicCatalogVersion {
	
	public SimpleCatalogVersion(int v) {
		this(v,false);
	}

	public SimpleCatalogVersion(int v, boolean infoSchemaUpgrade) {
		super(v, infoSchemaUpgrade);
	}
	
	@Override
	public void upgrade(DBHelper helper) throws PEException {
		for(String c : getUpgradeCommands(helper)) try {
			helper.executeQuery(c);
		} catch (SQLException sqle) {
			throw new PEException("Error executing '" + c + "'",sqle);
		}
		dropVariables(helper, getObsoleteVariables());
	}

	public abstract String[] getUpgradeCommands(DBHelper helper) throws PEException;

	// override this for your variables
	public Collection<String> getObsoleteVariables() {
		return Collections.emptySet();
	}
}
