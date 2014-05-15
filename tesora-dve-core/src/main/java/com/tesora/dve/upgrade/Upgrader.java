// OS_STATUS: public
package com.tesora.dve.upgrade;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.DBType;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.upgrade.CatalogVersions.CatalogVersionNumber;

// The upgrader upgrades the catalog from the persistent current version to the current version of the tool.
public class Upgrader {

	private Properties connectProperties;
	
	public Upgrader(Properties props) {
		connectProperties = props;
	}
	
	public void upgrade() throws PEException {
		DBHelper helper = new DBHelper(connectProperties);
		String driver = DBHelper.loadDriver(connectProperties.getProperty(DBHelper.CONN_DRIVER_CLASS));
		DBType dbType = DBType.fromDriverClass(driver);
		DBNative dbn = DBNative.DBNativeFactory.newInstance(dbType);
		helper.connect();
		try {
			CatalogSchemaVersion latest = CatalogVersions.getCurrentVersion();
			CatalogSchemaVersion current = CatalogSchemaVersion.getPersistentVersion(helper);
			CatalogVersionNumber oldestSupported = CatalogVersions.getOldestUpgradeSupported();
			if (current.getSchemaVersion() < oldestSupported.getCatalogVersion().getSchemaVersion())
				throw new PEException("No upgrade available for version " + current.getSchemaVersion() + ": please dump and load");
			List<CatalogVersion> versionHistory = CatalogVersions.getVersionHistory();
			int currentlyAt = -1;
			for(int i = 0; i < versionHistory.size(); i++) {
				if (versionHistory.get(i).getSchemaVersion() == current.getSchemaVersion()) {
					currentlyAt = i;
					break;
				}
			}
			if (currentlyAt == -1)
				throw new PEException("No known upgrade for catalog schema version " + current.getSchemaVersion());
			// keep track of whether an info schema upgrade is needed.  if so we will only do it at the end.
			boolean requiresInfoSchemaUpgrade = false;
			int firstUpgrade = currentlyAt + 1;
			for(int i = firstUpgrade; i < versionHistory.size(); i++) {
				CatalogVersion cv = versionHistory.get(i);
				if (cv.hasInfoSchemaUpgrade())
					requiresInfoSchemaUpgrade = true;
				if (cv.getSchemaVersion() == latest.getSchemaVersion()) 
					upgrade(helper,cv,latest,(requiresInfoSchemaUpgrade ? dbn : null));
				else
					upgrade(helper,cv,new CatalogSchemaVersion(cv.getSchemaVersion(),"via upgrade","current"),null);
			}
		} finally {
			helper.disconnect();
		}
	}
	
	private void upgrade(DBHelper helper, CatalogVersion cv, CatalogSchemaVersion finalVersion, DBNative dbn) throws PEException {
		try {
			helper.executeQuery("update pe_version set state = 'upgrading'");
			cv.upgrade(helper);
			if (dbn != null)
				upgradeInfoSchema(helper,dbn);
			helper.executeQuery("delete from pe_version");
			helper.executeQuery(finalVersion.getInsertCommand());
		} catch (SQLException sqle) {
			throw new PEException("Unable to upgrade to version " + cv.getSchemaVersion(),sqle);
		}
	}
	
	private void upgradeInfoSchema(DBHelper helper, DBNative dbn) throws PEException {
		InfoSchemaUpgrader.upgradeInfoSchema(helper, dbn);
	}
}
