package com.tesora.dve.upgrade;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.InformationCallback;
import com.tesora.dve.common.PELogUtils;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.upgrade.versions.*;

// these are the currently known version numbers.  if we come across a catalog with an older version
// number then we must dump and load.
public class CatalogVersions {
		
	public enum CatalogVersionNumber {

		SHOW_SERVERS(new InfoSchemaServerTable(22)),
		TEMPLATE_MATCH(new MatchTemplateVersion(23)),
		CREATE_OPTIONS_VERSIONS(new CreateTableOptionsVersion(24)),
		ADD_PLUGINS_SUPPORT(new InfoSchemaUpgradeVersion(25)),
		RAW_PLANS(new RawPlans(26)),
		ADD_CHARSET_IDS(new AddCharsetIds(27)),
		INFO_SCHEMA_TYPE_CHANGES(new InfoSchemaTypeChanges(28)),
		MT_FKS(new MTForeignKeys(29)),
		ENCRYPT_PASSWORD(new EncryptPasswordsVersion(30)),
		ADAPTIVE_AUTOINCREMENT(new AdaptiveAutoIncrementVersion(31)),
		VIEWS(new AddViewsVersion(32)),
		REF_CONSTRAINT_SCHEMAS(new InfoSchemaUpgradeVersion(33)),
		USER_SECURITY(new UserSecurityVersion(34)),
		COLUMN_CARDINALITY(new ColumnCardinality(35)),
		REBRANDING(new RebrandingVersion(36)),
		ADD_COLLATION(new AddCollation(37)),
		ADD_TEMPLATE_MODE(new AddTemplateMode(38)),
		EXTRA_INFO_SCHEMA_COLUMNS_COLUMNS(new ExtraInfoSchemaColumnsColumns(39)),
		EXTERNAL_SERVICE_SHOW_CONFIG(new InfoSchemaUpgradeVersion(40)),
		TEMPORARY_TABLES(new UserlandTemporaryTables(41)),
		GLOBAL_VARIABLES(new GlobalVariablesVersion(42)),
		COLLATION_ID_TYPE(new InfoSchemaUpgradeVersion(43)),
		INFOSCHEMA_CAPITALIZATION(new InfoSchemaUpgradeVersion(44)),
		DIRECT_INFOSCHEMA(new DirectInfoSchema(45)),
		TRIGGERS(new TriggersVersion(46)),
		ADD_MISSING_COLLATIONS(new InfoSchemaUpgradeVersion(47));
		
		private final CatalogVersion upgradeModule;
		
		private CatalogVersionNumber(CatalogVersion upgradeModule) {
			this.upgradeModule = upgradeModule;
		}

		public CatalogVersion getCatalogVersion() {
			return upgradeModule;
		}
	}

    private static final List<CatalogVersion> knownVersions = new ArrayList<CatalogVersion>();
    // this is dependent on major release - we should update this after every major release
    // we maintain a separate variable rather than just testing on version number for release management reasons.
    private static final CatalogVersionNumber mustDumpAndLoadBefore = CatalogVersionNumber.SHOW_SERVERS;
    
    
    static {
		for(CatalogVersionNumber cvn : CatalogVersionNumber.values()) {
			knownVersions.add(cvn.getCatalogVersion());
		}
    }

    static List<CatalogVersion> versionHistory = null;
	
	public static CatalogSchemaVersion getCurrentVersion() throws PEException {
		List<CatalogVersion> versions = getVersionHistory();
		
		CatalogSchemaVersion csv = new CatalogSchemaVersion(versions.get(versions.size() - 1).getSchemaVersion(),
				PELogUtils.getBuildVersionString(false),"current");
		return csv;
	}
	
	public static CatalogVersionNumber getOldestUpgradeSupported() {
		return mustDumpAndLoadBefore;
	}
	
	// throws ONLY IF the version doesn't match latest.  this is the check we do at server startup.
	public static CatalogSchemaVersion catalogVersionCheck(Properties props) throws PEException {
		DBHelper helper = null;
		try {
			helper = new DBHelper(props);
			CatalogSchemaVersion current = getCurrentVersion();
			helper.connect();
			CatalogSchemaVersion persistentVersion = CatalogSchemaVersion.getPersistentVersion(helper);
			CatalogSchemaVersion.check(persistentVersion, current, mustDumpAndLoadBefore.getCatalogVersion());
			
			return current;
		} finally {
			if(helper != null)
				helper.disconnect();
		}
	}
		
	public static List<CatalogVersion> getVersionHistory() throws PEException {
		if (versionHistory == null) {
			TreeMap<Integer,CatalogVersion> versions = new TreeMap<Integer,CatalogVersion>();
			for(CatalogVersion cv : knownVersions) {
				CatalogVersion already = versions.get(cv.getSchemaVersion());
				if (already != null)
					throw new PEException("Duplicate version numbers in catalog upgrade");
				versions.put(cv.getSchemaVersion(), cv);
			}
			versionHistory = Functional.toList(versions.values());
		}
		return versionHistory;
	}	
	
	public static void upgradeToLatest(Properties props, InformationCallback stdout) throws PEException {
		//make sure we have host services started, but no catalog.
        if (Singletons.lookup(HostService.class) == null){
			throw new PECodingException("An unexpected problem occurred while trying to access the required host services.");
		}

		if (CatalogDAO.CatalogDAOFactory.isSetup()){
		throw new PEException("An existing catalog service is currently running, please restart and try the upgrade again.");
		}

		Upgrader upgrader = new Upgrader(props);
		upgrader.upgrade(stdout);
	}
}
