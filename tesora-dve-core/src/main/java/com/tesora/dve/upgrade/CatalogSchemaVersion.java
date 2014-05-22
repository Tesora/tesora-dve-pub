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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.exceptions.PEException;

// the in memory representation of the single row in pe_version.
public class CatalogSchemaVersion {

	public static final String versionTableDecl = 
			"create table pe_version (schema_version int not null, code_version varchar(128) not null, state varchar(64) not null)";

	private final int schemaVersion;
	private final String codeVersion;
	private final String state;
	
	public CatalogSchemaVersion(int schemaVersion, String codeVersion, String state) {
		this.schemaVersion = schemaVersion;
		this.codeVersion = codeVersion;
		this.state = state;
	}

	public int getSchemaVersion() { return schemaVersion; }
	public String getCodeVersion() { return codeVersion; }
	public String getState() { return state; }
	
	public static CatalogSchemaVersion getPersistentVersion(DBHelper helper) throws PEException {
		ResultSet rs = null;
		try {
			helper.executeQuery("select schema_version, code_version, state from pe_version limit 1");

			rs = helper.getResultSet();
			if (!rs.next()) 
				return CatalogVersions.getCurrentVersion();

			Integer catalogVersion = rs.getInt(1);
			String codeVersion = rs.getString(2);
			String state = rs.getString(3);

			return new CatalogSchemaVersion(catalogVersion,codeVersion,state);
		} catch (SQLException sqle) {
			throw new PEException("Unable to read current version from catalog at '" + helper.getUrl() + "'", sqle);
		} finally {
			if (rs != null) try {
				rs.close();
			} catch (SQLException sqle) {
				// ignore, we're on our way out
			}
		}
	}
	
	public String getInsertCommand() {
		return "insert into pe_version values (" + schemaVersion + ", '" + codeVersion + "', '" + state + "')"; 
	}
	
	public List<String> buildCurrentVersionTable() {
		return Arrays.asList(new String[] { versionTableDecl, getInsertCommand() });
	}
	
	public static void check(CatalogSchemaVersion persistent, CatalogSchemaVersion current, CatalogVersion cutoffVersion) throws PEException {
		if (persistent.getSchemaVersion() < current.getSchemaVersion()) {
			String pstate = persistent.getState();
			if ("current".equals(pstate)) {
				if (persistent.getSchemaVersion() < cutoffVersion.getSchemaVersion()) {
					throw new PEException("Catalog version is " + current.getSchemaVersion()
							+ " but catalog is only at version " + persistent.getSchemaVersion()
							+ ". Please dump and load.");
				}
				throw new PEException("Catalog version is " + current.getSchemaVersion()
						+ " but catalog is only at version " + persistent.getSchemaVersion()
						+ ".  Please upgrade.");
			} else if ("upgrading".equals(pstate))
				throw new PEException("Catalog is currently being upgraded, try again later.");
			else
				throw new PEException("Invalid catalog version state: '" + pstate + "'");
		} else if (persistent.getSchemaVersion() > current.getSchemaVersion()) {
			throw new PEException("Server software is too old for catalog.  Catalog at version " + persistent.getSchemaVersion() 
					+ " (written by build " + persistent.getCodeVersion() + ") but this server is at version " + current.getSchemaVersion());
		}
	}
}
