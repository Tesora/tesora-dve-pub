package com.tesora.dve.charset;

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

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.charset.mysql.MysqlNativeCharSetCatalog;
import com.tesora.dve.common.DBType;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CharacterSets;
import com.tesora.dve.db.mysql.common.JavaCharsetCatalog;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;

public abstract class NativeCharSetCatalog implements Serializable, JavaCharsetCatalog {
	
	private static final long serialVersionUID = 1L;

	private Map<String, NativeCharSet> charSetsByName;
	private Map<String, NativeCharSet> charSetsByDVEName;
	private Map<Integer, NativeCharSet> charSetsById;

	public NativeCharSetCatalog() {
		charSetsByName = new HashMap<String, NativeCharSet>();
		charSetsByDVEName = new HashMap<String, NativeCharSet>();
		charSetsById = new HashMap<Integer, NativeCharSet>();
	}

	protected void addCharSet(NativeCharSet ncs) {
		charSetsByName.put(ncs.getName().toUpperCase(Locale.ENGLISH), ncs);
		charSetsByDVEName.put(ncs.getJavaCharset().name().toUpperCase(Locale.ENGLISH), ncs);
		charSetsById.put(ncs.getId(), ncs);
	}

	public abstract void load() throws PEException;

	public NativeCharSet findCharSetByName(String charSetName, boolean except) throws PEException {
		NativeCharSet t = charSetsByName.get(charSetName.toUpperCase(Locale.ENGLISH));

		if (t == null && except)
			throw new PEException("Unsupported CHARACTER SET '" + charSetName + "'");
		return t;
	}
	
	public NativeCharSet findCharSetByDVEName(Charset dveCharSet, boolean except) throws PEException {
		return findCharSetByDVEName(dveCharSet.name(), except);
	}
	
	public NativeCharSet findCharSetByDVEName(String dveCharSetName, boolean except) throws PEException {
		NativeCharSet t = charSetsByDVEName.get(dveCharSetName.toUpperCase(Locale.ENGLISH));

		if (t == null && except)
			throw new PEException("Unsupported CHARACTER SET '" + dveCharSetName + "'");
		return t;
	}

	public NativeCharSet findCharSetByCollation(final String collation, boolean except) throws PEException {
		NativeCollation nc = Singletons.require(HostService.class).getDBNative().getSupportedCollations().findCollationByName(collation, except);
		
		NativeCharSet t = findCharSetByName(nc.getCharacterSetName(), except);
		
		if (t == null && except)
			throw new PEException("No supported CHARACTER SET found for COLLATION '" + collation + "'");
		return t;
	}

	public int size() {
		return charSetsByName.size();
	}
	
	public Set<Map.Entry<String, NativeCharSet>> getCharSetCatalogEntriesByName() {
		return charSetsByName.entrySet();
	}

	public Set<Map.Entry<String, NativeCharSet>> getCharSetCatalogEntriesByPEName() {
		return charSetsByDVEName.entrySet();
	}
	
	public static NativeCharSetCatalog getDefaultCharSetCatalog(DBType dbType) {
		NativeCharSetCatalog ncc = null;
		
		switch (dbType) {
		case MYSQL:
		case MARIADB:
			ncc = MysqlNativeCharSetCatalog.DEFAULT_CATALOG;
			break;
		default:
			throw new PECodingException("No NativeCharsetCatalog defined for database type " + dbType);
		}
		
		return ncc;
	}

	public void save(CatalogDAO c) {
		for (NativeCharSet ncs : charSetsByName.values()) {
			CharacterSets cs = new CharacterSets(ncs.getId(), ncs.getName(), ncs.getDescription(),
					ncs.getMaxLen(), ncs.getJavaCharset().name());
			c.persistToCatalog(cs);
		}
	}

	@Override
    public Charset findJavaCharsetById(int clientCharsetId) {
		try {
			return findNativeCharsetById(clientCharsetId).getJavaCharset();
		} catch (Exception e) {
			throw new PECodingException("Invalid character set " + clientCharsetId, e);
		}
	}

	public NativeCharSet findNativeCharsetById(int clientCharsetId) {
		return charSetsById.get(clientCharsetId);
	}
}
