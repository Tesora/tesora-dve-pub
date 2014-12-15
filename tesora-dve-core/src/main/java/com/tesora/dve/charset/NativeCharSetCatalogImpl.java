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
import java.util.*;

import com.tesora.dve.common.DBType;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.singleton.Singletons;

public class NativeCharSetCatalogImpl implements Serializable, NativeCharSetCatalog {
	
	private static final long serialVersionUID = 1L;

	private Map<String, NativeCharSet> charSetsByName;
	private Map<String, NativeCharSet> charSetsByDVEName;
	private Map<Integer, NativeCharSet> charSetsById;

	public NativeCharSetCatalogImpl() {
		charSetsByName = new HashMap<String, NativeCharSet>();
		charSetsByDVEName = new HashMap<String, NativeCharSet>();
		charSetsById = new HashMap<Integer, NativeCharSet>();
	}

	@Override
	public void addCharSet(NativeCharSet ncs) {
		charSetsByName.put(ncs.getName().toUpperCase(Locale.ENGLISH), ncs);
		charSetsByDVEName.put(ncs.getJavaCharset().name().toUpperCase(Locale.ENGLISH), ncs);
		charSetsById.put(ncs.getId(), ncs);
	}

	@Override
	public NativeCharSet findCharSetByName(String charSetName) {
		return charSetsByName.get(charSetName.toUpperCase(Locale.ENGLISH));
	}

	@Override
	public NativeCharSet findCharSetByCollation(final String collation) {
		final NativeCollation nc = Singletons.require(NativeCollationCatalog.class).findCollationByName(collation);
		if (nc == null) {
			return null;
		}
		return findCharSetByName(nc.getCharacterSetName());
	}
	
	@Override
	public NativeCharSet findCharSetByCollationId(final int collationId) {
		final NativeCollation nc = Singletons.require(NativeCollationCatalog.class).findNativeCollationById(collationId);
		if (nc == null) {
			return null;
		}
		return findCharSetByName(nc.getCharacterSetName());
	}

	//	public NativeCharSet findCharSetByDVEName(Charset dveCharSet, boolean except) {
	//		return findCharSetByDVEName(dveCharSet.name(), except);
	//	}
	//	
	//	public NativeCharSet findCharSetByDVEName(String dveCharSetName, boolean except) {
	//		NativeCharSet t = charSetsByDVEName.get(dveCharSetName.toUpperCase(Locale.ENGLISH));
	//
	//		if (t == null && except)
	//			throw new PEException("Unsupported CHARACTER SET '" + dveCharSetName + "'");
	//		return t;
	//	}

	@Override
	public int size() {
		return charSetsByName.size();
	}
	
	@Override
	public Set<Map.Entry<String, NativeCharSet>> getCharSetCatalogEntriesByName() {
		return charSetsByName.entrySet();
	}

	@Override
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

	@Override
	public Collection<NativeCharSet> findAllNativeCharsets(){
		return charSetsByName.values();
	}

	@Override
    public Charset findJavaCharsetById(int clientCharsetId) {
		try {
			return findNativeCharsetById(clientCharsetId).getJavaCharset();
		} catch (Exception e) {
			throw new PECodingException("Invalid character set " + clientCharsetId, e);
		}
	}

	@Override
	public NativeCharSet findNativeCharsetById(int clientCharsetId) {
		return charSetsById.get(clientCharsetId);
	}

	@Override
	public boolean isCompatibleCharacterSet(String charSet) {
		return (findCharSetByName(charSet) != null);
	}
}
