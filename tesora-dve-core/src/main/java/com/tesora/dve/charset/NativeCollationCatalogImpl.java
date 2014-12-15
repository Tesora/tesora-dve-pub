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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;

import com.tesora.dve.common.DBType;
import com.tesora.dve.exceptions.PECodingException;

public class NativeCollationCatalogImpl implements Serializable, NativeCollationCatalog {
	
	private static final long serialVersionUID = 1L;
	
	private Map<String, NativeCollation> collationsByName;
	private Map<String, List<NativeCollation>> collationsByCharsetName;
	private Map<Long, NativeCollation> collationsById;

	public NativeCollationCatalogImpl() {
		collationsByName = new HashMap<String, NativeCollation>();
		collationsById = new HashMap<Long, NativeCollation>();
		collationsByCharsetName = new HashMap<String, List<NativeCollation>>();
	}

	@Override
	public void addCollation(NativeCollation nc) {
		collationsByName.put(nc.getName().toUpperCase(Locale.ENGLISH), nc);
		collationsById.put(nc.getId(), nc);
		List<NativeCollation> collations = collationsByCharsetName.get(nc.getCharacterSetName().toUpperCase(Locale.ENGLISH));
		if (collations == null) {
			collations = new ArrayList<NativeCollation>();
			collationsByCharsetName.put(nc.getCharacterSetName().toUpperCase(Locale.ENGLISH), collations);
		}
		collations.add(nc);
	}

	@Override
	public NativeCollation findCollationByName(String collationName) {
		return collationsByName.get(collationName.toUpperCase(Locale.ENGLISH));
	}
	
	@Override
	public NativeCollation findDefaultCollationForCharSet(String charsetName) {
		final List<NativeCollation> collations = collationsByCharsetName.get(charsetName.toUpperCase(Locale.ENGLISH));
		if (collations == null) {
			return null;
		}
		
		final NativeCollation defaultCollation = (NativeCollation) CollectionUtils.find(collations,
				new Predicate() {
					@Override
					public boolean evaluate(Object arg0) {
						if (arg0 instanceof NativeCollation) {
							final NativeCollation nc = (NativeCollation) arg0;
							return nc.isDefault();
						}
						return false;
					}
				});
		
		if (defaultCollation == null) {
			throw new PECodingException("No default collation found for character set '" + charsetName + "'");
		}

		return defaultCollation;
	}

	@Override
	public NativeCollation findNativeCollationById(long collationId) {
		return collationsById.get(collationId);
	}
	
	@Override
	public boolean isCompatibleCollation(String collation) {
		return (findCollationByName(collation) != null);
	}
	
	@Override
	public int size() {
		return collationsByName.size();
	}
	
	@Override
	public Set<String> getCollationsCatalogEntriesByName() {
		return collationsByName.keySet();
	}

	public static NativeCollationCatalog getDefaultCollationCatalog(DBType dbType) {
		NativeCollationCatalog ncc = null;
		
		switch (dbType) {
		case MYSQL:
		case MARIADB:
			ncc = MysqlNativeCollationCatalog.DEFAULT_CATALOG;
			break;
		default:
			throw new PECodingException("No NativeCollationCatalog defined for database type " + dbType);
		}
		
		return ncc;
	}

}
