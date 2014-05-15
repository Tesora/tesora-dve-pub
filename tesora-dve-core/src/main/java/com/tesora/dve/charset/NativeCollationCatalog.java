// OS_STATUS: public
package com.tesora.dve.charset;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;

import com.tesora.dve.charset.mysql.MysqlNativeCollationCatalog;
import com.tesora.dve.common.DBType;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.Collations;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;

public abstract class NativeCollationCatalog implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private Map<String, NativeCollation> collationsByName;
	private Map<String, List<NativeCollation>> collationsByCharsetName;
	private Map<Integer, NativeCollation> collationsById;

	public NativeCollationCatalog() {
		collationsByName = new HashMap<String, NativeCollation>();
		collationsById = new HashMap<Integer, NativeCollation>();
		collationsByCharsetName = new HashMap<String, List<NativeCollation>>();
	}

	protected void addCollation(NativeCollation nc) {
		collationsByName.put(nc.getName().toUpperCase(Locale.ENGLISH), nc);
		collationsById.put(nc.getId(), nc);
		List<NativeCollation> collations = collationsByCharsetName.get(nc.getCharacterSetName().toUpperCase(Locale.ENGLISH));
		if (collations == null) {
			collations = new ArrayList<NativeCollation>();
			collationsByCharsetName.put(nc.getCharacterSetName().toUpperCase(Locale.ENGLISH), collations);
		}
		collations.add(nc);
	}

	public abstract void load() throws PEException;

	public NativeCollation findCollationByName(String collationName, boolean except) throws PEException {
		NativeCollation t = collationsByName.get(collationName.toUpperCase(Locale.ENGLISH));

		if (t == null && except)
			throw new PEException("Unsupported COLLATION '" + collationName + "'");
		return t;
	}

	public NativeCollation findNativeCollationById(int collationId) {
		return collationsById.get(collationId);
	}
	

	public NativeCollation findDefaultCollationForCharSet(String charsetName, boolean except) throws PEException {
		List<NativeCollation> collations = collationsByCharsetName.get(charsetName.toUpperCase(Locale.ENGLISH));
		if (collations == null) {
			if (except) {
				throw new PEException("No collations found for character set '" + charsetName + "'");
			}
			return null;
		}
		
		NativeCollation defaultCollation = (NativeCollation) CollectionUtils.find(collations,
				new Predicate() {
					@Override
					public boolean evaluate(Object arg0) {
						if (arg0 instanceof NativeCollation) {
							NativeCollation nc = (NativeCollation)arg0;
							return nc.isDefault();
						}
						return false;
					}
				});
		
		if (defaultCollation == null && except) {
			throw new PEException("No default collation found for character set '" + charsetName + "'");
		}
		return defaultCollation;
	}
	
	public boolean validateUTF8Collation(String collation) throws PEException {
		return (findCollationByName(collation, false) != null);
	}
	
	public int size() {
		return collationsByName.size();
	}
	
	public Set<String> getCollationsCatalogEntriesByName() {
		return collationsByName.keySet();
	}
	
	public void save(CatalogDAO c) {
		for (NativeCollation nc : collationsByName.values()) {
			Collations cs = new Collations(nc.getName(), nc.getCharacterSetName(), 
					nc.getId(), nc.isDefault(), nc.isCompiled(), nc.getSortLen());
			c.persistToCatalog(cs);
		}
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
