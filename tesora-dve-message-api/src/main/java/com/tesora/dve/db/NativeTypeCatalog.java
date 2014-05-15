// OS_STATUS: public
package com.tesora.dve.db;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.exceptions.PEException;

public abstract class NativeTypeCatalog implements Serializable {
	
	private static final long serialVersionUID = 1L;

	protected Map<String, NativeType> typesByName;
	protected MultiMap<Integer, NativeType> typesByDataType;
	protected MultiMap<Integer, NativeType> typesByNativeTypeId;
	protected List<String> jdbcTypeNames;

	/**
	 * The numTypesLoaded attribute and associated getters/setters are provided
	 * to allow for checking that the number of types loaded = the size of 
	 * the TypeCatalog downstream (i.e. in the JDBC driver)
	 * Used by tests...
	 */
	private int numTypesLoaded;
	public int getNumTypesLoaded() {
		return numTypesLoaded;
	}
	public void setNumTypesLoaded( int numTypesLoaded ) {
		this.numTypesLoaded = numTypesLoaded;
	}

	public NativeTypeCatalog() {
		typesByName = new LinkedHashMap<String, NativeType>();
		typesByDataType = new MultiMap<Integer, NativeType>();
		jdbcTypeNames = new ArrayList<String>();
		typesByNativeTypeId = new MultiMap<Integer, NativeType>();
	}

	protected void addType(NativeType nativeType) {
		typesByName.put(nativeType.getTypeName(), nativeType);
		if ( nativeType.isJdbcType() )
			jdbcTypeNames.add(nativeType.getTypeName());
		
		for (NativeTypeAlias alias : nativeType.getAliases()) {
			String aliasFixedName = NativeType.fixName(alias.getAliasName(),true);
			typesByName.put(aliasFixedName, nativeType);
			if ( alias.isJdbcType() )
				jdbcTypeNames.add(aliasFixedName);
		}
		typesByDataType.put(nativeType.getDataType(), nativeType);
		
		typesByNativeTypeId.put(nativeType.getNativeTypeId(), nativeType);
	}

	protected void sortNativeTypeIdLists() {
		for (Integer nativeTypeId : typesByNativeTypeId.keySet()) {
			Collection<NativeType> ntList = typesByNativeTypeId.get(nativeTypeId);
			Collections.sort((List<NativeType>)ntList, new NativeTypePrecisionComparator());
		}
	}

	public abstract void load() throws PEException;

	public NativeType findType(String name, boolean except) throws PEException {
		NativeType nativeType = typesByName.get(NativeType.fixName(name, true));

		if (nativeType == null && except)
			throw new PEException("Unknown type: '" + name + "'");
		return nativeType;
	}
	
	public NativeType findType(int dataType, boolean except) throws PEException {
		NativeType nativeType = null;
		Collection<NativeType> nativeTypes = typesByDataType.get(dataType);
		if (nativeTypes != null && nativeTypes.size() > 0) {
			nativeType = ((List<NativeType>) nativeTypes).get(0);
		}
		if (nativeType == null && except)
			throw new PEException("Unknown data type code: " + dataType);
		return nativeType;
	}
	
	public Collection<NativeType> findMatchingTypes(int nativeTypeId, boolean except) throws PEException {
		Collection<NativeType> nativeTypeList = typesByNativeTypeId.get(nativeTypeId);
		
		if ( (nativeTypeList == null || nativeTypeList.isEmpty()) && except )
			throw new PEException("Unknown Native Type Id: '" + nativeTypeId + "'");
		
		return nativeTypeList;
	}
	
	public int size() {
		return typesByName.size();
	}
	
	public Set<Map.Entry<String, NativeType>> getTypeCatalogEntries() {
		return typesByName.entrySet();
	}

	public Map<String, NativeType> getTypesByName() {
		return typesByName;
	}

	public void setTypesByName(Map<String, NativeType> typesByName) {
		this.typesByName = typesByName;
	}
	
	public List<String> getJdbcTypeNames() {
		return jdbcTypeNames;
	}

	class NativeTypePrecisionComparator implements Comparator<NativeType> {

		@Override
		public int compare(NativeType nt1, NativeType nt2) {
			return compare(nt1.getPrecision(), nt2.getPrecision());
		}
		
		private int compare ( long first, long second) {
			 return first < second ? -1 : ( first > second ? 1 : 0 );
		}
	}

}
