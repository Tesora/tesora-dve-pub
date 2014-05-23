package com.tesora.dve.db;

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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.ArrayListFactory;
import com.tesora.dve.common.HashMapFactory;
import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.TwoDimensionalMultiMap;
import com.tesora.dve.db.mysql.MyFieldType;
import com.tesora.dve.exceptions.PEException;

public abstract class NativeTypeCatalog implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public static final class NativeTypeByPrecisionLookup {

		public static enum MyFieldDataKind {
			ANY,
			BINARY,
			TEXT
		}

		private static final Comparator<NativeType> NATIVE_TYPE_BY_PRECISION = new Comparator<NativeType>() {
			@Override
			public int compare(NativeType a, NativeType b) {
				return Long.compare(a.getPrecision(), b.getPrecision());
			}
		};

		private final TwoDimensionalMultiMap<Integer, MyFieldDataKind, NativeType> myFieldLookupTable = new TwoDimensionalMultiMap<Integer, MyFieldDataKind, NativeType>(
				new HashMapFactory<Integer, MultiMap<MyFieldDataKind, NativeType>>(),
				new HashMapFactory<MyFieldDataKind, Collection<NativeType>>(),
				new ArrayListFactory<NativeType>());
		
		protected NativeTypeByPrecisionLookup() {
		}

		public NativeType findSmallestSuitable(final int nativeTypeId, final MyFieldDataKind dataKind, final long typeLength, final Set<NativeType> ignored) {
			final List<NativeType> suitableTypes = (List<NativeType>) this.myFieldLookupTable.get(nativeTypeId, dataKind);
			return findSmallestSuitableType(typeLength, suitableTypes, ignored);
		}

		/**
		 * Find the index of the first element (e) such that e.getPrecision() >=
		 * typeLength.
		 * If there is only one compatible type return that.
		 * 
		 * @param types
		 *            Searched elements sorted by type precision.
		 */
		private NativeType findSmallestSuitableType(final long typeLength, final List<NativeType> types, final Set<NativeType> ignored) {
			final int numTypes = types.size();
			if (numTypes == 1) {
				return types.get(0);
			} else if (numTypes > 1) {
				for (final NativeType type : types) {
					if (!ignored.contains(type) && (type.getPrecision() >= typeLength)) {
						return type;
					}
				}
			}

			return null;
		}

		private void reload(final List<NativeType> types) {
			Collections.sort(types, NATIVE_TYPE_BY_PRECISION);
			reinitializeLookupTable(types);
		}

		private void reinitializeLookupTable(final List<NativeType> types) {
			this.myFieldLookupTable.clear();
			for (final NativeType type : types) {
				addTypeByNativeTypeId(type);
			}
		}

		private void addTypeByNativeTypeId(final NativeType type) {
			final int nativeTypeId = type.getNativeTypeId();

			this.myFieldLookupTable.put(nativeTypeId, MyFieldDataKind.ANY, type);

			if (type.isBinaryType()) {
				this.myFieldLookupTable.put(nativeTypeId, MyFieldDataKind.BINARY, type);
			}

			if (type.isStringType()) {
				this.myFieldLookupTable.put(nativeTypeId, MyFieldDataKind.TEXT, type);
			}
		}
	}

	protected List<NativeType> loadedTypes;
	protected Map<String, NativeType> typesByName;
	protected MultiMap<Integer, NativeType> typesByDataType;
	protected List<String> jdbcTypeNames;
	protected NativeTypeByPrecisionLookup typesByPrecision;

	public NativeTypeCatalog() {
		loadedTypes = new ArrayList<NativeType>();
		typesByName = new LinkedHashMap<String, NativeType>();
		typesByDataType = new MultiMap<Integer, NativeType>();
		jdbcTypeNames = new ArrayList<String>();
		typesByPrecision = new NativeTypeByPrecisionLookup();
	}

	protected void addType(NativeType nativeType) {
		loadedTypes.add(nativeType);
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
	}

	public void load() throws PEException {
		addTypes();
		initializeByPrecisionLookup(new ArrayList<NativeType>(this.loadedTypes));
	}

	protected abstract void addTypes() throws PEException;

	private void initializeByPrecisionLookup(final List<NativeType> types) {
		this.typesByPrecision.reload(types);
	}

	public NativeType findType(String name, boolean except) throws PEException {
		NativeType nativeType = typesByName.get(NativeType.fixName(name, true));

		if (except && (nativeType == null)) {
			throw new PEException("Unknown type: '" + name + "'");
		}

		return nativeType;
	}
	
	public NativeType findType(int dataType, boolean except) throws PEException {
		NativeType nativeType = null;
		Collection<NativeType> nativeTypes = typesByDataType.get(dataType);
		if (nativeTypes != null && nativeTypes.size() > 0) {
			nativeType = ((List<NativeType>) nativeTypes).get(0);
		}
		if (except && (nativeType == null)) {
			throw new PEException("Unknown data type code: " + dataType);
		}

		return nativeType;
	}
	
	public abstract NativeType findType(final MyFieldType mft, final int flags, final long maxLength, final boolean except) throws PEException;

	protected NativeType findSmallestSuitableType(final int nativeTypeId, final NativeTypeByPrecisionLookup.MyFieldDataKind dataKind, final long maxLength,
			final Set<NativeType> ignored) {
		return this.typesByPrecision.findSmallestSuitable(nativeTypeId, dataKind, maxLength, ignored);
	}
	
	public int size() {
		return this.loadedTypes.size();
	}
	
	public Set<Map.Entry<String, NativeType>> getTypeCatalogEntries() {
		return this.typesByName.entrySet();
	}

	public Map<String, NativeType> getTypesByName() {
		return this.typesByName;
	}
	
	public List<String> getJdbcTypeNames() {
		return this.jdbcTypeNames;
	}
}
