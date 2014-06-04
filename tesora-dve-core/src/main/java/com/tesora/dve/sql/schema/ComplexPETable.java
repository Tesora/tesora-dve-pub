package com.tesora.dve.sql.schema;

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

import java.util.List;

import com.tesora.dve.common.catalog.PersistentTable;
import com.tesora.dve.common.catalog.TableState;
import com.tesora.dve.sql.schema.modifiers.TableModifier;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryPredicate;

// specifically for handling tables created via create table as select and 
// userland temporary tables.  we use a separate class so that we can compose both
// behaviors.
public class ComplexPETable extends PETable {

	// for a temporary table I think we want to use a qualified name here maybe
	public ComplexPETable(SchemaContext pc, Name name,
			List<TableComponent<?>> fieldsAndKeys, DistributionVector dv,
			List<TableModifier> modifier, PEPersistentGroup defStorage,
			PEDatabase db, TableState theState) {
		super(pc, name, fieldsAndKeys, dv, modifier, defStorage, db, theState);
		// TODO Auto-generated constructor stub
	}

	private ListSet<ComplexTableType> types = new ListSet<ComplexTableType>();

	public ComplexPETable withTemporaryTable() {
		types.add(new TemporaryTableType());
		return this;
	}
	
	public ComplexPETable withCTA() {
		types.add(new CTATableType());
		return this;
	}
	
	@Override
	public boolean mustBeCreated() {
		return Functional.any(types, ComplexTableType.created);
	}
	
	@Override
	public Database<?> getDatabase(SchemaContext sc) {
		if (Functional.all(types, ComplexTableType.enclosed))
			return db.get(sc);
		return null;
	}

	@Override
	public boolean hasDatabase(SchemaContext sc) {
		if (Functional.all(types, ComplexTableType.enclosed))
			return false;
		if (db == null) {
			return false;
		}
		return (db.get(sc) != null);
	}
	
	@Override
	public PersistentTable getPersistentTable(SchemaContext sc) {
		if (cached == null) {
			if (Functional.any(types,ComplexTableType.persistentTable)) {
				cached = new CachedPETable(sc,this);
			}
		}
		return cached;
	}
	
	public static abstract class ComplexTableType {
		
		public abstract boolean hasPersistentTable();
		
		public abstract boolean mustBeCreated();

		// does the table 'live' in a database
		public abstract boolean isEnclosed();
		
		public static final UnaryPredicate<ComplexTableType> persistentTable = new UnaryPredicate<ComplexTableType>() {

			@Override
			public boolean test(ComplexTableType object) {
				return object.hasPersistentTable();
			}
			
		};
		
		public static final UnaryPredicate<ComplexTableType> created = new UnaryPredicate<ComplexTableType>() {

			@Override
			public boolean test(ComplexTableType object) {
				return object.mustBeCreated();
			}
			
		};

		public static final UnaryPredicate<ComplexTableType> enclosed = new UnaryPredicate<ComplexTableType>() {

			@Override
			public boolean test(ComplexTableType object) {
				return object.isEnclosed();
			}
			
		};
		
	}
	
	public static class CTATableType extends ComplexTableType {

		@Override
		public boolean hasPersistentTable() {
			return true;
		}

		@Override
		public boolean mustBeCreated() {
			return true;
		}

		@Override
		public boolean isEnclosed() {
			return true;
		}
		
		
	}

	public static class TemporaryTableType extends ComplexTableType {

		@Override
		public boolean hasPersistentTable() {
			return true;
		}

		@Override
		public boolean mustBeCreated() {
			return false;
		}

		@Override
		public boolean isEnclosed() {
			return false;
		}
		
		
	}
	
}
