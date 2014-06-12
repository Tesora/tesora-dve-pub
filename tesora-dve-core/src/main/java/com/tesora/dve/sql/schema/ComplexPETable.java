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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.common.catalog.PersistentTable;
import com.tesora.dve.common.catalog.TableState;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.schema.modifiers.TableModifier;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.IsInstance;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;
import com.tesora.dve.sql.util.UnaryPredicate;

// specifically for handling tables created via create table as select and 
// userland temporary tables.  we use a separate class so that we can compose both
// behaviors.
public class ComplexPETable extends PETable implements AutoIncrement {

	// for a temporary table I think we want to use a qualified name here maybe
	public ComplexPETable(SchemaContext pc, Name name,
			List<TableComponent<?>> fieldsAndKeys, DistributionVector dv,
			List<TableModifier> modifier, PEPersistentGroup defStorage,
			PEDatabase db, TableState theState) {
		super(pc, name, fieldsAndKeys, dv, modifier, defStorage, db, theState);
		// TODO Auto-generated constructor stub
	}

	private ListSet<ComplexTableType> types = new ListSet<ComplexTableType>();

	public ComplexPETable withTemporaryTable(SchemaContext sc) {
		TemporaryTableType ttt = new TemporaryTableType();
		ttt.setDatabaseName(super.getDatabaseName(sc));
		if (hasAutoInc())
			ttt.setHasAutoIncrement(0);
		types.add(ttt);
		return this;
	}
	
	public ComplexPETable withCTA() {
		types.add(new CTATableType());
		return this;
	}
	
	@Override
	public Name getDatabaseName(SchemaContext sc) {
		for(ComplexTableType ctt : types) {
			Name n = ctt.getDatabaseName();
			if (n != null)
				return n;
		}
		return super.getDatabaseName(sc);
	}

	public void setDatabaseName(Name n) {
		for(ComplexTableType ctt : types) {
			ctt.setDatabaseName(n);
		}
	}
	
	@Override
	public boolean mustBeCreated() {
		return Functional.any(types, ComplexTableType.created);
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
	
	@Override
	public UserTable persistTree(SchemaContext sc, boolean forRefresh) throws PEException {
		if (isUserlandTemporaryTable())
			return null;
		return super.persistTree(sc, forRefresh);
	}
	
	@Override
	public PETable recreate(SchemaContext sc, String decl, LockInfo li) {
		PETable recreated = super.recreate(sc,decl, li).asTable();
		if (isUserlandTemporaryTable()) {
			// copy over the autoinc value
			ComplexPETable pettab = (ComplexPETable) recreated;
			TemporaryTableType mtt = getTemporaryTableType();
			TemporaryTableType ytt = pettab.getTemporaryTableType();
			if (mtt.hasAutoIncrement()) {
				ytt.setHasAutoIncrement(mtt.readAutoIncrBlock(null));
			}
		}
		return recreated;
	}
	
	private TemporaryTableType getTemporaryTableType() {
		List<ComplexTableType> any = Functional.select(types, isTemporaryTable);
		if (any.isEmpty())
			return null;
		return (TemporaryTableType) any.get(0);
	}
	
	
	private static final IsInstance<ComplexTableType> isTemporaryTable = 
			new IsInstance<ComplexTableType>(TemporaryTableType.class);
			
	@Override
	public boolean isUserlandTemporaryTable() {
		return Functional.any(types, isTemporaryTable);
	}
	
	public static abstract class ComplexTableType {
		
		public abstract boolean hasPersistentTable();
		
		public abstract boolean mustBeCreated();
		
		public void setDatabaseName(Name n) {
			// default does nothing
		}
		
		public Name getDatabaseName() {
			return null;
		}
		
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

	}

	public static class TemporaryTableType extends ComplexTableType implements AutoIncrement {

		private Name dbName;
		
		private long autoincValue = -1; // unused
		
		@Override
		public boolean hasPersistentTable() {
			return true;
		}

		@Override
		public boolean mustBeCreated() {
			return false;
		}

		@Override
		public void setDatabaseName(Name n) {
			dbName = n;
		}
		
		@Override
		public Name getDatabaseName() {
			return dbName;
		}

		@Override
		public long getNextAutoIncrBlock(SchemaContext sc, long blockSize) {
			long current = autoincValue;
			autoincValue += blockSize;
			return current;
		}

		@Override
		public long readAutoIncrBlock(SchemaContext sc) {
			return autoincValue;
		}

		@Override
		public void removeValue(SchemaContext sc, long value) {
			if (value > autoincValue) {
				autoincValue = value+1;
			}
		}
		
		public void setHasAutoIncrement(long v) {
			autoincValue = v;
		}
		
		public boolean hasAutoIncrement() {
			return autoincValue > -1;
		}
	}

	@Override
	public long getNextAutoIncrBlock(SchemaContext sc, long blockSize) {
		TemporaryTableType ttt = getTemporaryTableType();
		if (ttt == null) return -1;
		return ttt.getNextAutoIncrBlock(sc, blockSize);
	}

	@Override
	public long readAutoIncrBlock(SchemaContext sc) {
		TemporaryTableType ttt = getTemporaryTableType();
		if (ttt == null) return -1;
		return ttt.readAutoIncrBlock(sc);
	}

	@Override
	public void removeValue(SchemaContext sc, long value) {
		TemporaryTableType ttt = getTemporaryTableType();
		if (ttt == null) return;
		ttt.removeValue(sc, value);
	}
	
	// to say that this is a leaky vacuum chamber is an understatement.
	public List<ResultRow> getShowColumns(final SchemaContext sc, String likeExpr) {
		// religious expletive is this vacuum chamber busted
		
		Pattern filtered = null;
		if (likeExpr != null)
			filtered = PEStringUtils.buildSQLPattern(likeExpr);

		ArrayList<ResultRow> out = new ArrayList<ResultRow>();
		for(PEColumn pec : getColumns(sc)) {
			if (filtered != null) {
				String n = pec.getName().getUnquotedName().get();
				Matcher m = filtered.matcher(n);
				if (!m.matches()) 
					continue;
			}
			out.add(pec.buildRow(sc));
		}
		return out;		
	}
	
	public List<ResultRow> getShowKeys(SchemaContext sc) {
		ArrayList<ResultRow> out = new ArrayList<ResultRow>();
		for(PEKey pek : getKeys(sc)) {
			List<ResultRow> sub = pek.buildRow(sc);
			if (sub == null) continue;
			out.addAll(sub);
		}
		return out;
	}
	
}
