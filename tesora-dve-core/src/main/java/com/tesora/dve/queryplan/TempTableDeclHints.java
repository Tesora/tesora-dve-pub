package com.tesora.dve.queryplan;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.common.catalog.IndexType;
import com.tesora.dve.common.catalog.Key;
import com.tesora.dve.common.catalog.KeyColumn;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.sql.util.ListSet;

// hints for the temp table declaration - info we can't figure out
// in the engine alone.
public class TempTableDeclHints extends TableHints {

	// each tuple of index columns becomes an index
	// use a set container to prevent insertion of duplicate keys.
	ListSet<List<String>> indexColumns;

	ListSet<List<String>> uniqueColumns;

	Map<String,TempTableColumnHints> columnHints;
	
	// overriding declaration for columns, by column name
	Map<String,UserColumn> overrideDecls;
	
	public TempTableDeclHints() {
		indexColumns = new ListSet<List<String>>();
		uniqueColumns = new ListSet<List<String>>();
		columnHints = new HashMap<String,TempTableColumnHints>();
		overrideDecls = new HashMap<String,UserColumn>();
	}

	public TempTableColumnHints getHint(String columnName) {
		TempTableColumnHints h = columnHints.get(columnName);
		if (h == null) {
			h = new TempTableColumnHints();
			columnHints.put(columnName, h);
		}
		return h;
	}
	
	public boolean addIndex(final String[] cols) {
		return indexColumns.add(Arrays.asList(cols));
	}
	
	public boolean addIndex(final String col) {
		return indexColumns.add(Collections.singletonList(col));
	}
	
	public boolean addUniqueKey(final String[] cols) {
		return uniqueColumns.add(Arrays.asList(cols));
	}

	public boolean addUniqueKey(final String col) {
		return uniqueColumns.add(Collections.singletonList(col));
	}

	public void addOverrideDecl(String columnName, UserColumn def) {
		overrideDecls.put(columnName, def);
	}
	
	public void addZeroFilled(String col) {
		getHint(col).setZeroFilled();
	}
	
	public void addCollation(String column, String collation) {
		getHint(column).setCollation(collation);
	}

	public void addCharset(String column, String charset) {
		getHint(column).setCharset(charset);
	}
	
	public List<List<String>> getIndexes() {
		return indexColumns;
	}
	
	public List<List<String>> getUniqueKeys() {
		return uniqueColumns;
	}

	@Override
	public void modify(UserTable ut) {
		addKeysOn(ut, indexColumns, null);
		addKeysOn(ut, uniqueColumns, ConstraintType.UNIQUE);

		for(Map.Entry<String, TempTableColumnHints> me : columnHints.entrySet()) {
			String cn = me.getKey();
			TempTableColumnHints ttch = me.getValue();
			UserColumn uc = ut.getUserColumn(cn);
			uc.setZerofill(ttch.isZeroFilled());
			if (ttch.getCharset() != null) {
				uc.setCharset(ttch.getCharset());
			}
			if (ttch.getCollation() != null) {
				uc.setCollation(ttch.getCollation());
			}
		}
		if (!overrideDecls.isEmpty()) {
			for(Map.Entry<String, UserColumn> me : overrideDecls.entrySet()) {
				UserColumn uc = ut.getUserColumn(me.getKey());
				UserColumn dd = me.getValue();
                uc.copyFrom(dd);
			}
		}
	}
	
	private void addKeysOn(final UserTable ut, final List<List<String>> keyDefs, final ConstraintType constraint) {
		if (!keyDefs.isEmpty()) {
			final StringBuilder keyName = new StringBuilder(ut.getName());
			if (constraint != null) {
				keyName.append(constraint.getSQL());
			}
			keyName.append("pekey");
			for (int i = 0; i < keyDefs.size(); ++i) {
				final List<String> keyColumnNames = keyDefs.get(i);
				final int keyLength = keyColumnNames.size();
				final List<KeyColumn> keyColumns = new ArrayList<KeyColumn>(keyLength);
				for (int j = 0; j < keyLength; ++j) {
					final UserColumn uc = ut.getUserColumn(keyColumnNames.get(j));
					final KeyColumn kc = new KeyColumn(uc, null, j + 1,-1L);
					keyColumns.add(kc);
				}
				final int offset = i + 1;
				final Key k = new Key(keyName.append(offset).toString(), IndexType.BTREE, ut, keyColumns, offset);
				k.setConstraint(constraint);
				ut.addKey(k);
			}
		}
	}

	protected static class TempTableColumnHints {
		
		protected boolean zeroFilled;
		protected String collation;
		protected String charset;
		
		public TempTableColumnHints() {
			this.zeroFilled = false;
			this.collation = null;
			this.charset = null;
		}
		
		public void setZeroFilled() {
			zeroFilled = true;
		}
		
		public void setCollation(String c) {
			this.collation = c;
		}
		
		public void setCharset(String charset) {
			this.charset = charset;
		}
		
		public boolean isZeroFilled() {
			return zeroFilled;
		}
		
		public String getCollation() {
			return collation;
		}
		
		public String getCharset() {
			return charset;
		}			
	}
	
}
