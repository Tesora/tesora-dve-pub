package com.tesora.dve.upgrade.versions;

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
import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.InformationCallback;
import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.db.mysql.MysqlNativeType;
import com.tesora.dve.db.mysql.MysqlNativeType.MysqlType;
import com.tesora.dve.db.mysql.MysqlNativeTypeCatalog;
import com.tesora.dve.db.mysql.common.ColumnAttributes;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.types.BasicType;

public class DirectInfoSchema extends ComplexCatalogVersion {

	public DirectInfoSchema(int version) {
		super(version, true);
	}

	/*
	 * In the previous version, user column has:
	 * 
	 * auto_generated bit(1),
	 * nullable bit(1),
	 * on_update int(11),
	 * default_value_is_constant int(11),
	 * native_type_modifiers varchar(255),
	 * has_default_value
	 * 
	 * these all go away, and are replaced by
	 * 
	 * es_universe longtext
	 * flags int(11) not null
	 * 
	 * I think we can pretty much compute the flags from the persistent data directly
	 * the issue is getting the es_universe.
	 * 
	 * in the old way, we put the whole enum/set decl in native_type_name
	 * native_type_modifiers would have unsigned, on update current timestamp, zerofill
	 */
	
	@Override
	public void upgrade(DBHelper helper, InformationCallback stdout)
			throws PEException {
		// add our new columns first
		stdout.println("Adding new columns to user_column");
		execQuery(helper,"alter table user_column add column `es_universe` longtext");
		execQuery(helper,"alter table user_column add column `flags` int(11)");
		convert(helper,stdout);
		stdout.println("Removing columns auto_generated, nullable, on_update, default_value_is_constant, native_type_modifiers");
		for(String c : new String[] { "auto_generated", "nullable", "on_update", "default_value_is_constant", "native_type_modifiers", "has_default_value" }) {
			execQuery(helper,String.format("alter table user_column drop column `%s`",c));
		}
		stdout.println("Marking flags not null");
		execQuery(helper,"alter table user_column modify `flags` int(11) not null");
		stdout.println("changing native_type_name to a varchar from a longtext");
		execQuery(helper,"alter table user_column modify native_type_name varchar(255) not null");
	}

	private void convert(DBHelper helper, InformationCallback stdout) throws PEException {
		stdout.println("Obtaining old column information");
		List<Column> columns = getUserColumns(helper);
		stdout.println("Converting columns");
		try {
			helper.prepare(Column.updateStatement);
		} catch (SQLException sqle) {
			throw new PEException("Unable to prepare update statement",sqle);
		}
		MysqlNativeTypeCatalog ntc = (MysqlNativeTypeCatalog) Singletons.require(DBNative.class).getTypeCatalog();
		for(int i = 0; i < columns.size(); i++) {
			if (i % 100 == 0) 
				stdout.println("Converted " + i + " columns");
			Column c = columns.get(i);
			try {
				helper.executePrepared(c.getParams(ntc));
			} catch (SQLException sqle) {
				throw new PEException("Unable to update column with id " + c.getID(), sqle);
			}
		}
	}
	
	private List<Column> getUserColumns(DBHelper helper) throws PEException {
		List<Column> out = new ArrayList<Column>(1024);
		ResultSet rs = null;
		try {
			if (helper.executeQuery("select uc.user_column_id, uc.data_type, uc.native_type_name, uc.native_type_modifiers, "
					+"uc.has_default_value, uc.on_update, uc.nullable, uc.auto_generated, uk.constraint_type, uk.index_type "
					+"from user_column uc "
					+"left join user_key_column ukc on uc.user_column_id = ukc.src_column_id "
					+"left join user_key uk on ukc.key_id = uk.key_id "
					+"order by uc.user_column_id ")) {
				rs = helper.getResultSet();
				int cid = -1;
				Column last = null;
				while(rs.next()) {
					int id = rs.getInt(1);
					if (cid != id) {
						cid = id;
						int datatype = rs.getInt(2);
						String typeName = rs.getString(3);
						String typeModifiers = rs.getString(4);
						boolean def = rs.getBoolean(5);
						boolean upd = rs.getInt(6) != 0;
						boolean nullable = rs.getBoolean(7);
						boolean autoinc = rs.getBoolean(8);
						last = new Column(id,typeName,typeModifiers,datatype,def,upd,nullable,autoinc);
						out.add(last);
					}
					String cons = rs.getString(9);
					String index = rs.getString(10);
					if (index != null) {
						if (cons != null) {
							if (ConstraintType.FOREIGN.name().equals(cons))
								continue;
							if (ConstraintType.PRIMARY.name().equals(cons))
								last.withKey(ColumnAttributes.PRIMARY_KEY_PART);
							if (ConstraintType.UNIQUE.name().equals(cons))
								last.withKey(ColumnAttributes.UNIQUE_KEY_PART);
						}
						last.withKey(ColumnAttributes.KEY_PART);
					}
				}
			}
		} catch (SQLException sqle) {
			throw new PEException("Unable to obtain user column ids", sqle);
		} finally {
			if (rs != null) try {
				rs.close();
			} catch (SQLException sqle) {
				// whatevs
			}
		}
		
		return out;
	}

	private static class Column {
		
		private final boolean auto_generated;
		private final boolean nullable;
		private final boolean on_update;
		private final boolean def;
		private final String native_type_modifiers;
		private final String native_type_name;
		private final int data_type;
		private final int id;
		private int key;
		
		public Column(int ucid, String typeName, String typeModifiers, int datatype, 
				boolean def, boolean onupdate, boolean nullable, boolean autoinc) {
			this.id = ucid;
			this.native_type_name = typeName;
			this.native_type_modifiers = typeModifiers;
			this.data_type = datatype;
			this.on_update = onupdate;
			this.nullable = nullable;
			this.auto_generated = autoinc;
			this.def = def;
			this.key = 0;
		}
		
		public void withKey(int val) {
			key = ColumnAttributes.set(key, ColumnAttributes.KEY_PART);
			// or in the new value
			key |= val;
		}
		
		
		public static final String updateStatement =
				"update user_column set flags = ?, native_type_name = ?, es_universe = ? where user_column_id = ?";
		
		// have to update native_type_name, flags, es_universe
		public List<Object> getParams(MysqlNativeTypeCatalog types) throws PEException {
			// fun times
			MysqlNativeType mnt = (MysqlNativeType) types.findType(native_type_name, true); 
			int flags = mnt.getDefaultColumnAttrFlags();
			if (def)
				flags = ColumnAttributes.set(flags, ColumnAttributes.HAS_DEFAULT_VALUE);
			if (!nullable)
				flags = ColumnAttributes.set(flags, ColumnAttributes.NOT_NULLABLE);
			if (auto_generated)
				flags = ColumnAttributes.set(flags, ColumnAttributes.AUTO_INCREMENT);
			if (on_update)
				flags = ColumnAttributes.set(flags, ColumnAttributes.ONUPDATE);
			// if part of a primary key, that wins
			if (ColumnAttributes.isSet(key, ColumnAttributes.PRIMARY_KEY_PART)) {
				flags = ColumnAttributes.set(flags, ColumnAttributes.PRIMARY_KEY_PART);
			} else if (ColumnAttributes.isSet(key, ColumnAttributes.UNIQUE_KEY_PART)) {
				// not pk but uk
				flags = ColumnAttributes.set(flags, ColumnAttributes.UNIQUE_KEY_PART);
			}
			if (ColumnAttributes.isSet(key, ColumnAttributes.KEY_PART)) {
				flags = ColumnAttributes.set(flags, ColumnAttributes.KEY_PART);
			}
			String actualNativeTypeName = native_type_name;
			String universe = "";
			if (mnt.getMysqlType() == MysqlType.SET || mnt.getMysqlType() == MysqlType.ENUM) {
				int leftParen = native_type_name.indexOf('(');
				int rparen = native_type_name.lastIndexOf(')');
				universe = native_type_name.substring(leftParen+1,rparen);
				if (mnt.getMysqlType() == MysqlType.SET) {
					actualNativeTypeName = "set";
				} else {
					actualNativeTypeName = "enum";
				}
			}
			// finally, yank out whatever is in the type modifiers
			if (native_type_modifiers != null) {
				if (native_type_modifiers.contains(MysqlNativeType.MODIFIER_UNSIGNED))
					flags = ColumnAttributes.set(flags, ColumnAttributes.UNSIGNED);
				if (native_type_modifiers.contains(MysqlNativeType.MODIFIER_ZEROFILL))
					flags = ColumnAttributes.set(flags, ColumnAttributes.ZEROFILL);
				if (native_type_modifiers.toLowerCase().contains("binary")) 
					flags = ColumnAttributes.set(flags, ColumnAttributes.BINARY);
				// the comparator is stored in the universe
				int offset = native_type_modifiers.indexOf(BasicType.COMPARISON_TAG);
				if (offset > -1) {
					int boundary = offset + BasicType.COMPARISON_TAG.length();
					int nextSpace = native_type_modifiers.indexOf(" ", boundary);
					String value = native_type_modifiers.substring(boundary,nextSpace);
					universe = String.format("%s %s %s",universe,BasicType.COMPARISON_TAG,value);
				}
			}
			
			ArrayList<Object> params = new ArrayList<Object>();
			params.add(flags);
			params.add(actualNativeTypeName);
			params.add(universe);
			params.add(id);
			return params;
		}
				
		public int getID() {
			return id;
		}
	}
	
}
