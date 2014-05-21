// OS_STATUS: public
package com.tesora.dve.sql.schema.modifiers;

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

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.util.Functional;



public abstract class CreateOptionModifier extends TableModifier {

	public boolean isCreateOption() {
		return true;
	}
	
	public abstract String persist();
	
	// specifically for create table options
	public static TableModifiers decode(String in) {
		if (in == null) return null;
		if ("".equals(in.trim())) return null;
		String[] kvs = in.split(" ");
		TableModifiers out = new TableModifiers();
		for(String s : kvs) {
			String[] bits = s.split("=");
			out.setModifier(decodeModifier(bits[0],bits[1]));
		}
		return null;
	}

	public static CreateOptionModifier decodeModifier(String key, String value) {
		TableModifierTag tmt = TableModifierTag.valueOf(key.toUpperCase());
		switch(tmt) {
		case MAX_ROWS:
			return new MaxRowsModifier(Long.parseLong(value));
		case CHECKSUM:
			return new ChecksumModifier(Integer.parseInt(value));
		case ROW_FORMAT:
			return new RowFormatTableModifier(value);
		default:
			throw new SchemaException(Pass.PLANNER, "Unknown create table option: " + tmt);
		}
		
	}
	
	public static String build(TableModifiers mods) {
		ArrayList<String> values = new ArrayList<String>(); 
		for(TableModifierTag tmt : TableModifierTag.values()) {
			if (tmt.isCreateOption()) {
				TableModifier tm = mods.getModifier(tmt);
				if (tm == null) continue;
				CreateOptionModifier com = (CreateOptionModifier) tm;
				values.add(com.persist());
			}
		}
		return Functional.join(values, " ");
	}

	public static TableModifiers combine(TableModifiers e, TableModifiers n) {
		if (e == null) return n;
		if (n == null) return e;
		// both exist - take everything from e and override as necessary from n
		TableModifiers out = new TableModifiers();
		for(TableModifierTag tmt : TableModifierTag.values()) {
			TableModifier tm = e.getModifier(tmt);
			TableModifier ntm = n.getModifier(tmt);
			TableModifier ptm = null;
			ptm = ntm;
			if (ptm == null) ptm = tm;
			if (ptm == null) continue;
			out.setModifier(ptm);
		}
		return out;
	}
	
	public static String build(String existing, TableModifiers tm) {
		TableModifiers emods = decode(existing);
		TableModifiers combined = combine(emods, tm);
		if (combined == null) return null;
		return build(combined);
	}
	
}
