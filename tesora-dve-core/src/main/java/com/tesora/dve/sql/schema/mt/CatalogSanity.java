package com.tesora.dve.sql.schema.mt;

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
import java.util.Collections;
import java.util.List;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.exceptions.PEException;

public final class CatalogSanity {

	@SuppressWarnings("unchecked")
	public static void assertCatalogSanity(CatalogDAO c) throws PEException {
		List<List<Object[]>> values = new ArrayList<List<Object[]>>();
		c.begin();
		for(Checker checker : checkers) {
			values.add(c.nativeQuery(checker.getSQL(), Collections.EMPTY_MAP));
		}
		c.commit();
		StringBuilder buf = new StringBuilder();
		buf.append("Bad catalog state:");
		boolean any = false;
		for(int i = 0; i < checkers.length; i++) {
			List<Object[]> v = values.get(i);
			if (v.isEmpty()) continue;
			any = true;
			for(Object[] r : v) {
				checkers[i].handleResult(r, buf);
			}
		}
		if (any)
			throw new PEException(buf.toString());
	}
	
	
	interface Checker {
		
		String getSQL();
		
		void handleResult(Object[] values, StringBuilder buf);
		
	}
	
	private static final Checker matchingUserKeyAndUserKeyColumn = new Checker() {

		@Override
		public String getSQL() {
			// make sure keys and key columns point to the same table
			return 	"select distinct kut.name keyname, cut.name keycolumnname "
					+"from user_key uk "
					+"inner join user_key_column ukc on uk.key_id = ukc.key_id "
					+"inner join user_column uc on ukc.targ_column_id = uc.user_column_id "
					+"inner join user_table kut on uk.referenced_table = kut.table_id "
					+"inner join user_table cut on uc.user_table_id = cut.table_id "
					+"where uk.referenced_table != uc.user_table_id and uk.constraint_type = 'FOREIGN'";
		}

		@Override
		public void handleResult(Object[] values, StringBuilder buf) {
			buf.append(" key table is ").append(values[0]).append(" but key column table is ").append(values[1]);
		}
				
	};
	
	private static final Checker sharedOnlyReferencesShared = new Checker() {

		@Override
		public String getSQL() {
			// it's invalid for shared tables to point to fixed tables
			return 	"select rut.state rs, rut.name rn, tut.state ts, tut.name tn, uk.constraint_name "
					+"from user_key uk "
					+"inner join user_table rut on uk.user_table_id = rut.table_id "
					+"inner join user_table tut on uk.referenced_table = tut.table_id "
					+"inner join scope s on rut.table_id = s.scope_table_id "
					+"where uk.constraint_type = 'FOREIGN' and rut.state = 'SHARED' and tut.state = 'FIXED'";
		}

		@Override
		public void handleResult(Object[] r, StringBuilder buf) {
			String rs = (String) r[0];
			String rn = (String) r[1];
			String ts = (String) r[2];
			String tn = (String) r[3];
			String cn = (String) r[4];
			buf.append(" table ").append(rn).append(" in state ").append(rs).append(" has fk ").append(cn).append(" that refers to table ").append(tn).append(" in state ").append(ts);
		}
				
	};

	
	private static final Checker[] checkers = new Checker[] {
		matchingUserKeyAndUserKeyColumn,
		sharedOnlyReferencesShared
	};


}
