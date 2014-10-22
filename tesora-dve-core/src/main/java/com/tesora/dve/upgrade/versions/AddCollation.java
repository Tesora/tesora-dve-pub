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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.BooleanUtils;

import com.tesora.dve.charset.NativeCollation;
import com.tesora.dve.charset.mysql.MysqlNativeCollationCatalog;
import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.InformationCallback;
import com.tesora.dve.exceptions.PEException;

public class AddCollation extends SimpleCatalogVersion {

	public AddCollation(int version) {
		super(version);
	}

	@Override
	public String[] getUpgradeCommands(DBHelper helper) throws PEException {
		return new String[] {
				"alter table character_sets drop column default_collate_name",
				"create table collations (name varchar(32) not null, character_set_name varchar(32) not null, id integer not null, is_default integer not null default '0', is_compiled integer not null default '1', sortlen bigint(3) not null, primary key(id)) ENGINE=InnoDB;"
		};
	}

	@Override
	public void upgrade(DBHelper helper, InformationCallback stdout) throws PEException {
		super.upgrade(helper, stdout);
		
		try {
			List<Object> params = new ArrayList<Object>();

			helper.prepare("insert into collations (id, name, character_set_name, is_default, is_compiled, sortlen) values (?,?,?,?,?,?)");
			
			for(String collationName : MysqlNativeCollationCatalog.DEFAULT_CATALOG.getCollationsCatalogEntriesByName()) {
				NativeCollation nc = MysqlNativeCollationCatalog.DEFAULT_CATALOG.findCollationByName(collationName);
				params.clear();
				params.add(nc.getId());
				params.add(nc.getName());
				params.add(nc.getCharacterSetName());
				params.add(BooleanUtils.toInteger(nc.isDefault()));
				params.add(BooleanUtils.toInteger(nc.isCompiled()));
				params.add(nc.getSortLen());
				helper.executePrepared(params);
			}
		} catch (SQLException sqle) {
			throw new PEException("Unable to insert collation values: " + sqle.getMessage());
		}
	}
}
