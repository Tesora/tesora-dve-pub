package com.tesora.dve.sql.parser;

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

import com.tesora.dve.db.NativeType;
import com.tesora.dve.db.NativeTypeCatalog;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryPredicate;

public class TestMySqlTypes extends TestTypes {

	@Override
	public List<NativeType> getNativeTypes() {
        NativeTypeCatalog ntc = Singletons.require(HostService.class).getDBNative().getTypeCatalog();
		return Functional.select(ntc.getTypesByName().values(), new UnaryPredicate<NativeType>() {

			@Override
			public boolean test(NativeType object) {
				// we now autoconvert numeric into decimal
				if ("numeric".equalsIgnoreCase(object.getTypeName()))
					return false;
				return true;
			}
			
		});
	}
}
