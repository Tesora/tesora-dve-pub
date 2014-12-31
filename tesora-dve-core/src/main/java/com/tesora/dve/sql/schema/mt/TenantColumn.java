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


import java.util.Collections;

import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.modifiers.TypeModifier;
import com.tesora.dve.sql.schema.modifiers.TypeModifierKind;
import com.tesora.dve.sql.schema.types.BasicType;
import com.tesora.dve.sql.schema.types.Type;

public class TenantColumn extends PEColumn {

	public static final String TENANT_COLUMN = "___mtid";
	public static final Type TENANT_COLUMN_TYPE = BasicType.buildType("int", 10,
			Collections.singletonList(new TypeModifier(TypeModifierKind.UNSIGNED)),
			Singletons.require(DBNative.class).getTypeCatalog()).normalize();

	private static final UnqualifiedName tenantColumnName = new UnqualifiedName("`" + TENANT_COLUMN + "`");

	public TenantColumn(SchemaContext sc) {
		super(sc,tenantColumnName,TENANT_COLUMN_TYPE,(short)TENANT_COLUMN_TYPE.getColumnAttributesFlags(),null,null);
		makeNotNullable();
	}
	
	public TenantColumn(UserColumn us, SchemaContext sc) {
		super(us,sc,true);
	}
	
	@Override
	public boolean isTenantColumn() {
		return true;
	}
}
