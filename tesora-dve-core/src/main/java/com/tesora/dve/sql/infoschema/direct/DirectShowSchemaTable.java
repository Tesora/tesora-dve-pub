package com.tesora.dve.sql.infoschema.direct;

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

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.sql.infoschema.InfoView;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.ShowOptions;
import com.tesora.dve.sql.infoschema.ShowSchemaBehavior;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;

public abstract class DirectShowSchemaTable extends DirectInformationSchemaTable
		implements ShowSchemaBehavior {

	public DirectShowSchemaTable(SchemaContext sc, InfoView view,
			List<PEColumn> cols, UnqualifiedName tableName,
			UnqualifiedName pluralTableName, boolean privileged,
			boolean extension, List<DirectColumnGenerator> columnGenerators) {
		super(sc, view, cols, tableName, pluralTableName, privileged, extension,
				columnGenerators);
	}

	@Override
	public List<CatalogEntity> getLikeSelectEntities(SchemaContext sc,
			String likeExpr, List<Name> scoping, ShowOptions options,
			Boolean overrideRequiresPrivilegeValue) {
		throw new InformationSchemaException("No entities available for direct tables");
	}

}
