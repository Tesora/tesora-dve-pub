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

import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.schema.PEViewTable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class ViewInformationSchemaTable extends DirectInformationSchemaTable {

	private final PEViewTable backing;
	
	public ViewInformationSchemaTable(SchemaContext sc, InfoView view, PEViewTable viewTab,
			UnqualifiedName tableName,
			UnqualifiedName pluralTableName,
			boolean privileged, boolean extension,
			List<DirectColumnGenerator> columnGenerators) {
		super(sc,view,viewTab.getColumns(sc),tableName,pluralTableName,privileged,extension,columnGenerators);
		this.backing = viewTab;
	}
	
	public PEViewTable getBackingView() {
		return backing;
	}
	


}
