package com.tesora.dve.sql.infoschema.info;

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

import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.AbstractInformationSchema;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.computed.BackedComputedInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.computed.ComputedInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class InfoSchemaGenerationSiteInformationSchemaTable extends
		ComputedInformationSchemaTable {

	public InfoSchemaGenerationSiteInformationSchemaTable(LogicalInformationSchemaTable basedOn) {
		super(InfoView.INFORMATION, basedOn, new UnqualifiedName("generation_site"), null, true, true);
		orderByColumn = new BackedComputedInformationSchemaColumn(InfoView.SHOW, basedOn.lookup("group_name"), new UnqualifiedName("group"));
		addColumn(null,orderByColumn);
		addColumn(null,new BackedComputedInformationSchemaColumn(InfoView.SHOW, basedOn.lookup("version"), new UnqualifiedName("version")));
		addColumn(null,new BackedComputedInformationSchemaColumn(InfoView.SHOW, basedOn.lookup("site_name"), new UnqualifiedName("site")));
	}

	@Override
	protected void validate(AbstractInformationSchema ofView) {
	}

}
