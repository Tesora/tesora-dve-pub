package com.tesora.dve.sql.infoschema.show;

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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchema;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.util.UnaryFunction;

public class ShowView extends SchemaView {

	public ShowView(LogicalInformationSchema lis) {
		super(lis,InfoView.SHOW,
				new UnaryFunction<Name[], InformationSchemaTableView>() {

			@Override
			public Name[] evaluate(InformationSchemaTableView object) {
				return new Name[] { object.getName(), object.getPluralName() };
			}
			
		});

	}

	@Override
	public void buildEntities(CatalogSchema schema, int groupid, int modelid, 
			String charSet, String collation, List<PersistedEntity> acc) throws PEException {	
	}
	
	public ShowInformationSchemaTable lookupTable(UnqualifiedName unq) {
		// temporary
		Object obj = lookup.lookup(unq);
		return (ShowInformationSchemaTable)obj;
	}
}
