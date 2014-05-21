// OS_STATUS: public
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

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.logical.catalog.DynamicPolicyCatalogInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class ShowDynamicPolicyInformationSchemaTable extends
		ShowInformationSchemaTable {

	private InformationSchemaColumnView nameColumn;
	
	public ShowDynamicPolicyInformationSchemaTable(DynamicPolicyCatalogInformationSchemaTable backing) {
		super(backing, new UnqualifiedName("dynamic site policy"), new UnqualifiedName("dynamic site policies"), 
				false, true);
	}

	@Override
	public void prepare(SchemaView schemaView, DBNative dbn) {
		nameColumn = new InformationSchemaColumnView(InfoView.SHOW, backing.lookup("name"), new UnqualifiedName("Name")) {
			@Override
			public boolean isIdentColumn() { return true; }
			@Override
			public boolean isOrderByColumn() { return true; }
		};
		addColumn(null,nameColumn);
		String[] names = new String[] { "strict", "strict", 
				"aggregate_class", "agg_class", "aggregate_count", "agg_count", "aggregate_provider", "agg_provider",
				"small_class", "sm_class", "small_count", "sm_count", "small_provider", "sm_provider",
				"medium_class", "med_class", "medium_count", "med_count", "medium_provider", "med_provider",
				"large_class", "l_class", "large_count", "l_count", "large_provider", "l_provider" };
		int tot = names.length / 2;
		for(int i = 0; i < tot; i++) 
			addColumn(null, new InformationSchemaColumnView(InfoView.SHOW, backing.lookup(names[2*i + 1]), new UnqualifiedName(names[2*i])));
	}
}
