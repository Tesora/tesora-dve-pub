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

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.common.ShowSchema.GenerationSite;
import com.tesora.dve.sql.infoschema.AbstractInformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.SelectStatement;

public class ShowGenerationSiteInformationSchemaTable extends
		ShowInformationSchemaTable {

	protected InformationSchemaColumnView secondaryOrderByColumn;

	public ShowGenerationSiteInformationSchemaTable(
			LogicalInformationSchemaTable basedOn) {
		super(basedOn, new UnqualifiedName("generation site"), new UnqualifiedName("generation sites"), true, true);
		orderByColumn = new InformationSchemaColumnView(InfoView.SHOW, basedOn.lookup("group_name"), 
				new UnqualifiedName(ShowSchema.GenerationSite.NAME)) {
			@Override
			public boolean isIdentColumn() { return true; }
			@Override
			public boolean isOrderByColumn() { return true; }
		}; 
		
		addColumn(null,orderByColumn);
		addColumn(null, new InformationSchemaColumnView(InfoView.SHOW, basedOn.lookup("version"), 
				new UnqualifiedName(GenerationSite.VERSION)));
		secondaryOrderByColumn = new InformationSchemaColumnView(InfoView.SHOW, basedOn.lookup("site_name"), 
				new UnqualifiedName(GenerationSite.SITE));
		addColumn(null, secondaryOrderByColumn);
	}

	@Override
	public boolean requiresPriviledge() {
		return true;
	}

	@Override
	public boolean isExtension() {
		return true;
	}

	@Override
	protected void validate(SchemaView ofView) {
		// no validation
	}

	@Override
	public void addSorting(SelectStatement ss, TableInstance ti) {
		super.addSorting(ss, ti);
		
		if (secondaryOrderByColumn != null) {
			SortingSpecification sort = new SortingSpecification(new ColumnInstance(null,secondaryOrderByColumn,ti),true);
			sort.setOrdering(Boolean.TRUE);
			ss.getOrderBysEdge().add(sort);
		}
	}
}
