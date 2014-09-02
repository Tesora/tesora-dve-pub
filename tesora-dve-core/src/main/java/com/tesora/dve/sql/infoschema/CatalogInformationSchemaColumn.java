package com.tesora.dve.sql.infoschema;

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

import com.tesora.dve.sql.infoschema.annos.ColumnView;
import com.tesora.dve.sql.infoschema.computed.BackedComputedInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.computed.ComputedInformationSchemaColumn;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class CatalogInformationSchemaColumn extends
		BackedComputedInformationSchemaColumn {

	protected ColumnView anno;
	
	public CatalogInformationSchemaColumn(ColumnView cv, LogicalInformationSchemaColumn basedOn) {
		super(cv.view(), basedOn, new UnqualifiedName(cv.name()));
		anno = cv;
	}

	public CatalogInformationSchemaColumn(CatalogInformationSchemaColumn ciscv) {
		super(ciscv);
		anno = ciscv.anno;
	}
	
	@Override
	public ComputedInformationSchemaColumn copy(InformationSchemaColumnAdapter givenAdapter) {
		ComputedInformationSchemaColumn out = new CatalogInformationSchemaColumn(this);
		if (givenAdapter != null)
			out.setAdapter(givenAdapter);
		return out;
	}
	
	@Override
	public boolean isOrderByColumn() {
		return anno.orderBy();
	}
	
	@Override
	public boolean isIdentColumn() {
		return anno.ident();
	}
	
	@Override
	public boolean requiresPrivilege() {
		return anno.priviledged();
	}

	@Override
	public boolean isExtension() {
		return anno.extension();
	}
	
	@Override
	public boolean isVisible() {
		return anno.visible();
	}
	
	@Override
	public boolean isInjected() {
		return anno.injected();
	}
}
