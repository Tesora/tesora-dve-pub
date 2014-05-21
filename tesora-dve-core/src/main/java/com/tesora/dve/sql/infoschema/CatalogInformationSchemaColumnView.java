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
import com.tesora.dve.sql.schema.UnqualifiedName;

public class CatalogInformationSchemaColumnView extends
		InformationSchemaColumnView {

	protected ColumnView anno;
	
	public CatalogInformationSchemaColumnView(ColumnView cv, LogicalInformationSchemaColumn basedOn) {
		super(cv.view(), basedOn, new UnqualifiedName(cv.name()));
		anno = cv;
	}

	public CatalogInformationSchemaColumnView(CatalogInformationSchemaColumnView ciscv) {
		super(ciscv);
		anno = ciscv.anno;
	}
	
	@Override
	public InformationSchemaColumnView copy() {
		return new CatalogInformationSchemaColumnView(this);
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
