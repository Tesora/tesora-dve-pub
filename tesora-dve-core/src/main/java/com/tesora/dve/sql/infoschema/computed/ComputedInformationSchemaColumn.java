package com.tesora.dve.sql.infoschema.computed;

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

import com.tesora.dve.sql.infoschema.InformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnAdapter;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.schema.UnqualifiedName;

public abstract class ComputedInformationSchemaColumn extends InformationSchemaColumn {
	
	public ComputedInformationSchemaColumn(InfoView view, 
			UnqualifiedName nameInView,
			InformationSchemaColumnAdapter adapter) {
		super(view,nameInView, adapter);
	}

	public ComputedInformationSchemaColumn(ComputedInformationSchemaColumn copy) {
		super(copy);
	}
	
	public void freeze() {
		
	}
	
//	@Override
//	public Type getType() {
//		return backing.getType();
//	}

	
	
//	public UserColumn persist(CatalogDAO c, UserTable parent, DBNative dbn) {
//		return backing.persist(c,parent,getName(),dbn);
//	}
	
//	@Override
//	public void buildColumnEntity(CatalogSchema schema, CatalogTableEntity cte, int offset, List<PersistedEntity> acc) throws PEException {
//		acc.add(backing.buildColumnEntity(schema, cte, offset, getName().get()));
//	}
	

}
