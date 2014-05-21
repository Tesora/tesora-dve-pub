// OS_STATUS: public
package com.tesora.dve.sql.infoschema.mysql;

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

import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class MysqlDBInformationSchemaTable extends InformationSchemaTableView {

	public MysqlDBInformationSchemaTable(LogicalInformationSchemaTable basedOn) {
		super(InfoView.MYSQL, basedOn, new UnqualifiedName("db"), null, false, false);
		orderByColumn = new InformationSchemaColumnView(InfoView.MYSQL, basedOn.lookup("db"), new UnqualifiedName("Db")); 
		addColumn(null,new InformationSchemaColumnView(InfoView.MYSQL, basedOn.lookup("host"), new UnqualifiedName("Host")));
		addColumn(null,orderByColumn);
		addColumn(null,new InformationSchemaColumnView(InfoView.MYSQL, basedOn.lookup("user"), new UnqualifiedName("User")));
	}


	@Override
	protected void validate(SchemaView ofView) {
	}

}
