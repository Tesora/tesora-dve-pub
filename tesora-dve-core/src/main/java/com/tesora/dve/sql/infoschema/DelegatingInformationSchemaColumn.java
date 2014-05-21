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

import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.infoschema.engine.ScopedColumnInstance;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class DelegatingInformationSchemaColumn extends LogicalInformationSchemaColumn {

	private List<LogicalInformationSchemaColumn> pathToActual;
	
	public DelegatingInformationSchemaColumn(List<LogicalInformationSchemaColumn> actual, UnqualifiedName logicalName) {
		super(logicalName, actual.get(actual.size() - 1).getType()); 
		pathToActual = actual;
	}

	public DelegatingInformationSchemaColumn(LogicalInformationSchemaColumn actual, UnqualifiedName logicalName) {
		this(Collections.singletonList(actual),logicalName);
	}
	
	public LogicalInformationSchemaTable getActualTable() {
		return pathToActual.get(0).getTable();
	}
	
	public List<LogicalInformationSchemaColumn> getPath() {
		return pathToActual;
	}
	
	public ColumnInstance rewriteToActual(TableInstance onTable) {
		ColumnInstance current = null;
		for(LogicalInformationSchemaColumn lisc : pathToActual) {
			if (current == null)
				current = new ColumnInstance(lisc,onTable);
			else {
				current = new ScopedColumnInstance(lisc,current);
			}
		}
		return current;
	}
	
}
