// OS_STATUS: public
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

import com.tesora.dve.sql.infoschema.engine.ScopedColumnInstance;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.types.Type;

public abstract class SyntheticLogicalInformationSchemaColumn extends
		LogicalInformationSchemaColumn {

	public SyntheticLogicalInformationSchemaColumn(UnqualifiedName columnName,
			Type t) {
		super(columnName, t);		
	}

	public boolean matches(ColumnInstance subject) {
		return (subject.getColumn() == this);
	}
	
	public abstract LanguageNode explode(ColumnInstance subject);
	
	protected ColumnInstance buildColumnInstance(ColumnInstance subject, LogicalInformationSchemaColumn lisc) {
		if (subject instanceof ScopedColumnInstance) {
			ScopedColumnInstance sci = (ScopedColumnInstance) subject;
			return new ScopedColumnInstance(lisc,sci.getRelativeTo());
		} else {
			return new ColumnInstance(lisc,subject.getTableInstance());
		}
	}
	
	public Object getValue() {
		return null;
	}
}
