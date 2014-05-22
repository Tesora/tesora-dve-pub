package com.tesora.dve.sql.schema.validate;

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

import java.util.Map;

import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class ForeignKeyValidateResult extends ValidateResult {

	public enum FKValidateKind {
		NO_UNIQUE_KEY,
		NOT_COLOCATED 
	}
	
	PEForeignKey subject;
	PETable targTab;
	boolean error;
	FKValidateKind kind;
	
	// used in mt - we use this to map mangled names into scope names
	Map<PETable, UnqualifiedName> mapping;
	
	public ForeignKeyValidateResult(SchemaContext sc, PEForeignKey pefk, FKValidateKind variety, boolean error) {
		this.subject = pefk;
		this.targTab = pefk.getTargetTable(sc);
		this.error = error;
		this.kind = variety;
		this.mapping = null;
	}
		
	// have to use a target name rather than a scope as the scope may not exist yet
	public void setMTMapping(Map<PETable,UnqualifiedName> visible) {
		mapping = visible;
	}
	
	@Override
	public boolean isError() {
		return error;
	}
	
	private String getTableName(PETable tab) {
		if (mapping != null) {
			UnqualifiedName any = mapping.get(tab);
			if (any != null) 
				return any.getUnquotedName().get();
		}
		return tab.getName().getUnqualified().getUnquotedName().get();
	}
	
	@Override
	public String getMessage(SchemaContext sc) {
		if (this.kind == FKValidateKind.NO_UNIQUE_KEY) {
			String fmt = "Invalid foreign key in table %s: no matching unique key in table %s";
			return String.format(fmt, getTableName(subject.getTable(sc)), getTableName(subject.getTargetTable(sc)));
		} else {
			String fmt = "Invalid foreign key %s.%s: table %s is not colocated with %s";
			String encName = getTableName(subject.getTable(sc));
			return String.format(fmt, encName, subject.getSymbol().get(), encName, getTableName(subject.getTargetTable(sc)));
		}
	}
	
	@Override
	public Persistable<?, ?> getSubject() {
		return subject;
	}

	
}
