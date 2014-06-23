package com.tesora.dve.sql.schema;

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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;

// Temporary tables have wierd lookup rules.  They appear to be:
// + a temporary table always hides a persistent table
// + always schema extend unqualified names with the current db
public class TemporaryTableSchema implements Schema<ComplexPETable> {

	final HashMap<QualifiedName, ComplexPETable> tables = new HashMap<QualifiedName, ComplexPETable>();
	
	private static final UnqualifiedName unnamed = new UnqualifiedName("");
	
	@Override
	public ComplexPETable addTable(SchemaContext sc, ComplexPETable t) {
		Name encName = t.getDatabaseName(sc);
		QualifiedName lookupName = buildLookupName(encName,t.getName());
		return tables.put(lookupName, t);
	}

	public void removeTable(SchemaContext sc, ComplexPETable t) {
		Name encName = t.getDatabaseName(sc);
		QualifiedName lookupName = buildLookupName(encName,t.getName());
		tables.remove(lookupName);
	}
	
	private static QualifiedName buildLookupName(Name enc, Name tname) {
		return new QualifiedName(enc.getUnquotedName().getUnqualified(),
				tname.getUnquotedName().getUnqualified());
	}
	
	@Override
	public Collection<ComplexPETable> getTables(SchemaContext sc) {
		return tables.values();
	}

	public TableInstance buildInstance(SchemaContext sc, Name n) {
		QualifiedName ln = null;
		if (n.isQualified()) {
			QualifiedName qn = (QualifiedName) n;
			ln = buildLookupName(qn.getNamespace(),qn.getUnqualified());
		} else {
			Database<?> db = sc.getCurrentDatabase(false);
			if (db == null)
				return null;
			ln = buildLookupName(db.getName(),n);
		}
		ComplexPETable matching = tables.get(ln);
		if (matching != null)
			return new TableInstance(matching,sc.getOptions().isResolve());
		return null;
	}
	
	@Override
	public TableInstance buildInstance(SchemaContext sc, UnqualifiedName n,
			LockInfo lockType, boolean domtchecks) {
		throw new SchemaException(Pass.SECOND, "Invalid lookup method for temporary tables");
	}

	@Override
	public TableInstance buildInstance(SchemaContext sc, UnqualifiedName n,
			LockInfo lockType) {
		throw new SchemaException(Pass.SECOND, "Invalid lookup method for temporary tables");
	}

	@Override
	public UnqualifiedName getSchemaName(SchemaContext sc) {
		return unnamed;
	}

	public boolean isEmpty() {
		return tables.isEmpty();
	}
	
	// used in connection close
	public List<String> getQualifiedTemporaryTableNames() {
		return Functional.apply(tables.keySet(), new UnaryFunction<String,QualifiedName>() {

			@Override
			public String evaluate(QualifiedName object) {
				return object.get();
			}
			
		});
	}
	
}
