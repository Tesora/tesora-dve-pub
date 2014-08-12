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

import com.tesora.dve.errmap.DVEErrors;
import com.tesora.dve.errmap.ErrorInfo;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.util.UnaryProcedure;

public class TableResolver {

	private boolean mtchecks = false;
	
	private UnaryProcedure<Database<?>> onDatabase = null;
	private String qualifiedMissingDBFormat = "No such database: '%s'"; 
	private String unqualifiedMissingDBFormat = "No database specified (unqualified name)";
	private MissingTableFunction missingTable = null;
	
	public TableResolver() {
	}

	public TableResolver withMTChecks() {
		mtchecks = true;
		return this;
	}
	
	public TableResolver withDatabaseFunction(UnaryProcedure<Database<?>> f) {
		onDatabase = f;
		return this;
	}
	
	// accepts a single string 
	public TableResolver withQualifiedMissingDBFormat(String fmt) {
		qualifiedMissingDBFormat = fmt;
		return this;
	}
	
	public TableResolver withUnqualifiedMissingDBFormat(String fmt) {
		unqualifiedMissingDBFormat = fmt;
		return this;
	}
	
	public TableResolver withMissingTableFunction(MissingTableFunction f) {
		missingTable = f;
		return this;
	}
	
	// for resolution.  unifying this down to one spot to handle temporary tables correctly.
	public TableInstance lookupTable(SchemaContext sc, Name n, LockInfo lockType) {
		TableInstance raw = sc.getTemporaryTableSchema().buildInstance(sc, n);
		if (raw != null)
			return raw;
		Database<?> db = null;
		UnqualifiedName tableName = null;
		if (n.isQualified()) {
			QualifiedName qname = (QualifiedName) n;
			UnqualifiedName ofdb = qname.getNamespace();
			tableName = qname.getUnqualified();
			db = sc.findDatabase(ofdb);
			if (db == null)
				throw new SchemaException(Pass.SECOND, String.format(qualifiedMissingDBFormat, ofdb.getUnquotedName().get()));
		} else {
			tableName = (UnqualifiedName) n;
			db = sc.getCurrentDatabase(false);
			if (db == null)
				throw new SchemaException(new ErrorInfo(DVEErrors.NO_DATABASE_SELECTED));
		}
		if (onDatabase != null)
			onDatabase.execute(db);
		Schema<?> schema = db.getSchema();
		raw = schema.buildInstance(sc,tableName.getUnqualified(), lockType, mtchecks);
		if (raw == null && missingTable != null)
			missingTable.onMissingTable(sc, schema, n);
		return raw;
	}
	
	public TableInstance lookupTable(SchemaContext sc, Schema<?> inSchema, Name n, LockInfo lockType) {
		// only for show schema - so don't do the temporary table lookup
		Name tableName = n;
		Schema<?> schema = inSchema;
		if (tableName.isQualified()) {
			QualifiedName qname = (QualifiedName) tableName;
			UnqualifiedName ofdb = qname.getNamespace();
			tableName = qname.getUnqualified();
			Database<?> db = sc.findDatabase(ofdb);
			if (db == null) throw new SchemaException(Pass.SECOND, "No such database: '" + ofdb + "'");
			schema = db.getSchema();
			if (schema == null) 
				throw new SchemaException(Pass.SECOND, "No database specified (qualified name)");
		}
		if (schema == null) 
			throw new SchemaException(Pass.SECOND, "No database specified (unqualified name)");
		TableInstance raw = schema.buildInstance(sc,tableName.getUnqualified(), lockType);
		return raw;
	}

	public interface MissingTableFunction {
		
		public void onMissingTable(SchemaContext sc, Schema<?> schema, Name name);
		
	}

}
