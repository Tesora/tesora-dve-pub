package com.tesora.dve.sql.statement.ddl;

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

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.CacheType;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep;
import com.tesora.dve.sql.transform.execution.EmptyExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.SimpleDDLExecutionStep;

public abstract class DDLStatement extends Statement {

	protected boolean isDVEOnly;
	
	public DDLStatement(boolean dveOnly) {
		super(null);
		isDVEOnly = dveOnly;
	}

	public boolean isDVEOnly() {
		return isDVEOnly;
	}
		
	@Override
	public boolean isDDL() { return true; }
	@Override
	public boolean isDML() { return false; }
	
	@Override
	public void normalize(SchemaContext sc) {
		// does nothing; may want to revisit this
	}	
	
	public List<CatalogEntity> getCatalogObjects(SchemaContext pc) throws PEException {
		return Collections.emptyList();
	}
	
	public List<CatalogEntity> getDeleteObjects(SchemaContext pc) throws PEException {
		return Collections.emptyList();
	}
	
	public abstract CatalogModificationExecutionStep.Action getAction();
	public abstract Persistable<?,?> getRoot();
	public abstract CacheInvalidationRecord getInvalidationRecord(SchemaContext sc);
	
	@Override
	public void plan(SchemaContext sc, ExecutionSequence es) throws PEException {
		normalize(sc);
		ExecutionStep s = buildStep(sc);
		if (s == null) s = new EmptyExecutionStep(0,"already exists - " + getSQL(sc));
		es.append(s);
	}

	@Override
	protected void preplan(SchemaContext pc, ExecutionSequence es,boolean explain) throws PEException {
		// make sure we're using a mutable source
		if (pc.getSource().getType() != CacheType.MUTABLE)
			throw new SchemaException(Pass.PLANNER,"Unable to plan ddl with nonmutable schema");
		// also, if we are in an xa txn we should toss an error
		// well any txn but let's just stick to xa for now
		if (pc.getConnection().isInXATxn())
			throw new SchemaException(Pass.PLANNER,"Unable to plan ddl while in an active XA transaction");
	}
	
	protected ExecutionStep buildStep(SchemaContext sc) throws PEException {
		return new SimpleDDLExecutionStep(getDatabase(sc), getStorageGroup(sc), getRoot(), getAction(), getSQLCommand(sc),
				getDeleteObjects(sc), getCatalogObjects(sc),getInvalidationRecord(sc));
	}
	
	@Override
	public PEDatabase getDatabase(SchemaContext sc) {
		// current database might be info schema - check for that
		Database<?> other = sc.getCurrentDatabase(false);
		if (other instanceof PEDatabase)
			return (PEDatabase)other;
		// for create statements that have no default database (persistent site, persistent group, etc.)
		// accept no database.  if a database was needed we would throw when building the pers rep.
		return null;
	}
	
	@Override	
	public PEStorageGroup getStorageGroup(SchemaContext pc) {
		if (getRoot() instanceof PETable)
			return ((PETable)getRoot()).getStorageGroup(pc);
		if (getRoot() instanceof PEDatabase)
			return ((PEDatabase)getRoot()).getDefaultStorage(pc);
		PEPersistentGroup pesg = pc.getPersistentGroup();
		return pesg;
	}

	@Override
	protected boolean schemaSelfEqual(LanguageNode other) {
		return illegalSchemaSelf(other);
	}

	@Override
	protected int selfHashCode() {
		return illegalSchemaHash();
	}
		
	

	
}
