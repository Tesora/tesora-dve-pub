// OS_STATUS: public
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


import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.db.Emitter;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.transform.execution.DDLQueryExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;

public class SchemaQueryStatement extends DDLStatement {

	private String schemaTag;
	private List<CatalogEntity> entities;
	private IntermediateResultSet results;
	private boolean isPlural;
	private String likeStr;
	
	public SchemaQueryStatement(boolean peOnly, String tag, List<CatalogEntity> ents, boolean isPluralForm, String likeStr) {
		super(peOnly);
		schemaTag = tag;
		entities = ents;
		isPlural = isPluralForm;
		this.likeStr = likeStr;
	}

	public SchemaQueryStatement(boolean peOnly, String tag, IntermediateResultSet results) {
		super(peOnly);
		schemaTag = tag;
		entities = null;
		this.results = results;
		isPlural = false;
		likeStr = null;
	}
	
	@Override
	public Action getAction() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Persistable<?, ?> getRoot() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void plan(SchemaContext pc, ExecutionSequence es) throws PEException {
		if (results != null) {
			es.append(new DDLQueryExecutionStep(schemaTag,results));
		} else {
			es.append(new DDLQueryExecutionStep(schemaTag, entities, isPlural, (pc.getPolicyContext().isMTMode() && !pc.getPolicyContext().isRoot())));
		}
	}
	
	@Override
	protected void preplan(SchemaContext pc, ExecutionSequence es,boolean explain) throws PEException {
	}

	
	@Override
	public String getSQL(SchemaContext sc, Emitter emitter, EmitOptions opts, boolean unused) {
		return "show " + schemaTag + (likeStr == null ? " " : " like " + likeStr); 
	}

	public String getTag() {
		return schemaTag;
	}
	
	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		// does not invalidate
		return null;
	}
}
