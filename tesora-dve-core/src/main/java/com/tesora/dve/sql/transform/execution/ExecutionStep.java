// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

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

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.ExplainOptions;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.ValueManager;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryProcedure;

public abstract class ExecutionStep implements HasPlanning {

	protected Database<?> database;
	protected PEStorageGroup sg;
	protected ExecutionType type;
	
	public ExecutionStep(Database<?> db, PEStorageGroup storageGroup, ExecutionType givenType) {
		database = db;
		sg = storageGroup;
		type = givenType;
	}
	
	public Database<?> getDatabase() {
		return database;
	}
	
	public PersistentDatabase getPersistentDatabase() {
		return database;
	}
	
	public UserDatabase getUserDatabase(SchemaContext sc) {
		if (database == null) return null;
		return database.getPersistent(sc);
	}
	
	public StorageGroup getStorageGroup(SchemaContext sc) {
		if (sg == null) return null;
		StorageGroup out = sg.getScheduledGroup(sc);
		return out;
	}
	
	public PEStorageGroup getPEStorageGroup() {
		return sg;
	}
	
	protected void addStep(SchemaContext sc, List<QueryStep> steps, QueryStepOperation qso) {
		steps.add(new QueryStep(getStorageGroup(sc), qso));
	}

	public void getSQL(SchemaContext sc, List<String> buf, EmitOptions opts) {
		
	}

	@Override
	public ExecutionType getExecutionType() {
		return type;
	}
	
	public ExecutionType getEffectiveExecutionType() {
		return getExecutionType();
	}
	
	public String getSQL(SchemaContext sc, EmitOptions opts) {
		ArrayList<String> buf = new ArrayList<String>();
		getSQL(sc,buf, opts);
		return Functional.join(buf,";");
	}
		
	public void displaySQL(SchemaContext sc, List<String> buf, String indent, EmitOptions opts) {
		buf.add(indent + "  sql:");
		buf.add(getSQL(sc,opts));
	}
	
	@Override
	public void display(SchemaContext sc, List<String> buf, String indent, EmitOptions opts) {
		String execType = null;
		if (getEffectiveExecutionType() != getExecutionType()) {
			execType = getEffectiveExecutionType().name() + " (" + getExecutionType().name() + ")";
		} else {
			execType = getEffectiveExecutionType().name();
		}
		buf.add(indent + execType + " on " 
				+ (getDatabase() == null ? "null" : getDatabase().getName().get()) 
				+ "/" + getStorageGroup(sc));
		if (opts == null) buf.add(indent + "  sql: '" + getSQL(sc,opts) + "'");
		else displaySQL(sc, buf, indent, opts);
	}
	
	@Override
	public void explain(SchemaContext sc, List<ResultRow> rows, ExplainOptions opts) {
		ResultRow rr = new ResultRow();
		addExplainColumns(sc,rr,opts);
		// add any nulls that are needed
		int width = ExecutionPlan.basicExplainColumns.length + (opts.isStatistics() ? StepExecutionStatistics.statsExplainColumns.length : 0); 
		int diff = width - 1 - rr.getRow().size();
		for(int i = 0; i < diff; i++)
			rr.addResultColumn(null,true);
		rr.addResultColumn(getSQL(sc,null), false);
		rows.add(rr);
	}
	
	protected void addStringResult(ResultRow rr, String v) {
		if (v == null)
			rr.addResultColumn(null, true);
		else
			rr.addResultColumn(v, false);
	}
	
	protected String explainStorageGroup(SchemaContext sc, PEStorageGroup storageGroup) {
		if (storageGroup == null) return null;
		return storageGroup.getPersistent(sc).getName();
	}
	
	protected String explainStepType() {
		return getExecutionType().toString();
	}
	
	protected String explainSourceGroup(SchemaContext sc) {
		return explainStorageGroup(sc,getPEStorageGroup());
	}

	protected void addExplainColumns(SchemaContext sc,ResultRow rr,ExplainOptions opts) {
		addStringResult(rr, explainStepType());
	}
		
	@Override
	public boolean useRowCount() {
		return false;
	}	
	
	@Override
	public Long getlastInsertId(ValueManager vm, SchemaContext sc) {
		return null;
	}


	@Override
	public Long getUpdateCount(SchemaContext sc) {
		return null;
	}

	@Override
	public CacheInvalidationRecord getCacheInvalidation(SchemaContext sc) {
		return null;
	}

	@Override
	public void prepareForCache() {
		
	}
	
	@Override
	public void visitInExecutionOrder(UnaryProcedure<HasPlanning> proc) {
		proc.execute(this);
	}
	
}
