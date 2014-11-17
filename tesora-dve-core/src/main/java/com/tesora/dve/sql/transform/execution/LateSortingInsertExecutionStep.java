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

import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepMultiInsertByKeyOperation;
import com.tesora.dve.queryplan.QueryStepMultiInsertByKeyOperation.StatementSource;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.JustInTimeInsert;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.ValueManager;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryFunction;

public class LateSortingInsertExecutionStep extends DirectExecutionStep {

	// if true we ignore whatever update count we get after sorting
	protected boolean ignoreUpdateCount;
	
	private LateSortingInsertExecutionStep(SchemaContext pc, Database<?> db, PEStorageGroup tsg, PETable tab, boolean ignoreUpdateCount) throws PEException {
		super(db, tsg, ExecutionType.INSERT, tab.getDistributionVector(pc), null, null, null);
		this.ignoreUpdateCount = ignoreUpdateCount;
	}

	public static LateSortingInsertExecutionStep build(SchemaContext pc, Database<?> db, PEStorageGroup tsg, PETable tab,
			boolean ignoreUpdateCount) throws PEException {
		return new LateSortingInsertExecutionStep(pc, db, tsg, tab, ignoreUpdateCount);
	}
	
	@Override
	public Long getlastInsertId(ValueManager vm, SchemaContext sc, ConnectionValues cv) {
		return cv.getLastInsertId();
	}
	
	@Override
	public Long getUpdateCount(SchemaContext sc, ConnectionValues cv) {
		if (ignoreUpdateCount)
			return null;
		List<JustInTimeInsert> late = cv.getLateSortedInserts();
		long uc = 0;
		for(JustInTimeInsert jti : late)
			uc += jti.getUpdateCount();
		return uc;
	}

	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStepOperation> qsteps, ProjectionInfo projection, 
			SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing)
			throws PEException {
		ConnectionValues cv = cvm.getValues(containing);
		StatementSource src = null;
		if (containing.isRoot()) {
			List<JustInTimeInsert> late = cv.getLateSortedInserts(); 

			List<Pair<IKeyValue,SQLCommand>> computed = new ArrayList<Pair<IKeyValue,SQLCommand>>();
			for(JustInTimeInsert jti : late) {
				SQLCommand sqlc = jti.getSQL();
				sqlc.withReferenceTime(cv.getCurrentTimestamp());
				computed.add(new Pair<IKeyValue,SQLCommand>(jti.getKey().getDetachedKey(sc, cv),sqlc));
			}
		
			src = new PlannerTimeInserts(computed);
		} else {
			src = new RunTimeInserts();
		}
		QueryStepMultiInsertByKeyOperation qso = new QueryStepMultiInsertByKeyOperation(getStorageGroup(sc,cv),getPersistentDatabase(),src);
		qso.setStatistics(getStepStatistics(sc));
		qsteps.add(qso);
	}

	
	
	@Override
	public void display(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing, List<String> buf, String indent, EmitOptions opts) {
		String execType = getEffectiveExecutionType().name();
		StringBuilder prefix = new StringBuilder();
		ConnectionValues cv = cvm.getValues(containing);
		prefix.append(indent).append(execType).append(" on ").append((getDatabase() == null ? "null" : getDatabase().getName().get()))
			.append("/").append(getStorageGroup(sc,cv));
		if (!containing.isRoot()) {
			buf.add("inserts not available at planning time");
			return;
		}
		for(JustInTimeInsert jti : cv.getLateSortedInserts()) {
			buf.add(prefix.toString());
			if (opts == null)
				buf.add(indent + "  sql: '" + jti.getSQL() + "'");
			else {
				buf.add(indent + "  sql:");
				buf.add(jti.getSQL().getRawSQL());
			}
			buf.add(indent + " dist key: " + jti.getKey().describe(cv));
		}
	}
	
	private static class RunTimeInserts implements StatementSource {

		@Override
		public List<Pair<IKeyValue, SQLCommand>> generate(final SchemaContext sc,
				final ConnectionValues cv) {
			ParserOptions was = sc.getOptions();
			try {
				sc.setOptions(was.setNestedPlan());
				return Functional.apply(cv.getLateSortedInserts(), new UnaryFunction<Pair<IKeyValue,SQLCommand>,JustInTimeInsert>() {

					@Override
					public Pair<IKeyValue, SQLCommand> evaluate(
							JustInTimeInsert object) {
						SQLCommand sqlc = object.getSQL();
						sqlc.withReferenceTime(cv.getCurrentTimestamp());
						return new Pair<IKeyValue,SQLCommand>(object.getKey().getDetachedKey(sc, cv),sqlc);
					}

				});
			} finally {
				sc.setOptions(was);
			}
		}
		
	}
	
	private static class PlannerTimeInserts implements StatementSource {

		private final List<Pair<IKeyValue,SQLCommand>> stmts;
		
		public PlannerTimeInserts(List<Pair<IKeyValue,SQLCommand>> stmts) {
			this.stmts = stmts;
		}
	
		
		@Override
		public List<Pair<IKeyValue, SQLCommand>> generate(SchemaContext sc,
				ConnectionValues cv) {
			return stmts;
		}
		
		
	}
}
