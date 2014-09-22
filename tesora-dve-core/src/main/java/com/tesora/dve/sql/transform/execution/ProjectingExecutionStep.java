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



import java.util.List;

import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.distribution.StaticDistributionModel;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepDMLOperation;
import com.tesora.dve.queryplan.QueryStepResultsOperation;
import com.tesora.dve.queryplan.QueryStepSelectAllOperation;
import com.tesora.dve.queryplan.QueryStepSelectByKeyOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.ExplainOptions;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;

public final class ProjectingExecutionStep extends AbstractProjectingExecutionStep {


	protected LiteralExpression inMemLimit = null;
	
	private ProjectingExecutionStep(SchemaContext sc, Database<?> db, PEStorageGroup storageGroup, DistributionVector vect, DistributionKey distKey,
			DMLStatement command, DMLExplainRecord splain) throws PEException {
		super(sc,db,storageGroup,vect,distKey,command,splain);
	}

	public static ProjectingExecutionStep build(SchemaContext sc, Database<?> db, PEStorageGroup storageGroup, DistributionVector vect, DistributionKey distKey,
			DMLStatement command, DMLExplainRecord splain) throws PEException {
		maybeApplyMultitenant(sc, command);
		return new ProjectingExecutionStep(sc, db, storageGroup, vect, distKey, command, splain);
	}

	private ProjectingExecutionStep(final SchemaContext sc, Database<?> db, PEStorageGroup storageGroup, String sql) throws PEException {
		super(db, storageGroup, null, null, new GenericSQLCommand(sc, sql), DMLExplainReason.ADHOC.makeRecord());
	}
	
	public static ProjectingExecutionStep build(final SchemaContext sc, Database<?> db, PEStorageGroup storageGroup, String sql) throws PEException {
		return new ProjectingExecutionStep(sc, db, storageGroup, sql);
	}
		
	private ProjectingExecutionStep(Database<?> db, PEStorageGroup storageGroup, GenericSQLCommand gsql) throws PEException {
		super(db, storageGroup, null, null, gsql, DMLExplainReason.ADHOC.makeRecord());
	}
	
	public static ProjectingExecutionStep build(Database<?> db, PEStorageGroup storageGroup, GenericSQLCommand gsql) throws PEException {
		return new ProjectingExecutionStep(db, storageGroup, gsql);
	}
	
	@Override
	public void display(SchemaContext sc, List<String> buf, String indent, EmitOptions opts) {
		super.display(sc, buf, indent, opts);
		long lim = getInMemLimit(sc);
		if (lim > -1)
			buf.add(indent + "  limit " + lim);
	}	
	
	
	@Override
	public boolean useRowCount() {
		if (useRowCount != null) return useRowCount.booleanValue();
		return super.useRowCount();
	}

	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps, ProjectionInfo projection, SchemaContext sc)
			throws PEException {
		QueryStepDMLOperation qso = null;
		long inMem = getInMemLimit(sc);
		IKeyValue ikv = getKeyValue(sc);
		SQLCommand sqlCommand = getCommand(sc).withProjection(projection).withReferenceTime(getReferenceTimestamp(sc));
		QueryStepResultsOperation qsro = null;
		if (ikv != null) {
			qsro = new QueryStepSelectByKeyOperation(getPersistentDatabase(), ikv, sqlCommand);
		} else {
			qsro = new QueryStepSelectAllOperation(getPersistentDatabase(), StaticDistributionModel.SINGLETON, sqlCommand);
		}
		if (inMem > -1)
			qsro.setResultsLimit(inMem);
		qso = qsro;
		qso.setStatistics(getStepStatistics(sc));
		addStep(sc,qsteps,qso);
	}
	
	@Override
	public void prepareForCache() {
		if (inMemLimit != null)
			inMemLimit.setParent(null);
		super.prepareForCache();
	}
	
	public void setInMemoryLimit(LiteralExpression litex) {
		inMemLimit = litex;
	}
	
	public boolean usesInMemoryLimit() {
		return inMemLimit != null;
	}
	
	private long getInMemLimit(SchemaContext sc) {
		if (inMemLimit == null) return -1;
		Object value = inMemLimit.getValue(sc);
		long lim = -1;
		if (value instanceof Number) {
			lim = ((Number)value).longValue();
		}
		return lim;
	}
	
	@Override
	protected void addStepExplainColumns(SchemaContext sc, ResultRow rr, ExplainOptions opts) {
		super.addStepExplainColumns(sc, rr, opts);
		addStringResult(rr,null); // target group
		addStringResult(rr,null); // target table
		addStringResult(rr,""); // target dist
		addStringResult(rr,null); // target hints
        addStringResult(rr,explainExplainHint(sc));
	}

}
