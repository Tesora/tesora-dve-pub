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

import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.catalog.HasAutoIncrementTracker;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepMultiTupleRedistOperation;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.TableHints;
import com.tesora.dve.queryplan.TempTableDeclHints;
import com.tesora.dve.queryplan.TempTableGenerator;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.DistributionKeyTemplate;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.ExplainOptions;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.mt.TableScope;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.Pair;

public final class RedistributionExecutionStep extends
		AbstractProjectingExecutionStep {

	// for redists
	protected PETable targetTable = null;
	// if redist, the dv
	protected DistributionKeyTemplate distKey = null;
	// if redist, the target persistent group
	protected PEStorageGroup targetGroup = null;
	// hints for the temp table declaration
	protected TempTableDeclHints declarationHints = null;
	
	protected PEColumn missingAutoInc;
	protected Integer offsetOfExistingAutoinc;
	// any on dup key clause from the original
	protected SQLCommand redistOnDupKey;
	// if redisting to a tenant table, the tenant scope
	protected TableScope targetScope;

	boolean enforceScalarValue = false;
	boolean insertIgnore = false;

	boolean userlandTemporaryTable = false;
	
	// any table generator
	protected TempTableGenerator generator;

	public static RedistributionExecutionStep build(SchemaContext sc, Database<?> db, PEStorageGroup srcGroup, DistributionVector sourceDV,
			ProjectingStatement sql, PEStorageGroup targetGroup, PETable redistToTable,
			TableScope redistToScopedTable,
			DistributionKeyTemplate dv,
			PEColumn missingAutoInc,
			Integer offsetToExistingAutoInc,
			List<ExpressionNode> onDupKey,
			Boolean rc, DistributionKey dk,
			boolean mustEnforceScalarValue,
			boolean insertIgnore,
			TempTableGenerator tempTableGenerator,
			DMLExplainRecord splain) throws PEException {
		maybeApplyMultitenant(sc,sql);
		return new RedistributionExecutionStep(sc, db, srcGroup, sourceDV, sql, targetGroup, redistToTable,
				redistToScopedTable, dv, missingAutoInc, offsetToExistingAutoInc, 
				onDupKey, rc, dk, mustEnforceScalarValue, insertIgnore, tempTableGenerator, splain);
	}
	
	public static RedistributionExecutionStep build(SchemaContext sc, Database<?> db, PEStorageGroup storageGroup, String sql, DistributionVector sourceVect,
			PETable redistToTable,
			PEStorageGroup targetGroup,
			DistributionKeyTemplate distKeyTemplate,
			DMLExplainRecord splain) throws PEException {
		if (redistToTable != null)
			redistToTable.setFrozen();
		return new RedistributionExecutionStep(sc, db, storageGroup, sql, sourceVect, redistToTable, targetGroup, distKeyTemplate, splain);
	}
		
	private RedistributionExecutionStep(SchemaContext sc, Database<?> db, PEStorageGroup srcGroup, 
			DistributionVector sourceDV, ProjectingStatement sql, PEStorageGroup targetGroup, PETable redistToTable,
			TableScope redistToScopedTable,
			DistributionKeyTemplate dv,
			PEColumn missingAutoInc,
			Integer offsetOfExistingAutoInc,
			List<ExpressionNode> onDupKey,
			Boolean rc,
			DistributionKey dk, boolean mustEnforceScalarValue,
			boolean insertIgnore,
			TempTableGenerator tempTableGenerator,
			DMLExplainRecord splain) throws PEException {
		super(db, srcGroup, sourceDV, dk, sql.getGenericSQL(sc, false, true), splain);
		effectiveType = sql.getExecutionType();
		targetTable = redistToTable;
		distKey = dv;
		this.targetGroup = targetGroup;

		if ((redistToTable != null) && redistToTable.isTempTable()) {
			this.declarationHints = ((TempTable) redistToTable).finalizeHints(sc);
		}

		if (targetTable.isUserlandTemporaryTable() || 
				sql.getDerivedInfo().hasUserlandTemporaryTables())
			userlandTemporaryTable = true;
		
		this.missingAutoInc = missingAutoInc;
		this.offsetOfExistingAutoinc = offsetOfExistingAutoInc;
		useRowCount = rc;
		targetScope = redistToScopedTable;
		requiresReferenceTimestamp = sql.getDerivedInfo().doSetTimestampVariable();
		if (onDupKey != null && !onDupKey.isEmpty()) {
			StringBuilder buf = new StringBuilder();
            Singletons.require(HostService.class).getDBNative().getEmitter().emitInsertSuffix(sc, sc.getValues(), onDupKey, buf);
            // should think about moving this into schedule
			redistOnDupKey = new SQLCommand(sc, buf.toString());
		}
		this.enforceScalarValue = mustEnforceScalarValue;
		this.insertIgnore = insertIgnore;
		this.generator = tempTableGenerator;
	}

	private RedistributionExecutionStep(SchemaContext sc, Database<?> db, PEStorageGroup storageGroup, String sql, DistributionVector sourceVect,
			PETable redistToTable,
			PEStorageGroup targetGroup,
			DistributionKeyTemplate distKeyTemplate,
			DMLExplainRecord splain) throws PEException {
		super(db, storageGroup, sourceVect, null, new GenericSQLCommand(sc, sql), splain);
		targetTable = redistToTable;
		this.targetGroup = targetGroup;
		distKey = distKeyTemplate;

		if ((redistToTable != null) && redistToTable.isTempTable()) {
			this.declarationHints = ((TempTable) redistToTable).finalizeHints(sc);
		}
	}
	

	
	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStepOperation> qsteps,
			ProjectionInfo projection, SchemaContext sc,
			ConnectionValuesMap cvm, ExecutionPlan containing) throws PEException {
		QueryStepMultiTupleRedistOperation qsrdo = null;
		
		ConnectionValues cv = cvm.getValues(containing);
		StorageGroup sg = getStorageGroup(sc,cv);
		
		if (targetTable.mustBeCreated()) {
			qsrdo = new QueryStepMultiTupleRedistOperation(sg, getPersistentDatabase(), getCommand(sc,cv),	getDistributionModel(sc));
			if (targetTable.isExplicitlyDeclared()) {
				// need to create a new context to avoid leaking
				SchemaContext mutableContext = SchemaContext.makeMutableIndependentContext(sc);
				mutableContext.setValues(sc.getValues());
				mutableContext.beginSaveContext();
				try {
					UserTable ut = targetTable.getPersistent(mutableContext);
					qsrdo.toUserTable(targetGroup.getPersistent(sc,cv), ut, declarationHints, true);
				} finally {
					mutableContext.endSaveContext();
				}
			} else {
				final Database<?> targetDb = targetTable.getDatabase(sc);
				qsrdo.toTempTable(targetGroup.getPersistent(sc,cv), (targetDb != null) ? targetDb : getPersistentDatabase(), targetTable.getName(sc,cv).get(), true);
			}
			if (distKey.usesColumns(sc)) {
				if (Model.RANGE.equals(distKey.getModel(sc)))
					qsrdo.distributeOn(distKey.getColumnNames(), distKey.getTable().asTable().getPersistentTable(sc));
				else
					qsrdo.distributeOn(distKey.getColumnNames());
			}
			qsrdo.withTempHints(declarationHints);
		} else {
			TableHints hints = new TableHints();
			sc.beginSaveContext();
			try {
				HasAutoIncrementTracker hait = null;
				if (missingAutoInc != null || offsetOfExistingAutoinc != null) {
					if (targetScope != null)
						hait = targetScope.persistTree(sc);
					else
						hait = targetTable.persistTree(sc);
				}
				if (missingAutoInc != null) {
					hints.withMissingAutoIncs(new Pair<UserColumn,HasAutoIncrementTracker>(missingAutoInc.persistTree(sc), hait));
				}
				if (offsetOfExistingAutoinc != null) {
					hints.withExistingAutoIncs(new Pair<Integer, HasAutoIncrementTracker>(offsetOfExistingAutoinc, hait));
				}
					
				qsrdo = new QueryStepMultiTupleRedistOperation(sg, getPersistentDatabase(), getCommand(sc,cv), getDistributionModel(sc))
						.toUserTable(targetTable.getPersistentStorage(sc).getPersistent(sc), targetTable.getPersistentTable(sc), hints, true);
			} finally {
				sc.endSaveContext();
			}
			if (redistOnDupKey != null)
				qsrdo.onDupKey(redistOnDupKey);
		}
		DistributionKey dk = getDistributionKey();
		if (dk != null)
			qsrdo.setSpecifiedDistKeyValue(dk.getDetachedKey(sc,cv));
		qsrdo.setEnforceScalarValue(enforceScalarValue);
		qsrdo.setInsertIgnore(insertIgnore);
		if (generator != null)
			qsrdo.withTableGenerator(generator);
		if (userlandTemporaryTable)
			qsrdo.withUserlandTemporaryTables();
		qsrdo.setStatistics(getStepStatistics(sc));
		qsteps.add(qsrdo);

	}

	public PEStorageGroup getTargetGroup(SchemaContext sc, ConnectionValues cv) {
		if (targetGroup == null) return null;
		return targetGroup.getPEStorageGroup(sc,cv);
	}
	
	public PETable getTargetTable() {
		return targetTable;
	}

	public String getRedistTable(SchemaContext sc, ConnectionValues cv) {
		if (targetTable == null) return null;
		return targetTable.getName(sc,cv).get();
	}
	
	public DistributionKeyTemplate getDistKey() {
		return distKey;
	}
	
	public String getTargetDistributionModel(SchemaContext sc) {
		if (targetTable == null) return null;
		return distKey.getModel(sc).getPersistentName();
	}

	@Override
	protected String explainStepType() {
		return "REDISTRIBUTE";
	}

	@Override
	public void display(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing, List<String> buf, String indent, EmitOptions opts) {
		super.display(sc, cvm,containing,buf, indent, opts);
		ConnectionValues cv = cvm.getValues(containing);
		buf.add(indent + "  redist to (" + targetTable.getName(sc,cv).get() + ") using model " + describeTargetModel(sc,cv) + " on " + targetGroup.getPEStorageGroup(sc,cv).getPersistent(sc,cv));
		if (declarationHints != null) {
			List<List<String>> indices = declarationHints.getIndexes();
			if (!indices.isEmpty()) {
				StringBuilder ibuf = new StringBuilder();
				ibuf.append("indexes ");
				boolean first = true;
				for (List<String> s : indices) {
					if (first) first = false;
					else ibuf.append(",");
					ibuf.append("(");
					ibuf.append(StringUtils.join(s, ','));
					ibuf.append(")");
				}
				buf.add(indent + "  " + ibuf.toString());
			}
		}
	}	

	private String describeTargetModel(SchemaContext sc, ConnectionValues cv) {
		if (distKey == null) return targetTable.getDistributionVector(sc).describe(sc, cv);
		return distKey.describe(sc);
	}
	
	@Override
	protected void addStepExplainColumns(SchemaContext sc, ConnectionValues cv, ResultRow rr, ExplainOptions opts) {
		super.addStepExplainColumns(sc, cv, rr, opts);
		addStringResult(rr,explainTargetGroup(sc,cv));
		addStringResult(rr,explainTargetTable(sc, cv));
		addStringResult(rr,explainTargetDist(sc));
        addStringResult(rr,explainTargetHints(sc));
        addStringResult(rr,explainExplainHint(sc));
	}

	protected String explainTargetGroup(SchemaContext sc,ConnectionValues cv) {
		if (targetGroup == null) return null;
		return explainStorageGroup(sc,targetGroup,cv);
	}
	
	protected String explainTargetTable(SchemaContext sc, ConnectionValues cv) {
		if (targetTable == null) return null;
		return targetTable.getName(sc,cv).get();
	}
		
	protected String explainTargetDist(SchemaContext sc) {
		StringBuilder buf = new StringBuilder();
		String model = (targetGroup != null ? getTargetDistributionModel(sc) : null);
		String vect = null;
		if (targetGroup != null && distKey != null)
			vect = Functional.join(distKey.getColumnNames(), ",");
		if (model != null)
			buf.append(model).append(" distribute");
		if (vect != null && !"".equals(vect.trim()))
			buf.append(" on ").append(vect);
		return buf.toString();
	}

    protected String explainTargetHints(SchemaContext sc) {
		if (declarationHints == null) {
			return null;
		}

		List<List<String>> indices = declarationHints.getIndexes();
		List<List<String>> uniques = declarationHints.getUniqueKeys();
		if (indices == null && uniques == null) {
            return null;
        }

		StringBuilder buf = new StringBuilder();
		if (uniques != null && !uniques.isEmpty())
			explainIndexHints("u",uniques,buf);
		if (indices != null && !indices.isEmpty())
			explainIndexHints("i",indices,buf);

		return buf.toString();

    }

    private void explainIndexHints(String prefix, List<List<String>> cols, StringBuilder buf) {
    	buf.append(prefix).append(":");
		for(Iterator<List<String>> iter = cols.iterator(); iter.hasNext();) {
			buf.append(iter.next());
			if (iter.hasNext())
				buf.append(",");
		}
    }
    
	
	@Override
	public void prepareForCache() {
		if (targetTable != null)
			targetTable.setFrozen();
		super.prepareForCache();
	}
}
