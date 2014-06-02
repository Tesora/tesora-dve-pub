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
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepMultiTupleRedistOperation;
import com.tesora.dve.queryplan.TableHints;
import com.tesora.dve.queryplan.TempTableDeclHints;
import com.tesora.dve.queryplan.TempTableGenerator;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.DistributionKeyTemplate;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.ExplainOptions;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.DistributionVector.Model;
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
			DMLExplainRecord splain) throws PEException {
		maybeApplyMultitenant(sc,sql);
		return new RedistributionExecutionStep(sc, db, srcGroup, sourceDV, sql, targetGroup, redistToTable,
				redistToScopedTable, dv, missingAutoInc, offsetToExistingAutoInc, onDupKey, rc, dk, mustEnforceScalarValue, insertIgnore, splain);
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
			DMLExplainRecord splain) throws PEException {
		super(db, srcGroup, sourceDV, dk, sql.getGenericSQL(sc, false, true), splain);
		effectiveType = sql.getExecutionType();
		targetTable = redistToTable;
		distKey = dv;
		this.targetGroup = targetGroup;

		if ((redistToTable != null) && redistToTable.isTempTable()) {
			this.declarationHints = ((TempTable) redistToTable).getHints(sc);
		}

		this.missingAutoInc = missingAutoInc;
		this.offsetOfExistingAutoinc = offsetOfExistingAutoInc;
		useRowCount = rc;
		targetScope = redistToScopedTable;
		requiresReferenceTimestamp = sql.getDerivedInfo().doSetTimestampVariable();
		if (onDupKey != null && !onDupKey.isEmpty()) {
			StringBuilder buf = new StringBuilder();
            Singletons.require(HostService.class).getDBNative().getEmitter().emitInsertSuffix(sc, onDupKey, buf);
			redistOnDupKey = new SQLCommand(buf.toString());
		}
		this.enforceScalarValue = mustEnforceScalarValue;
		this.insertIgnore = insertIgnore;
	}

	private RedistributionExecutionStep(SchemaContext sc, Database<?> db, PEStorageGroup storageGroup, String sql, DistributionVector sourceVect,
			PETable redistToTable,
			PEStorageGroup targetGroup,
			DistributionKeyTemplate distKeyTemplate,
			DMLExplainRecord splain) throws PEException {
		super(db, storageGroup, sourceVect, null, new GenericSQLCommand(sql), splain); 
		targetTable = redistToTable;
		this.targetGroup = targetGroup;
		distKey = distKeyTemplate;

		if ((redistToTable != null) && redistToTable.isTempTable()) {
			this.declarationHints = ((TempTable) redistToTable).getHints(sc);
		}
	}
	

	
	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps,
			ProjectionInfo projection, SchemaContext sc) throws PEException {
		QueryStepMultiTupleRedistOperation qsrdo = null;
		
		if (targetTable.mustBeCreated()) {
			qsrdo = new QueryStepMultiTupleRedistOperation(getPersistentDatabase(), getCommand(sc),	getDistributionModel(sc));
			if (targetTable.isExplicitlyDeclared()) {
				// need to create a new context to avoid leaking
				SchemaContext mutableContext = SchemaContext.makeMutableIndependentContext(sc);
				mutableContext.setValues(sc._getValues());
				mutableContext.beginSaveContext();
				try {
					UserTable ut = targetTable.getPersistent(mutableContext);
					qsrdo.toUserTable(targetGroup.getPersistent(sc), ut, declarationHints, true);
				} finally {
					mutableContext.endSaveContext();
				}
			} else {
				qsrdo.toTempTable(targetGroup.getPersistent(sc), getPersistentDatabase(), targetTable.getName(sc).get(), true);
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
					
				qsrdo = new QueryStepMultiTupleRedistOperation(getPersistentDatabase(), getCommand(sc), getDistributionModel(sc))
						.toUserTable(targetTable.getPersistentStorage(sc).getPersistent(sc), targetTable.getPersistentTable(sc), hints, true);
			} finally {
				sc.endSaveContext();
			}
			if (redistOnDupKey != null)
				qsrdo.onDupKey(redistOnDupKey);
		}
		DistributionKey dk = getDistributionKey();
		if (dk != null)
			qsrdo.setSpecifiedDistKeyValue(dk.getDetachedKey(sc));
		qsrdo.setEnforceScalarValue(enforceScalarValue);
		qsrdo.setInsertIgnore(insertIgnore);
		TempTableGenerator generator = targetTable.getTableGenerator(sc);
		if (generator != null)
			qsrdo.withTableGenerator(generator);
		qsrdo.setStatistics(getStepStatistics(sc));
		addStep(sc,qsteps,qsrdo);

	}

	public PEStorageGroup getTargetGroup(SchemaContext sc) {
		if (targetGroup == null) return null;
		return targetGroup.getPEStorageGroup(sc);
	}
	
	public PETable getTargetTable() {
		return targetTable;
	}

	public String getRedistTable(SchemaContext sc) {
		if (targetTable == null) return null;
		return targetTable.getName(sc).get();
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
	public void display(SchemaContext sc, List<String> buf, String indent, EmitOptions opts) {
		super.display(sc, buf, indent, opts);
		buf.add(indent + "  redist to (" + targetTable.getName(sc).get() + ") using model " + describeTargetModel(sc) + " on " + targetGroup.getPEStorageGroup(sc).getPersistent(sc));
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

	private String describeTargetModel(SchemaContext sc) {
		if (distKey == null) return targetTable.getDistributionVector(sc).describe(sc);
		return distKey.describe(sc);
	}
	
	@Override
	protected void addStepExplainColumns(SchemaContext sc, ResultRow rr, ExplainOptions opts) {
		super.addStepExplainColumns(sc, rr, opts);
		addStringResult(rr,explainTargetGroup(sc));
		addStringResult(rr,explainTargetTable(sc));
		addStringResult(rr,explainTargetDist(sc));
        addStringResult(rr,explainTargetHints(sc));
        addStringResult(rr,explainExplainHint(sc));
	}

	protected String explainTargetGroup(SchemaContext sc) {
		if (targetGroup == null) return null;
		return explainStorageGroup(sc,targetGroup);
	}
	
	protected String explainTargetTable(SchemaContext sc) {
		if (targetTable == null) return null;
		return targetTable.getName(sc).get();
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
		if (indices == null) {
            return null;
        }

		StringBuilder buf = new StringBuilder();
		Iterator<List<String>> entries = indices.iterator();
		while (entries.hasNext()) {
			List<String> entry = entries.next();
			buf.append(entry.toString());
			if (entries.hasNext())
				buf.append(",");
		}

		return buf.toString();

    }

	
	@Override
	public void prepareForCache() {
		if (targetTable != null)
			targetTable.setFrozen();
		super.prepareForCache();
	}
}
