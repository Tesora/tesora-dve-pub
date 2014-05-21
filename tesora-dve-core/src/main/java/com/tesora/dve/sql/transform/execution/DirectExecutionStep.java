// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.common.catalog.DistributionModel;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.distribution.IKeyValue;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.TransformException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.ExplainOptions;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaVariables;
import com.tesora.dve.sql.statement.dml.DMLStatement;

public abstract class DirectExecutionStep extends ExecutionStep {

	protected GenericSQLCommand sql;
	// null if none (not a key qso)
	protected DistributionKey distributionKey;
	// effective type for execution steps that are not directly planned
	protected ExecutionType effectiveType;
	// how the query is distributed
	protected DistributionVector distributionVector;

	protected boolean requiresReferenceTimestamp = false;
	
	protected final DMLExplainRecord explainHelp;
	
	protected StepExecutionStatistics stats = new StepExecutionStatistics();
	
	public DirectExecutionStep(Database<?> db, PEStorageGroup storageGroup, ExecutionType givenType, 
			DistributionVector vector, DistributionKey distKey, GenericSQLCommand command,
			DMLExplainRecord splain) throws PEException {
		this(db, storageGroup, givenType, vector, distKey, command, false, splain);
	}
	
	public DirectExecutionStep(Database<?> db, PEStorageGroup storageGroup, ExecutionType givenType, 
			DistributionVector vector, DistributionKey distKey, GenericSQLCommand command,
			boolean requiresReferenceTimestamp,
			DMLExplainRecord splain) throws PEException {
		super(db, storageGroup, givenType);
		sql = command;
		distributionKey = distKey;
		effectiveType = givenType;
		distributionVector = vector;
		this.requiresReferenceTimestamp = requiresReferenceTimestamp;
		this.explainHelp = splain;
	}

	// TODO:
	// should consider applying multitenant as a post-plan transform
	protected static void maybeApplyMultitenant(SchemaContext sc, DMLStatement dmls) throws PEException {
		sc.getPolicyContext().applyDegenerateMultitenantFilter(dmls);
	}
	
	@Override
	public ExecutionType getEffectiveExecutionType() {
		return effectiveType;
	}
	
	public GenericSQLCommand getRawSQL() {
		return sql;
	}
	
	public GenericSQLCommand getSQL(SchemaContext sc, String pretty) {
		return sql.resolve(sc, pretty);
	}
	
	@Override
	public String getSQL(SchemaContext sc, EmitOptions opts) {
		return sql.resolve(sc, (opts == null ? null : opts.getMultilinePretty())).getUnresolved();
	}

	public SQLCommand getCommand(SchemaContext sc) {
		GenericSQLCommand gen = getSQL(sc,(String)null);
		List<Object> actualParams = sql.getFinalParams(sc);
		return new SQLCommand(gen,actualParams);
	}

	public IKeyValue getKeyValue(SchemaContext sc) {
		if (distributionKey != null) return distributionKey.getDetachedKey(sc);
		if (distributionVector != null) return (distributionVector.usesColumns(sc) ? null : distributionVector.getKeyValue(sc));			
		return null;
	}
	
	public DistributionKey getDistributionKey() {
		return distributionKey;
	}

	public DistributionVector getDistributionVector() {
		return distributionVector;
	}
	
	public DistributionModel getDistributionModel(SchemaContext sc) {
		if (distributionVector == null) return null;
		return distributionVector.getModel().getSingleton();
	}
	
	@Override
	public void getSQL(SchemaContext sc, List<String> buf, EmitOptions opts) {
		buf.add(getSQL(sc,opts));
	}

	@Override
	public void displaySQL(SchemaContext sc, List<String> buf, String indent, EmitOptions opts) {
		ArrayList<String> sub = new ArrayList<String>();
		sql.display(sc, false, "  ", sub);
		for(String s : sub) {
			buf.add(indent + "    " + s);
		}
	}
	
	public static DirectExecutionStep buildUnplannedStep(SchemaContext sc, ExecutionType forType, 
			TableKey onTable, DistributionVector vect, Database<?> onDB, PEStorageGroup storageGroup, 
			DMLStatement stmt, ExecutionType effectiveType,
			DMLExplainRecord splain) throws PEException {
		DirectExecutionStep des = null;
		if (forType == ExecutionType.SELECT || forType == ExecutionType.UNION) {
			des = ProjectingExecutionStep.build(sc,onDB, storageGroup, vect, null, stmt, splain);
		} else if (forType == ExecutionType.UPDATE) {
			if (onTable.getAbstractTable().isView())
				throw new SchemaException(Pass.PLANNER,"No support for updating views");
			des = UpdateExecutionStep.build(sc,onDB, storageGroup, onTable.getAbstractTable().asTable(), null, stmt,
					stmt.getDerivedInfo().doSetTimestampVariable(), splain);
		} else if (forType == ExecutionType.DELETE) {
			des = DeleteExecutionStep.build(
					sc, 
					onDB, 
					storageGroup, 
					onTable, 
					null,
					stmt,
					false,
					splain);
		} else {
			throw new TransformException(Pass.PLANNER, "No unplanned step available for " + forType.name());
		}
		des.effectiveType = effectiveType;
		return des;
	}
	
	
	@Override
	public void display(SchemaContext sc, List<String> buf, String indent, EmitOptions opts) {
		super.display(sc, buf, indent, opts);
		if (distributionKey != null) {
			buf.add(indent + "  dist key: " + distributionKey.describe(sc));
		} else if (distributionVector != null) {
			buf.add(indent + "  dist on: " + distributionVector.describe(sc));
		}
		if (explainHelp != null)
			buf.add(indent + "  reason: " + explainHelp);
	}
	
	protected String explainSourceModel() {
		if (getDistributionVector() == null) return null;
		return getDistributionVector().getModel().getPersistentName();
	}
	
	protected String explainSourceDistKey(SchemaContext sc) {
		if (distributionKey == null) return null;
		return distributionKey.describe(sc);
	}
	
	@Override
	public void prepareForCache() {
		if (distributionKey == null) return;
		distributionKey.setFrozen();
	}

	public boolean requiresReferenceTimestamp() {
		return requiresReferenceTimestamp;
	}

	public void setRequiresReferenceTimestamp(boolean requiresReferenceTimestamp) {
		this.requiresReferenceTimestamp = requiresReferenceTimestamp;
	}
	
	public long getReferenceTimestamp(SchemaContext sc) {
		return (requiresReferenceTimestamp ? sc.getValueManager().getCurrentTimestamp(sc) : 0);
	}
	
	public StepExecutionStatistics getReportStatistics() {
		return stats;
	}
	
	public StepExecutionStatistics getStepStatistics(SchemaContext sc) {
		if (SchemaVariables.getStepwiseStatistics(sc.getConnection()))
			return stats;
		return null;
	}
	
	public void clearStatistics() {
		stats = new StepExecutionStatistics();
	}
		
	public DMLExplainRecord getExplainHint() {
		return explainHelp;
	}
	
	protected void addStepExplainColumns(SchemaContext sc, ResultRow rr, ExplainOptions opts) {
	}
	
	protected void addExplainColumns(SchemaContext sc,ResultRow rr,ExplainOptions opts) {
		super.addExplainColumns(sc, rr, opts);
		addStepExplainColumns(sc,rr, opts);
		if (opts.isStatistics()) {
			StepExecutionStatistics t = stats;
			if (t != null) 
				t.addColumns(rr);
		}
	}
}
