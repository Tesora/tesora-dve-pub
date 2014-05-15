// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;



import java.util.List;

import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepSetScopedVariableOperation;
import com.tesora.dve.queryplan.QueryStepSetScopedVariableOperation.ConstantValueAccessor;
import com.tesora.dve.queryplan.QueryStepSetScopedVariableOperation.ValueAccessor;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.variable.VariableAccessor;
import com.tesora.dve.variable.VariableScopeKind;
import com.tesora.dve.worker.WorkerGroup;

public class SetVariableExecutionStep extends ExecutionStep {

	protected VariableScopeKind scopeKind;
	protected String scopeName;
	protected String variableName;
	
	protected VariableValueSource valueSource;
	
	public SetVariableExecutionStep(VariableScopeKind vsk, String scopeName, String variableName, VariableValueSource valueSource, PEStorageGroup storageGroup) {
		super(null, storageGroup, ExecutionType.SESSION);
		this.scopeKind = vsk;
		this.scopeName = scopeName;
		this.variableName = variableName;
		this.valueSource = valueSource;
	}
	
	@Override
	public void getSQL(SchemaContext sc, List<String> buf, EmitOptions opts) {
		String sn = getScopeName();
		String prefix = "set " + sn + " " + variableName + " = ";
		if (valueSource.isConstant()) {
			buf.add(prefix + " " + valueSource.getConstantValue(sc));
		} else {
			buf.add(prefix + " <callback>");
		}
	}

	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps, ProjectionInfo projection, SchemaContext sc)
			throws PEException {
		boolean requireWorkers = getStorageGroup(sc) != null;
		addStep(sc,qsteps,new QueryStepSetScopedVariableOperation(scopeKind, scopeName, variableName, valueSource.buildAccessor(sc), requireWorkers));
	}

	public String getScopeName() {
		return (scopeName != null ? scopeName : scopeKind.name());
	}
	
	public String getVariableName() {
		return variableName;
	}
	
	public VariableValueSource getValueSource() {
		return valueSource;
	}
	
	public interface VariableValueSource {
		
		public ValueAccessor buildAccessor(SchemaContext sc);

		public boolean isConstant();
		
		public String getConstantValue(SchemaContext sc);
		
	}
	
	public static VariableValueSource makeSource(ConstantExpression ce) {
		return new LiteralVariableValueSource(ce);
	}
	
	public static VariableValueSource makeSource(VariableAccessor va) {
		return new VariableVariableValueSource(va);
	}
	
	public static VariableValueSource makeSource(String value) {
		return new ConstantVariableValueSource(value);
	}
	
	private static class LiteralVariableValueSource implements VariableValueSource {

		private final ConstantExpression constant;
		
		public LiteralVariableValueSource(ConstantExpression ce) {
			constant = ce;
		}
		
		@Override
		public ValueAccessor buildAccessor(SchemaContext sc) {
			return new ConstantValueAccessor(getConstantValue(sc));
		}

		@Override
		public boolean isConstant() {
			return true;
		}

		@Override
		public String getConstantValue(SchemaContext sc) {
			if (constant instanceof LiteralExpression) {
				LiteralExpression litex = (LiteralExpression) constant;
				if (litex.isNullLiteral())
					return null;
			}
			return constant.getValue(sc).toString();
		}
			
	}
	
	private static class VariableVariableValueSource implements VariableValueSource {
		
		private final VariableAccessor accessor;
		
		public VariableVariableValueSource(VariableAccessor acc) {
			accessor = acc;
		}

		@Override
		public ValueAccessor buildAccessor(SchemaContext sc) {
			return new ValueAccessor() {

				@Override
				public String getValue(SSConnection ssCon, WorkerGroup wg) throws PEException {
					return accessor.getValue(ssCon);
				}};
		}

		@Override
		public boolean isConstant() {
			return false;
		}

		@Override
		public String getConstantValue(SchemaContext sc) {
			return null;
		}
	}
	
	private static class ConstantVariableValueSource implements VariableValueSource {
		
		private final String value;
		
		public ConstantVariableValueSource(String v) {
			value = v;
		}

		@Override
		public ValueAccessor buildAccessor(SchemaContext sc) {
			return new ConstantValueAccessor(value);
		}

		@Override
		public boolean isConstant() {
			return true;
		}

		@Override
		public String getConstantValue(SchemaContext sc) {
			return value;
		}
	}
}
