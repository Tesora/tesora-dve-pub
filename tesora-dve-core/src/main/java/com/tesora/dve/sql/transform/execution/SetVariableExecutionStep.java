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

import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepSetScopedVariableOperation;
import com.tesora.dve.queryplan.QueryStepSetScopedVariableOperation.ConstantValueAccessor;
import com.tesora.dve.queryplan.QueryStepSetScopedVariableOperation.ValueAccessor;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.variables.AbstractVariableAccessor;
import com.tesora.dve.worker.WorkerGroup;

public class SetVariableExecutionStep extends ExecutionStep {

	protected VariableScope scope;
	protected String variableName;
	
	protected VariableValueSource valueSource;
	
	public SetVariableExecutionStep(VariableScope vs, String variableName, VariableValueSource valueSource, PEStorageGroup storageGroup) {
		super(null, storageGroup, ExecutionType.SESSION);
		this.scope = vs;
		this.variableName = variableName;
		this.valueSource = valueSource;
	}
	
	@Override
	public void getSQL(SchemaContext sc, ConnectionValuesMap cvm, ExecutionPlan containing, List<String> buf, EmitOptions opts) {
		String sn = getScopeName();
		String prefix = "set " + sn + " " + variableName + " = ";
		if (valueSource.isConstant()) {
			buf.add(prefix + " " + valueSource.getConstantValue(cvm.getValues(containing)));
		} else {
			buf.add(prefix + " <callback>");
		}
	}

	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStepOperation> qsteps, ProjectionInfo projection, SchemaContext sc,
			ConnectionValuesMap cvm, ExecutionPlan containing) throws PEException {
		ConnectionValues cv = cvm.getValues(containing);
		StorageGroup sg = getStorageGroup(sc,cv);
		qsteps.add(new QueryStepSetScopedVariableOperation(sg,scope, variableName, valueSource.buildAccessor(cv), sg != null));
	}

	public String getScopeName() {
		if (scope.getScopeName() == null) return scope.getKind().name();
		return scope.getScopeName();
	}
	
	public String getVariableName() {
		return variableName;
	}
	
	public VariableValueSource getValueSource() {
		return valueSource;
	}
	
	public interface VariableValueSource {
		
		public ValueAccessor buildAccessor(ConnectionValues cv);

		public boolean isConstant();
		
		public String getConstantValue(ConnectionValues cv);
		
	}
	
	public static VariableValueSource makeSource(ConstantExpression ce) {
		return new LiteralVariableValueSource(ce);
	}
	
	public static VariableValueSource makeSource(AbstractVariableAccessor va) {
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
		public ValueAccessor buildAccessor(ConnectionValues cv) {
			return new ConstantValueAccessor(getConstantValue(cv));
		}

		@Override
		public boolean isConstant() {
			return true;
		}

		@Override
		public String getConstantValue(ConnectionValues cv) {
			if (constant instanceof LiteralExpression) {
				LiteralExpression litex = (LiteralExpression) constant;
				if (litex.isNullLiteral())
					return null;
			}
			return constant.getValue(cv).toString();
		}
			
	}
	
	private static class VariableVariableValueSource implements VariableValueSource {
		
		private final AbstractVariableAccessor accessor;
		
		public VariableVariableValueSource(AbstractVariableAccessor acc) {
			accessor = acc;
		}

		@Override
		public ValueAccessor buildAccessor(ConnectionValues cv) {
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
		public String getConstantValue(ConnectionValues cv) {
			return null;
		}
	}
	
	private static class ConstantVariableValueSource implements VariableValueSource {
		
		private final String value;
		
		public ConstantVariableValueSource(String v) {
			value = v;
		}

		@Override
		public ValueAccessor buildAccessor(ConnectionValues cv) {
			return new ConstantValueAccessor(value);
		}

		@Override
		public boolean isConstant() {
			return true;
		}

		@Override
		public String getConstantValue(ConnectionValues cv) {
			return value;
		}
	}
}
