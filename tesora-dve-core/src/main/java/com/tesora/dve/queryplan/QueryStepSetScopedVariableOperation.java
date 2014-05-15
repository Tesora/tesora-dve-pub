// OS_STATUS: public
package com.tesora.dve.queryplan;

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.variable.VariableScopeKind;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepSetScopedVariableOperation extends
		QueryStepOperation {

	protected VariableScopeKind scopeKind;
	protected String scopeName;
	protected String variableName;
	protected ValueAccessor accessor;
	protected boolean requireWorkers = false;
	
	public QueryStepSetScopedVariableOperation(VariableScopeKind vsk, String scopeName, String variableName, ValueAccessor accessor, boolean requireWorkers) {
		super();
		this.scopeKind = vsk;
		this.scopeName = scopeName;
		this.variableName = variableName;
		this.accessor = accessor;
		this.requireWorkers = requireWorkers;
	}

	public QueryStepSetScopedVariableOperation(VariableScopeKind vsk, String scopeName, String variableName, String value) {
		this(vsk, scopeName, variableName, new ConstantValueAccessor(value), false);
	}
	
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer)
			throws Throwable {
		String nv = accessor.getValue(ssCon, wg);
		if (VariableScopeKind.DVE == scopeKind) {
            Singletons.require(HostService.class).setGlobalVariable(ssCon.getCatalogDAO(), variableName, nv);
		} else if (VariableScopeKind.SESSION == scopeKind) {
			ssCon.setSessionVariable(variableName, nv);
		} else if (VariableScopeKind.USER == scopeKind) {
			ssCon.setUserVariable(variableName, nv);
		} else {
            Singletons.require(HostService.class).setScopedVariable(scopeName, variableName, nv);
		}
	}

	@Override
	public boolean requiresWorkers() {
		return requireWorkers;
	}
	
	@Override
	public boolean requiresTransaction() {
		return false;
	}
	public interface ValueAccessor {
		
		String getValue(SSConnection ssCon, WorkerGroup wg) throws PEException;
		
	}
	
	public static class ConstantValueAccessor implements ValueAccessor {

		private String value;
		
		public ConstantValueAccessor(String v) {
			value = v;
		}
		
		@Override
		public String getValue(SSConnection ssCon, WorkerGroup wg) {
			return value;
		}
		
	}
	

}
