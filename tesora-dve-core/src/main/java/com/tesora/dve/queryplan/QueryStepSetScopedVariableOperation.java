package com.tesora.dve.queryplan;

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


import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.schema.VariableScopeKind;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepSetScopedVariableOperation extends
		QueryStepOperation {

	protected VariableScope scope;
	protected String variableName;
	protected ValueAccessor accessor;
	protected boolean requireWorkers = false;
	
	public QueryStepSetScopedVariableOperation(VariableScope vs, String variableName, ValueAccessor accessor, boolean requireWorkers) {
		super();
		this.scope = vs;
		this.variableName = variableName;
		this.accessor = accessor;
		this.requireWorkers = requireWorkers;
	}

	public QueryStepSetScopedVariableOperation(VariableScope vs, String variableName, String value) {
		this(vs, variableName, new ConstantValueAccessor(value), false);
	}
	
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer)
			throws Throwable {
		String nv = accessor.getValue(ssCon, wg);
		if (VariableScopeKind.GLOBAL == scope.getKind()) {
			Singletons.require(HostService.class).getVariableManager().lookup(variableName, true).setGlobalValue(nv);
		} else if (VariableScopeKind.SESSION == scope.getKind()) {
			ssCon.setSessionVariable(variableName, nv);
		} else if (VariableScopeKind.USER == scope.getKind()) {
			ssCon.setUserVariable(variableName, nv);
		} else if (VariableScopeKind.PERSISTENT == scope.getKind()) {
			Singletons.require(HostService.class).getVariableManager().lookup(variableName, true).setPersistentValue(ssCon.getCatalogDAO(), nv);
		} else {
            Singletons.require(HostService.class).setScopedVariable(scope.getScopeName(), variableName, nv);
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
