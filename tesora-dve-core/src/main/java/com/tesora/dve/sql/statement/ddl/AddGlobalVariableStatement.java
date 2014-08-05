package com.tesora.dve.sql.statement.ddl;

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

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;

import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepDDLGeneralOperation.DDLCallback;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.schema.VariableScopeKind;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.transform.execution.ComplexDDLExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.variables.ServerGlobalVariableStore;
import com.tesora.dve.variables.ValueMetadata;
import com.tesora.dve.variables.VariableHandler;
import com.tesora.dve.variables.VariableManager;
import com.tesora.dve.variables.VariableOption;
import com.tesora.dve.worker.WorkerGroup;

//
// The sql would be
// alter dve add variable foo options='options' scopes='scopes' valuetype='type' value='value' description='help text'
// fields are all fairly self explanatory
public class AddGlobalVariableStatement extends AlterStatement {

	private final VariableHandler handler;
	
	
	public AddGlobalVariableStatement(VariableHandler newHandler) {
		super(true);
		this.handler = newHandler;
	}

	@Override
	public Action getAction() {
		return Action.ALTER;
	}

	@Override
	public Persistable<?, ?> getRoot() {
		return null;
	}

	@Override
	public CacheInvalidationRecord getInvalidationRecord(SchemaContext sc) {
		return null;
	}

	@Override
	protected ExecutionStep buildStep(SchemaContext sc) throws PEException {
		return new ComplexDDLExecutionStep(null,null,null,Action.ALTER,new AddVariableCallback(handler));
		
	}
	
	public static AddGlobalVariableStatement decode(SchemaContext sc, String varName, List<Pair<Name,LiteralExpression>> options) {
		EnumSet<VariableScopeKind> scopes = null;
		EnumSet<VariableOption> opts = null;
		ValueMetadata vmd = null;
		String value = null;
		String help = null;
		
		for(Pair<Name,LiteralExpression> p : options) {
			String key = p.getFirst().getUnqualified().getUnquotedName().get().toLowerCase(Locale.ENGLISH);
			String val = PEStringUtils.dequote(p.getSecond().asString(sc));
			if ("options".equals(key)) {
				opts = VariableHandler.convertOptions(val);
			} else if ("scopes".equals(key)) {
				scopes = VariableHandler.convertScopes(val);
			} else if ("value".equals(key)) {
				value = val;
			} else if ("valuetype".equals(key)) {
				try {
					vmd = VariableManager.findMetadataConverter(val, varName);
				} catch (PEException pe) {
					throw new SchemaException(Pass.SECOND, "Invalid type declaration for variable " + varName,pe);
				}
			} else if ("description".equals(key)) {
				help = val;
			}
		}
		if (scopes == null)
			throw new SchemaException(Pass.SECOND, "Must specify scopes");
		if (vmd == null)
			throw new SchemaException(Pass.SECOND, "Must specify variable type");
		if (value == null)
			throw new SchemaException(Pass.SECOND, "Must specify a value");
		if (opts == null)
			opts = EnumSet.noneOf(VariableOption.class);
		Object defVal = null;
		try {
			defVal = vmd.convertToInternal(varName, value);
		} catch (PEException pe) {
			throw new SchemaException(Pass.SECOND, "Invalid default value",pe);
		}
		VariableHandler vh = new VariableHandler(varName,vmd,scopes,defVal,opts,help);
		return new AddGlobalVariableStatement(vh);
	}
	
	private static class AddVariableCallback extends DDLCallback {

		private final VariableHandler newHandler;
		private Boolean success = null;
		
		public AddVariableCallback(VariableHandler handler) {
			this.newHandler = handler;
		}
		
		@Override
		public SQLCommand getCommand(CatalogDAO c) {
			return SQLCommand.EMPTY;
		}

		@Override
		public String description() {
			return "Add system variable " + newHandler.getName();
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return null;
		}
		
		public List<CatalogEntity> getUpdatedObjects() throws PEException {
			return Collections.singletonList((CatalogEntity)newHandler.buildNewConfig());
		}

		public void onCommit(SSConnection conn, CatalogDAO c, WorkerGroup wg) {
			success = Boolean.TRUE;
		}
		
		/*
		 * Called for every rollback
		 */
		public void onRollback(SSConnection conn, CatalogDAO c, WorkerGroup wg) {
			success = Boolean.FALSE;
		}

		public void onFinally(SSConnection conn) throws Throwable {
			if (Boolean.TRUE.equals(success))
				ServerGlobalVariableStore.INSTANCE.addVariable(newHandler, newHandler.getDefaultOnMissing());
		}

		public boolean requiresWorkers() {
			return false;
		}

	
	}
}
