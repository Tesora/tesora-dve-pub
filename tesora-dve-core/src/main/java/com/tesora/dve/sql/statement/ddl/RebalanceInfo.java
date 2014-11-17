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

import com.tesora.dve.db.Emitter;
import com.tesora.dve.db.GenericSQLCommand;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.db.Emitter.EmitterInvoker;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TempTableInstance;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.TruncateStatement;

public abstract class RebalanceInfo {

	protected final SchemaContext cntxt;
	
	public RebalanceInfo(SchemaContext cntxt) {
		this.cntxt = cntxt;
	}

	protected SQLCommand getCommand(Statement stmt) {
		EmitOptions opts = EmitOptions.NONE.addQualifiedTables();
        GenericSQLCommand gsql = stmt.getGenericSQL(cntxt, Singletons.require(HostService.class).getDBNative().getEmitter(), opts);
		return gsql.resolve(cntxt.getValues(),true,null).getSQLCommand();			
	}
	
	protected SQLCommand getInsertPrefix(final InsertIntoValuesStatement iivs) {
		try {
			EmitOptions opts = EmitOptions.NONE.addQualifiedTables();
			Emitter emitter = Singletons.require(HostService.class).getDBNative().getEmitter();
			emitter.setOptions(opts);
			final GenericSQLCommand prefix = new EmitterInvoker(emitter) {
				@Override
				protected void emitStatement(final SchemaContext sc, final StringBuilder buf) {
					getEmitter().emitInsertPrefix(sc, sc.getValues(),iivs, buf);
				}
			}.buildGenericCommand(cntxt);
			return prefix.resolve(cntxt.getValues(),null).getSQLCommand();
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to build insert prefix",pe);
		}

	}
	
	protected SQLCommand getTruncateSideTableStmt(TempTable tt) {
		TableInstance ti = new TempTableInstance(cntxt,tt);
		TruncateStatement trunc = new TruncateStatement(ti,null);
		return getCommand(trunc);
	}
	
	protected SQLCommand getCreateSideTableStmt(TempTable tt) {
		PECreateTableStatement stmt = new PECreateTableStatement(tt,true,false);
		return getCommand(stmt);
	}
	
	protected SQLCommand getDropSideTableStmt(TempTable tt) {
		PEDropTableStatement stmt = new PEDropTableStatement(cntxt,Collections.singletonList(new TempTableInstance(cntxt,tt).getTableKey()),
				Collections.EMPTY_LIST,true,false);
		return getCommand(stmt);
	}

	
}
