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
import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.User;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PEUser;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.statement.session.FlushPrivilegesStatement;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.SimpleDDLExecutionStep;
import com.tesora.dve.sql.util.Functional;

public class PECreateUserStatement extends
		PECreateStatement<PEUser, User> {

	private List<PEUser> toCreate;
	
	public PECreateUserStatement(List<PEUser> newUsers, boolean exists) {
		super(newUsers.get(0), false, false, "USER", exists);
		toCreate = newUsers;
	}

	@Override
	public List<CatalogEntity> createCatalogObjects(SchemaContext pc) throws PEException {
		pc.beginSaveContext();
		try {
			for(PEUser peu : toCreate)
				peu.persistTree(pc);
			return Functional.toList(pc.getSaveContext().getObjects());
		} finally {
			pc.endSaveContext();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void plan(SchemaContext pc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		if (alreadyExists) return;
		PEStorageGroup sg = buildAllSitesGroup(pc);
		getCatalogObjects(pc);
		// TODO
		// manipulate the cat entities to add to both ends of rels
		for(PEUser peu : toCreate) {
			StringBuilder buf = new StringBuilder();
			buf.append("CREATE USER ");
            Singletons.require(HostService.class).getDBNative().getEmitter().emitUserDeclaration(peu, buf);

			final FlushPrivilegesStatement flush = new FlushPrivilegesStatement();
			flush.plan(pc, es, null);
			es.append(new SimpleDDLExecutionStep(null, sg, (Persistable<?, ?>) peu, getAction(), new SQLCommand(pc, buf.toString()),
					(List<CatalogEntity>) Collections.EMPTY_LIST,
					Collections.singletonList((CatalogEntity) peu.getPersistent(pc)), null));
			flush.plan(pc, es, null);
		}
	}

	@Override
	protected CatalogModificationExecutionStep buildStep(SchemaContext pc) throws PEException {
		throw new PEException("Should not call PECreateUserStatement.buildStep - may have to build multiple users");
	}

	@Override
	public StatementType getStatementType() {
		return StatementType.CREATE_USER;
	}
		

}
