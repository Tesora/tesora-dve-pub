// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;


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
	public void plan(SchemaContext pc, ExecutionSequence es) throws PEException {
		if (alreadyExists) return;
		PEStorageGroup sg = buildAllSitesGroup(pc);
		getCatalogObjects(pc);
		// TODO
		// manipulate the cat entities to add to both ends of rels
		for(PEUser peu : toCreate) {
			StringBuilder buf = new StringBuilder();
			buf.append("CREATE USER ");
            Singletons.require(HostService.class).getDBNative().getEmitter().emitUserDeclaration(peu, buf);
			es.append(new SimpleDDLExecutionStep(null, sg, (Persistable<?,?>)peu, getAction(), new SQLCommand(buf.toString()), 
					(List<CatalogEntity>)Collections.EMPTY_LIST, 
					Collections.singletonList((CatalogEntity)peu.getPersistent(pc)), null));
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
