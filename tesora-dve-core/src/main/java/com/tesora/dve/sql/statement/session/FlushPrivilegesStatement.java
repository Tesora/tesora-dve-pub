// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.SessionExecutionStep;

public class FlushPrivilegesStatement extends SessionStatement {

	public FlushPrivilegesStatement() {
		super("FLUSH PRIVILEGES");
	}

	@Override
	public void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
		// see if we can hit as many sites as possible
		PEStorageGroup allSitesGroup = buildAllSitesGroup(sc);
		es.append(new SessionExecutionStep(null, allSitesGroup, getSQL(sc)));
	}

}
