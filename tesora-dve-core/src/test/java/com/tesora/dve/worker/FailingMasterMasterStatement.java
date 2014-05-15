// OS_STATUS: public
package com.tesora.dve.worker;

import com.tesora.dve.db.DBConnection;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PECommunicationsException;
import com.tesora.dve.exceptions.PESQLException;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.worker.MasterMasterStatement;
import com.tesora.dve.worker.Worker;

public class FailingMasterMasterStatement extends MasterMasterStatement {
	
	static double failProbability = 1;
	
	public FailingMasterMasterStatement(Worker w, DBConnection connection)
			throws PESQLException {
		super(w, connection);
	}

	@Override
	protected boolean doExecute(SQLCommand sql, DBResultConsumer resultConsumer) throws PESQLException {
		if (Math.random() < failProbability)
			throw new PECommunicationsException(this.getClass().getSimpleName());
		else
			return super.doExecute(sql, resultConsumer);
	}

	public static void setFailProbability(double failProbability) {
		FailingMasterMasterStatement.failProbability = failProbability;
	}

}
