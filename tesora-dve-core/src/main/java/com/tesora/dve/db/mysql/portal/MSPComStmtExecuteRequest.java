package com.tesora.dve.db.mysql.portal;

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

import com.tesora.dve.clock.NoopTimingService;
import com.tesora.dve.clock.Timer;
import com.tesora.dve.clock.TimingService;
import com.tesora.dve.db.mysql.portal.protocol.MSPComStmtExecuteRequestMessage;
import com.tesora.dve.db.mysql.portal.protocol.MSPMessage;
import com.tesora.dve.singleton.Singletons;
import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.log4j.Logger;
import com.tesora.dve.db.mysql.libmy.MyParameter;
import com.tesora.dve.db.mysql.MyPrepStmtConnectionContext;
import com.tesora.dve.db.mysql.libmy.MyPreparedStatement;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.messages.ExecutePreparedStatementRequestExecutor;
import com.tesora.dve.worker.MysqlSyntheticPreparedResultForwarder;

public class MSPComStmtExecuteRequest extends MSPActionBase {

	static final Logger logger = Logger.getLogger(MSPComStmtExecuteRequest.class);

	public final static MSPComStmtExecuteRequest INSTANCE = new MSPComStmtExecuteRequest();

    TimingService timingService = Singletons.require(TimingService.class, NoopTimingService.SERVICE);
    enum TimingDesc {FRONTEND_STMT_EXEC_DECODE_META}

    @Override
	public void execute(ExecutorService clientExecutorService, ChannelHandlerContext ctx,
                        SSConnection ssCon, MSPMessage protocolMessage) throws PEException {
        MSPComStmtExecuteRequestMessage executeMessage = castProtocolMessage(MSPComStmtExecuteRequestMessage.class, protocolMessage);

		long stmtId = executeMessage.getStatementID(); 
		// Get the prepared statement and copy the parameters from the
		// packet
		MyPrepStmtConnectionContext mpscc = ctx.attr(
				MyPrepStmtConnectionContext.PSTMT_CONTEXT_KEY).get();
		MyPreparedStatement<String> pStmt = mpscc.getPreparedStatement(stmtId);

		if (pStmt == null) {
			throw new PEException("A prepared statement with id " + stmtId
					+ " could not be found in the connection's context");
		}


		MysqlSyntheticPreparedResultForwarder resultConsumer = new MysqlSyntheticPreparedResultForwarder(ctx);
		try {
            Timer readMetaTimer = timingService.startSubTimer(TimingDesc.FRONTEND_STMT_EXEC_DECODE_META);
			executeMessage.readParameterMetadata(pStmt);

			// Execute the statement with the last parameters
			List<Object> params = new ArrayList<Object>(pStmt.getNumParams());
			for (MyParameter mp : pStmt.getParameters())
				params.add((mp.getValue() == null ? null : mp.getValueForQuery()));
            readMetaTimer.end();
			ExecutePreparedStatementRequestExecutor.execute(ssCon, pStmt.getStmtId(), params, resultConsumer);
			resultConsumer.sendSuccess(ssCon);
			
		} catch (PEException e) {
			if (logger.isDebugEnabled())
				logger.warn("Reducing exception for user - original exception: ", e);
			resultConsumer.sendError(e.rootCause());
		} catch (Throwable t) {
			logger.warn("Reducing exception for user - original exception: ", t);
			resultConsumer.sendError(new Exception(t));
		}
	}

    @Override
	public byte getMysqlMessageType() {
		return (byte) 0x17;
	}

}
