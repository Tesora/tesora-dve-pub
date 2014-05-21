// OS_STATUS: public
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

import com.tesora.dve.db.mysql.portal.protocol.MSPComPrepareStmtRequestMessage;
import com.tesora.dve.db.mysql.portal.protocol.MSPMessage;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.tesora.dve.charset.NativeCharSet;
import com.tesora.dve.charset.mysql.MysqlNativeCharSet;
import com.tesora.dve.db.mysql.MysqlPrepareStatementForwarder;
import com.tesora.dve.db.mysql.MyPrepStmtConnectionContext;
import com.tesora.dve.db.mysql.libmy.MyPreparedStatement;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.messages.PrepareRequestExecutor;


public class MSPComPrepareStmtRequest extends MSPActionBase {
	private static final Logger logger = Logger.getLogger(MSPComPrepareStmtRequest.class);
	
	public final static MSPComPrepareStmtRequest INSTANCE = new MSPComPrepareStmtRequest();

    @Override
	public void execute(ExecutorService clientExecutorService, ChannelHandlerContext ctx,
                        SSConnection ssCon, MSPMessage protocolMessage) throws PEException {

        MSPComPrepareStmtRequestMessage prepareRequest = castProtocolMessage(MSPComPrepareStmtRequestMessage.class,protocolMessage);

		byte[] query = prepareRequest.getPrepareBytes();

		MyPrepStmtConnectionContext mpscc = ctx.attr(MyPrepStmtConnectionContext.PSTMT_CONTEXT_KEY).get();

		if ( mpscc == null ) {
			mpscc = new MyPrepStmtConnectionContext(ssCon);
			ctx.attr(MyPrepStmtConnectionContext.PSTMT_CONTEXT_KEY).set(mpscc);
		}
		
		MyPreparedStatement<String> pStmt = new MyPreparedStatement<String>(null);
		pStmt.setQuery(query);
		mpscc.addPreparedStatement(pStmt);

		NativeCharSet clientCharSet = MysqlNativeCharSet.UTF8;
		MysqlPrepareStatementForwarder resultConsumer = new MysqlPrepareStatementForwarder(ctx, pStmt);
		try {
			PrepareRequestExecutor.execute(ssCon, resultConsumer, pStmt.getStmtId(), clientCharSet.getJavaCharset(), query);
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
		return (byte) 0x16;
	}
}
