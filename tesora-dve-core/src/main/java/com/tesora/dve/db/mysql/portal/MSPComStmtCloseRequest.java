// OS_STATUS: public
package com.tesora.dve.db.mysql.portal;

import java.util.concurrent.ExecutorService;

import com.tesora.dve.db.mysql.portal.protocol.MSPComStmtCloseRequestMessage;
import com.tesora.dve.db.mysql.portal.protocol.MSPMessage;
import io.netty.channel.ChannelHandlerContext;

import com.tesora.dve.db.mysql.MyPrepStmtConnectionContext;
import com.tesora.dve.db.mysql.libmy.MyPreparedStatement;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryPlanner;
import com.tesora.dve.server.connectionmanager.SSConnection;


public class MSPComStmtCloseRequest extends MSPActionBase {
	
	public final static MSPComStmtCloseRequest INSTANCE = new MSPComStmtCloseRequest();

    @Override
	public void execute(ExecutorService clientExecutorService, ChannelHandlerContext ctx,
                        SSConnection ssCon, MSPMessage protocolMessage) throws PEException {

        MSPComStmtCloseRequestMessage stmtCloseMessage = castProtocolMessage(MSPComStmtCloseRequestMessage.class,protocolMessage);
		Long stmtId = stmtCloseMessage.getStatementID();
		
		MyPrepStmtConnectionContext mpscc = ctx.attr(MyPrepStmtConnectionContext.PSTMT_CONTEXT_KEY).get();
		MyPreparedStatement<String> pStmt = mpscc.getPreparedStatement(stmtId);
		if ( pStmt == null ) {
			throw new PEException("A prepared statement with id " + stmtId + " could not be found in the connection's context");
		}
		mpscc.removePreparedStatement(stmtId);
		QueryPlanner.destroyPreparedStatement(ssCon, Long.toString(stmtId));
	}

	@Override
	public byte getMysqlMessageType() {
		return (byte) 0x19;
	}

}