// OS_STATUS: public
package com.tesora.dve.db.mysql.portal;

import com.tesora.dve.db.mysql.portal.protocol.MSPAuthenticateV10MessageMessage;
import com.tesora.dve.db.mysql.portal.protocol.MSPMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;

import java.util.concurrent.ExecutorService;

import com.tesora.dve.charset.NativeCharSet;
import com.tesora.dve.charset.mysql.MysqlNativeCharSetCatalog;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.db.mysql.libmy.MyOKResponse;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.messages.ExecuteRequestExecutor;
import com.tesora.dve.worker.UserCredentials;

public class MSPAuthenticateV10Message extends MSPActionBase {
	
	@Override
	public void execute(ExecutorService clientExecutorService, ChannelHandlerContext ctx, SSConnection ssCon, MSPMessage protocolMessage) throws PEException {

        MSPAuthenticateV10MessageMessage authMessage = castProtocolMessage(MSPAuthenticateV10MessageMessage.class,protocolMessage);
        byte sequenceId = authMessage.getSequenceID();

		ssCon.setClientCapabilities(authMessage.getClientCapabilities());

		byte clientCharsetId = authMessage.getCharsetID();
		String username = authMessage.getUsername();
		String password = authMessage.getPassword();

		// Login in the SSConnection
		MyMessage mysqlResp;
		try {
			UserCredentials userCred = new UserCredentials(username, password, false);
			ssCon.startConnection(userCred);
            String initialDB = authMessage.getInitialDatabase();
			if (! "".equals(authMessage.getInitialDatabase()) ) {
				final DBEmptyTextResultConsumer resultConsumer = new DBEmptyTextResultConsumer();
				byte[] query = ("USE " + initialDB).getBytes(CharsetUtil.UTF_8);
				ExecuteRequestExecutor.execute(ssCon, resultConsumer, query);
			}
			
			NativeCharSet cliendCharSet = MysqlNativeCharSetCatalog.DEFAULT_CATALOG.findNativeCharsetById(clientCharsetId);
			if (cliendCharSet != null) {
				mysqlResp = new MyOKResponse();
				mysqlResp.setPacketNumber(sequenceId + 1);
				ssCon.setClientCharSet(cliendCharSet);
			} else {
				mysqlResp = new MyErrorResponse(new PEException("Unsupported character set specified (id=" + clientCharsetId + ")"));
				mysqlResp.setPacketNumber(sequenceId + 1);
			}
		} catch (PEException e) {
			mysqlResp = new MyErrorResponse(e.rootCause());
			mysqlResp.setPacketNumber(sequenceId + 1);
		} catch (Throwable t) {
			mysqlResp = new MyErrorResponse(new Exception(t.getMessage()));
			mysqlResp.setPacketNumber(sequenceId + 1);
		}
		
		ctx.write(mysqlResp);
		ctx.flush();
	}

	@Override
	public byte getMysqlMessageType() {
		return (byte) 0xc0;
	}

	private static final class InstanceHolder {
		static final MSPAuthenticateV10Message INSTANCE = new MSPAuthenticateV10Message();
	}
	
	public static MSPAuthenticateV10Message getInstance() {
		return InstanceHolder.INSTANCE;
	}
}
