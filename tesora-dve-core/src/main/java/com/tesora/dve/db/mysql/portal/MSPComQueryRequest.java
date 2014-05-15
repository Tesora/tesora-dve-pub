// OS_STATUS: public
package com.tesora.dve.db.mysql.portal;

import com.tesora.dve.db.mysql.portal.protocol.MSPComQueryRequestMessage;
import com.tesora.dve.db.mysql.portal.protocol.MSPMessage;

import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.PEMysqlErrorException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.connectionmanager.messages.ExecuteRequestExecutor;
import com.tesora.dve.worker.MysqlTextResultForwarder;

public class MSPComQueryRequest extends MSPActionBase {
	
	static final Logger logger = Logger.getLogger(MSPComQueryRequest.class);

    static final Pattern IS_LOAD_DATA_STATEMENT = Pattern.compile("^\\s*load\\s*data.*", Pattern.CASE_INSENSITIVE);

    public static final MSPComQueryRequest INSTANCE = new MSPComQueryRequest();
    
    public MSPComQueryRequest() {
	}

	@Override
	public void execute(ExecutorService clientExecutorService, ChannelHandlerContext ctx,
                        SSConnection ssCon, MSPMessage protocolMessage) throws PEException {
        MSPComQueryRequestMessage queryMessage = castProtocolMessage(MSPComQueryRequestMessage.class,protocolMessage);
        byte sequenceId = protocolMessage.getSequenceID();
        byte[] query = queryMessage.getQueryBytes();
        executeQuery(ctx, ssCon, sequenceId, query);
	}

    public static void executeQuery(ChannelHandlerContext ctx, final SSConnection ssCon, byte sequenceId, final byte[] query) throws PEException {
//		final NativeCharSet clientCharSet = MysqlNativeCharSet.UTF8;
		final MysqlTextResultForwarder resultConsumer = new MysqlTextResultForwarder(ctx, sequenceId);
		try {
			ExecuteRequestExecutor.execute(ssCon, resultConsumer, query);
			resultConsumer.sendSuccess(ssCon);
		} catch (PEMysqlErrorException e) {
			if (logger.isDebugEnabled())
				logger.debug("Exception returned directly to user: ", e);
			// The result consumer has already processed the error, so we do nothing here
		} catch (PEException e) {
			if (logger.isInfoEnabled())
				logger.info("Exception returned to user: ", e);
			if (!e.hasCause(PEMysqlErrorException.class))
				resultConsumer.sendError(e.rootCause());
		} catch (Throwable t) {
			if (logger.isInfoEnabled())
				logger.info("Exception returned to user: ", t);
			resultConsumer.sendError(new Exception(t));
		}
	}
	
	@Override
	public byte getMysqlMessageType() {
		return (byte) 0x03;
	}
	
}
