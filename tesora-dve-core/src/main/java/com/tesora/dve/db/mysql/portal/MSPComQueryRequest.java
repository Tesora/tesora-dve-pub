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


import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.db.mysql.portal.protocol.MSPComQueryRequestMessage;
import com.tesora.dve.db.mysql.portal.protocol.MSPMessage;

import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.PEMysqlErrorException;
import com.tesora.dve.errmap.ErrorMapper;
import com.tesora.dve.errmap.FormattedErrorInfo;
import com.tesora.dve.exceptions.HasErrorInfo;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PEMappedException;
import com.tesora.dve.exceptions.PEMappedRuntimeException;
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
        byte[] query = queryMessage.getQueryBytes();
        executeQuery(ctx, ssCon, query);
	}

    public static void executeQuery(ChannelHandlerContext ctx, final SSConnection ssCon, final byte[] query) throws PEException {
//		final NativeCharSet clientCharSet = MysqlNativeCharSet.UTF8;
		final MysqlTextResultForwarder resultConsumer = new MysqlTextResultForwarder(ctx);
		try {
			ExecuteRequestExecutor.execute(ssCon, resultConsumer, query);
            //TODO: this response should really be generated inside execution.  Doing it here forces synchronous behavior and extra locking + context switching. -sgossard.
			resultConsumer.sendSuccess(ssCon);
		} catch (PEMysqlErrorException e) {
			if (logger.isDebugEnabled())
				logger.debug("Exception returned directly to user: ", e);
			// The result consumer has already processed the error, so we do nothing here			
		} catch (PEMappedRuntimeException se) {
			if (handleMappedError(resultConsumer,se))
				return;
		} catch (PEMappedException se) {
			if (handleMappedError(resultConsumer,se))
				return;			
		} catch (PEException e) {
			if (logger.isInfoEnabled())
				logger.info("Exception returned to user: ", e);
			if (!e.hasCause(PEMysqlErrorException.class)) {
				resultConsumer.sendError(e.rootCause());
			}
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
	
	private static <T extends HasErrorInfo>  boolean handleMappedError(MysqlTextResultForwarder resultConsumer, T ex) {
		FormattedErrorInfo fei = ErrorMapper.makeResponse(ex);
		if (fei != null) {
			MyErrorResponse err = new MyErrorResponse(fei);
			if (logger.isInfoEnabled() && ex.getErrorInfo().getCode().log())  
				logger.info("Exception returned to user: ", (Exception)ex);
			resultConsumer.sendError(err);
			return true;
		} else {
			resultConsumer.sendError((Exception)ex);
		}
		return false;
		
	}
	
}
