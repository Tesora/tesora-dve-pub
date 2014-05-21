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

import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import com.tesora.dve.db.mysql.portal.protocol.MSPComInitDBRequestMessage;
import com.tesora.dve.db.mysql.portal.protocol.MSPComQueryRequestMessage;
import com.tesora.dve.db.mysql.portal.protocol.MSPMessage;
import io.netty.channel.ChannelHandlerContext;
import com.tesora.dve.charset.mysql.MysqlNativeCharSet;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;

public class MSPComInitDBRequest extends MSPActionBase {

	final static MSPComInitDBRequest INSTANCE = new MSPComInitDBRequest();

	@Override
	public void execute(ExecutorService clientExecutorService, ChannelHandlerContext ctx,
                        SSConnection ssCon, MSPMessage protocolMessage) throws PEException {

        MSPComInitDBRequestMessage initDBMessage = castProtocolMessage(MSPComInitDBRequestMessage.class,protocolMessage);
        byte sequenceId = protocolMessage.getSequenceID();

		MysqlNativeCharSet ncs = MysqlNativeCharSet.UTF8;
        Charset javaCharset = ncs.getJavaCharset();
        initDBMessage.setDecodingCharset(javaCharset);
        String database = initDBMessage.getInitialDatabase();
		String query = "use " + database;
        //TODO: convert this to an ExecuteRequestExecutor.execute() call, rather than construct protocol objects and call the protocol handler directly.
		MSPComQueryRequestMessage useRequest = MSPComQueryRequestMessage.newMessage(sequenceId,query, javaCharset);
		MSPComQueryRequest.INSTANCE.execute(clientExecutorService, ctx, ssCon, useRequest);
	}

	@Override
	public byte getMysqlMessageType() {
		return (byte) 0x02;
	}
}
