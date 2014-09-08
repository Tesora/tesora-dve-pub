package com.tesora.dve.mysqlapi.repl;

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

import com.tesora.dve.db.mysql.SynchronousResultProcessor;
import com.tesora.dve.exceptions.PECommunicationsException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.repl.messages.MyReplEvent;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.log4j.Logger;

import com.tesora.dve.db.mysql.libmy.MyEOFPktResponse;
import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.mysqlapi.MyClientConnectionContext;

public class MyReplSlaveAsyncHandler extends SynchronousResultProcessor {
	static final Logger logger = Logger.getLogger(MyReplSlaveAsyncHandler.class);

    protected MyClientConnectionContext context;
	protected MyReplicationSlaveService plugin;

	public MyReplSlaveAsyncHandler(MyClientConnectionContext clientContext, MyReplicationSlaveService plugin) {
		this.context = clientContext;
		this.plugin = plugin;
	}

    public MyClientConnectionContext getContext() {
        return context;
    }

    @Override
    public void active(ChannelHandlerContext ctx) {
        context.setCtx(ctx);
        super.active(ctx);
    }

    @Override
    public boolean processPacket(ChannelHandlerContext ctx, MyMessage msg) throws PEException {
		if (plugin.stopCalled()) {
			// don't process any more messages
			return false;
		}
		
		if (msg instanceof MyEOFPktResponse) {
			// TODO we need to implement retry logic - this means that the master went away
			if (logger.isDebugEnabled())
				logger.debug("EOF packet received from master");
			
			// we need to instrument this a little for the Repl Slave IT test
			// if we get an EOF pkt with -1 for status and warningCount we
			// are going to stop the slave (this helps the test)
			// For real use - we don't actually understand the real reason
			// this packet comes down so we are going to ignore it
			MyEOFPktResponse eofPkt = (MyEOFPktResponse) msg;
			if ( eofPkt.getStatusFlags() == -1 && eofPkt.getWarningCount() == -1 )
				plugin.stop();
		} else if (msg instanceof MyErrorResponse) {
			logger.error("Error received from master: "
					+ ((MyErrorResponse) msg).getErrorMsg());
		} else if (msg instanceof MyReplEvent) {
            MyReplicationVisitorDispatch dispatch = new MyReplicationVisitorDispatch(plugin);
			((MyReplEvent) msg).accept(dispatch);
		}
		return false;
	}

    @Override
    public void failure(Exception e) {

        if (e instanceof PECommunicationsException && logger.isDebugEnabled()){
            logger.debug("Channel closed.");
        }

        if (plugin.stopCalled())
            return;
        else {
            try {
                plugin.restart();
            } catch (PEException e1) {
                super.failure(e1);
            }
        }
    }
	
}
