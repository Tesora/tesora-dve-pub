package com.tesora.dve.db.mysql;

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

import com.tesora.dve.db.DBConnection;
import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.db.mysql.libmy.MyOKResponse;
import com.tesora.dve.db.mysql.portal.protocol.MSPComQuitRequestMessage;
import com.tesora.dve.exceptions.PECommunicationsException;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.Charset;

import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;

public class MysqlQuitCommand extends MysqlCommand implements
		MysqlCommandResultsProcessor {

    boolean closed = false;


    @Override
    public boolean isDone(ChannelHandlerContext ctx) {
        return closed;
    }

    @Override
    public void packetStall(ChannelHandlerContext ctx) {
    }

    @Override
	public boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException {
        if (message instanceof MyOKResponse)
            closed = true;
        return isDone(ctx);
	}

	@Override
	public void failure(Exception e) {
        if (e instanceof PECommunicationsException){
            PECommunicationsException comm = (PECommunicationsException)e;
            if (comm.getMessage().startsWith("Connection closed")){
                //a connection close can return an OK, or close the socket, so this isn't an error.
                closed = true;
                return;
            }
        }
		throw new PECodingException(this.getClass().getSimpleName() + " encountered unhandled exception", e);
	}

	@Override
	void execute(DBConnection.Monitor monitor, ChannelHandlerContext ctx, Charset charset)
			throws PEException {
        MSPComQuitRequestMessage quitRequest = MSPComQuitRequestMessage.newMessage();
        ctx.write(quitRequest);
    }

	@Override
	MysqlCommandResultsProcessor getResultHandler() {
		return this;
	}

    @Override
    public void active(ChannelHandlerContext ctx) {
        //NOOP.
    }

}
