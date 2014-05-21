// OS_STATUS: public
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

import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.db.mysql.portal.protocol.MSPComStmtCloseRequestMessage;
import com.tesora.dve.db.mysql.portal.protocol.MysqlGroupedPreparedStatementId;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.Charset;

import com.tesora.dve.db.mysql.libmy.MyPreparedStatement;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;

public class MysqlStmtCloseCommand extends MysqlCommand implements MysqlCommandResultsProcessor {
	

	MyPreparedStatement<MysqlGroupedPreparedStatementId> pstmt;

	public MysqlStmtCloseCommand(MyPreparedStatement<MysqlGroupedPreparedStatementId> pstmt) {
		this.pstmt = pstmt;
	}

	@Override
	public void execute(ChannelHandlerContext ctx, Charset charset) throws PEException {
        MSPComStmtCloseRequestMessage closeReq = MSPComStmtCloseRequestMessage.newMessage((byte) 0, pstmt.getStmtId().getStmtId(ctx.channel()));
        ctx.write(closeReq);
    }

    @Override
    public boolean isDone(ChannelHandlerContext ctx){
        //no response returned from server, returning true immediately is OK.
        return true;
    }

    @Override
    public void packetStall(ChannelHandlerContext ctx) {
    }

	@Override
	public boolean processPacket(ChannelHandlerContext ctx,MyMessage message) throws PEException {
		return isDone(ctx);
	}

	@Override
	public void failure(Exception e) {
		throw new PECodingException(this.getClass().getSimpleName() + " encountered unhandled exception", e);
	}

	@Override
	public MysqlCommandResultsProcessor getResultHandler() {
		return this;
	}
}
