// OS_STATUS: public
package com.tesora.dve.db.mysql;

import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.db.mysql.portal.protocol.MSPComQuitRequestMessage;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.Charset;

import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;

public class MysqlQuitCommand extends MysqlCommand implements
		MysqlCommandResultsProcessor {
	
	public static MysqlQuitCommand INSTANCE = new MysqlQuitCommand();


    @Override
    public boolean isDone(ChannelHandlerContext ctx) {
        return false;
    }

    @Override
    public void packetStall(ChannelHandlerContext ctx) {
    }

    @Override
	public boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException {
		return isDone(ctx);
	}

	@Override
	public void failure(Exception e) {
		throw new PECodingException(this.getClass().getSimpleName() + " encountered unhandled exception", e);
	}

	@Override
	void execute(ChannelHandlerContext ctx, Charset charset)
			throws PEException {
        MSPComQuitRequestMessage quitRequest = new MSPComQuitRequestMessage();
        ctx.write(quitRequest);
    }

	@Override
	MysqlCommandResultsProcessor getResultHandler() {
		return this;
	}

}
