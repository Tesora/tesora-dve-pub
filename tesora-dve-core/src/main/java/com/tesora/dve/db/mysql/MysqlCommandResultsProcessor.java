// OS_STATUS: public
package com.tesora.dve.db.mysql;

import com.tesora.dve.db.mysql.libmy.MyMessage;
import io.netty.channel.ChannelHandlerContext;

import com.tesora.dve.exceptions.PEException;

public interface MysqlCommandResultsProcessor {

    boolean isDone(ChannelHandlerContext ctx);

	boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException;

    /**
     * Notifies the processor that the source is temporarily out of messages.  This is mainly useful for allowing the
     * processor to buffer some number of messages in processPacket while they are available, but flush them if the upstream
     * socket runs out of immediately available bytes.
     *
     * @param ctx
     * @throws PEException
     */
    void packetStall(ChannelHandlerContext ctx) throws PEException;

	void failure(Exception e);

}