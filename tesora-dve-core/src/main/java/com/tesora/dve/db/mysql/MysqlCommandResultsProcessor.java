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