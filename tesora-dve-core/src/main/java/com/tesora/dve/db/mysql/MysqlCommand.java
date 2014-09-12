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

import com.tesora.dve.clock.Timer;
import com.tesora.dve.clock.TimingService;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.db.DBConnection;
import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.singleton.Singletons;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;

import com.tesora.dve.exceptions.PEException;

public abstract class MysqlCommand implements MysqlCommandResultsProcessor {

    //these Timers are used to measure how much time is being spent on the backend for a given frontend request.
    //the frontend timer is picked up from a thread local, since changing all the subclasses of MysqlCommand and their callers would be prohibitive.

    Timer frontendTimer = Singletons.require(TimingService.class).getTimerOnThread();

    //a place for MysqlCommandSenderHandler to hang backend timing info for this command.  Not great encapsulation / ood, ,but makes life much easier.
    protected Timer commandTimer;

    abstract void execute(DBConnection.Monitor monitor, ChannelHandlerContext ctx, Charset charset) throws PEException;

    @Override
    abstract public boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException;

    @Override
    abstract public void packetStall(ChannelHandlerContext ctx) throws PEException;

    @Override
    abstract public void failure(Exception e);

    @Override
    abstract public void active(ChannelHandlerContext ctx);


    final void executeInContext(DBConnection.Monitor monitor, ChannelHandlerContext ctx, Charset charset) throws PEException {
		execute(monitor, ctx, charset);
	}

    public boolean isExpectingResults(ChannelHandlerContext ctx){
        return true;
    }

}
