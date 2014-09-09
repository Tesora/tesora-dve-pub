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
import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.singleton.Singletons;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;

import com.tesora.dve.exceptions.PEException;

public abstract class MysqlCommand {

	boolean executeImmediately = false;
	
	AtomicInteger resultsProcessedCount = new AtomicInteger();

    //these Timers are used to measure how much time is being spent on the backend for a given frontend request.
    //the frontend timer is picked up from a thread local, since changing all the subclasses of MysqlCommand and their callers would be prohibitive.

    Timer frontendTimer = Singletons.require(TimingService.class).getTimerOnThread();

    //a place for MysqlCommandSenderHandler to hang backend timing info for this command.  Not great encapsulation / ood, ,but makes life much easier.
    protected Timer commandTimer;

    void executeInContext(ChannelHandlerContext ctx, Charset charset) throws PEException {
		execute(ctx, charset);
	}

	abstract void execute(ChannelHandlerContext ctx, Charset charset) throws PEException;
	
	abstract MysqlCommandResultsProcessor getResultHandler();

	public boolean isExecuteImmediately() {
		return executeImmediately;
	}

	public void setExecuteImmediately(boolean executeImmediately) {
		this.executeImmediately = executeImmediately;
	}
	
	public boolean isPreemptable() {
		return false;
	}

	public void incrementResultsProcessedCount() {
		resultsProcessedCount.incrementAndGet();
	}

	public int resultsProcessedCount() {
		return resultsProcessedCount.get();
	}

    //Delegate calls on nested results handler, to reduce structural dependency, AKA, law of demeter.

    boolean processPacket(ChannelHandlerContext ctx, MyMessage message) throws PEException {
        return getResultHandler().processPacket(ctx,message);
    }

    public void packetStall(ChannelHandlerContext ctx) throws PEException {
        getResultHandler().packetStall(ctx);
    }

    public void failure(Exception e){
        getResultHandler().failure(e);
    }

    public boolean isDone(ChannelHandlerContext ctx){
        return getResultHandler().isDone(ctx);
    }

    public void active(ChannelHandlerContext ctx) {
        getResultHandler().active(ctx);
    }
    
}
