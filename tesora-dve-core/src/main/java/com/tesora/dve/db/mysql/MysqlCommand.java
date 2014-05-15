// OS_STATUS: public
package com.tesora.dve.db.mysql;

import com.tesora.dve.clock.Timer;
import com.tesora.dve.clock.TimingService;
import com.tesora.dve.singleton.Singletons;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;

import com.tesora.dve.common.PEContext;
import com.tesora.dve.common.PEThreadContext;
import com.tesora.dve.exceptions.PEException;

public abstract class MysqlCommand {

	boolean executeImmediately = false;
	
	AtomicInteger resultsProcessedCount = new AtomicInteger();
	
	final PEContext debugContext = PEThreadContext.copy();

    //these Timers are used to measure how much time is being spent on the backend for a given frontend request.
    //the frontend timer is picked up from a thread local, since changing all the subclasses of MysqlCommand and their callers would be prohibitive.

    Timer frontendTimer = Singletons.require(TimingService.class).getTimerOnThread();

    //a place for MysqlCommandSenderHandler to hang backend timing info for this command.  Not great encapsulation / ood, ,but makes life much easier.
    protected Timer commandTimer;

    void executeInContext(ChannelHandlerContext ctx, Charset charset) throws PEException {
		PEThreadContext.inherit(debugContext.copy());
		PEThreadContext.pushFrame(getClass().getSimpleName()).put("cmd", this);
		try {
			execute(ctx, charset);
		} finally {
            PEThreadContext.clear();
		}
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

}
