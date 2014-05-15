// OS_STATUS: public
package com.tesora.dve.db.mysql;

import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.Charset;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.concurrent.PEPromise;
import com.tesora.dve.exceptions.PEException;

public class MysqlForwardedExecuteCommand extends MysqlConcurrentCommand {
	
	static Logger logger = Logger.getLogger( MysqlForwardedExecuteCommand.class );

	final MysqlMultiSiteCommandResultsProcessor resultsHandler;
	final StorageSite site;

	public MysqlForwardedExecuteCommand(MysqlMultiSiteCommandResultsProcessor resultHandler, 
			PEPromise<Boolean> completionPromise, StorageSite site) {
		super(completionPromise);
		this.resultsHandler = resultHandler;
		this.site = site;
	}

	@Override
	public void execute(ChannelHandlerContext ctx, Charset charset) throws PEException {
		if (logger.isDebugEnabled())
			logger.debug("Written: " + this);
		resultsHandler.addSite(site, ctx);
	}

	@Override
	public MysqlCommandResultsProcessor getResultHandler() {
		return resultsHandler;
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName()+"{"+getPromise()+", " + site + "}";
	}

	@Override
	public boolean isPreemptable() {
		return true;
	}
}
