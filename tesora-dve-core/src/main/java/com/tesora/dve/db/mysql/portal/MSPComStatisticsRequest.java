package com.tesora.dve.db.mysql.portal;

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

import java.util.concurrent.ExecutorService;

import com.tesora.dve.db.mysql.portal.protocol.MSPMessage;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.global.StatusVariableService;
import com.tesora.dve.singleton.Singletons;
import io.netty.channel.ChannelHandlerContext;

import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.db.mysql.MysqlNativeConstants;
import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.db.mysql.libmy.MyResponseMessage;
import com.tesora.dve.db.mysql.libmy.MyStatisticsResponse;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.worker.agent.Envelope;
import com.tesora.dve.server.statistics.manager.GetStatisticsRequest;
import com.tesora.dve.server.statistics.manager.GetStatisticsResponse;


public class MSPComStatisticsRequest extends MSPActionBase {

	final static MSPComStatisticsRequest INSTANCE = new MSPComStatisticsRequest();
	
	@Override
	public void execute(ExecutorService clientExecutorService, ChannelHandlerContext ctx,
                        SSConnection ssCon, MSPMessage protocolMessage) throws PEException {
		MyResponseMessage respMsg = null;

		try {
			GetStatisticsRequest req = new GetStatisticsRequest();
            Envelope e = ssCon.newEnvelope(req).to(Singletons.require(HostService.class).getStatisticsManagerAddress());
			ResponseMessage rm = ssCon.sendAndReceive(e);

			MyStatisticsResponse statResp = new MyStatisticsResponse();

            StatusVariableService statusVariableService = Singletons.require(StatusVariableService.class);

			statResp.setThreads(Long.valueOf(statusVariableService.getStatusVariable(MysqlNativeConstants.MYSQL_THREAD_COUNT)));
			statResp.setUptime(Long.valueOf(statusVariableService.getStatusVariable(MysqlNativeConstants.MYSQL_UPTIME)));
			statResp.setQuestions(Long.valueOf(statusVariableService.getStatusVariable(MysqlNativeConstants.MYSQL_QUESTIONS)));
			//			statResp.setSlowQueries(Long.valueOf(Host.getStatusVariable(ssCon.getCatalogDAO(), MysqlNativeConstants.MYSQL_SLOW_QUERIES)));

			// we are going to use QPS over the last minute for now as it is all
			// that we have 
			statResp.setQueriesPerSecAvg(((GetStatisticsResponse) rm).getStats().getGlobalQuery().getOneMin().getTransactionsPerSecond());
			respMsg = statResp;
		} catch (PEException e) {
			respMsg = new MyErrorResponse(e.rootCause());
		} catch (Throwable t) {
			respMsg = new MyErrorResponse(new Exception(t.getMessage()));
		}

		ctx.channel().write(respMsg);
	}

	@Override
	public byte getMysqlMessageType() {
		return (byte) 0x09;
	}



}
