package com.tesora.dve.db.mysql.libmy;

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

import io.netty.buffer.ByteBuf;

import java.text.DecimalFormat;

import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;


public class MyStatisticsResponse extends MyResponseMessage {

	long uptime = 0;
	long threads = 0;
	long questions = 0;
	long slowQueries = 0;
	long opens = 0;
	long flushTables = 0;
	long openTables = 0;
	float queriesPerSecAvg = 0;
	
	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
		
		StringBuffer message = new StringBuffer(110);
		message.append("Uptime: ").append(uptime);
		message.append(" Threads: ").append(threads);
		message.append(" Questions: ").append(questions);
		message.append(" Slow queries: ").append(slowQueries);
		message.append(" Opens: ").append(opens);
		message.append(" Flush tables: ").append(flushTables);
		message.append(" Open tables: ").append(openTables);
		message.append(" Queries per second avg: ").append(new DecimalFormat("#0.000").format(queriesPerSecAvg));
		cb.writeBytes(message.toString().getBytes());
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) throws PEException {
		throw new PECodingException(getClass().getSimpleName());
	}

	@Override
	public MyMessageType getMessageType() {
		throw new PECodingException(getClass().getSimpleName());
	}

	public long getThreads() {
		return threads;
	}

	public void setThreads(long threads) {
		this.threads = threads;
	}

	public void setQueriesPerSecAvg(float qps) {
		this.queriesPerSecAvg = qps;
	}

	public float getQueriesPerSec() {
		return queriesPerSecAvg;
	}

	public long getUptime() {
		return uptime;
	}

	public void setUptime(long uptime) {
		this.uptime = uptime;
	}

	public void setQuestions(long questions) {
		this.questions = questions;
	}

	public void setSlowQueries(long slowQueries) {
		this.slowQueries = slowQueries;
	}
}
