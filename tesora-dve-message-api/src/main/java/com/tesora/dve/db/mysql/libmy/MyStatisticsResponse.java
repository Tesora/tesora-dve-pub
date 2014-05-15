// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

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
