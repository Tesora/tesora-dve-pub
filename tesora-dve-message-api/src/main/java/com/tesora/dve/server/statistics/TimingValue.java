// OS_STATUS: public
package com.tesora.dve.server.statistics;

import java.io.Serializable;

public class TimingValue implements Serializable {

	private static final long serialVersionUID = 1L;

	float responseTimeMS = 0; 
	int transactionsPerSecond = 0;
	long sampleSize = 0;

	public TimingValue() {
	}
	
	public TimingValue(float responseTimeMS, int tps) {
		this.responseTimeMS = responseTimeMS;
		this.transactionsPerSecond = tps;
		this.sampleSize = 1;
	}

	public TimingValue(float responseTimeMS, long sampleSize) {
		this.responseTimeMS = responseTimeMS;
		this.transactionsPerSecond = 0;
		this.sampleSize = sampleSize;
	}

	public float getResponseTimeMS() {
		return responseTimeMS;
	}
	public int getTransactionsPerSecond() {
		return transactionsPerSecond;
	}
	public long getSampleSize() {
		return sampleSize;
	}

	public void setResponseTimeMS(float responseTimeMS) {
		this.responseTimeMS = responseTimeMS;
	}

	public void setTransactionsPerSecond(int transactionsPerSecond) {
		this.transactionsPerSecond = transactionsPerSecond;
	}
	
	public void addToValues(float responseTime, long sampleSize) {
		this.responseTimeMS += responseTime;
		this.sampleSize += sampleSize;
	}
}
