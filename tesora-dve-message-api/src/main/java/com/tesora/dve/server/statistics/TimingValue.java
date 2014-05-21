package com.tesora.dve.server.statistics;

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
