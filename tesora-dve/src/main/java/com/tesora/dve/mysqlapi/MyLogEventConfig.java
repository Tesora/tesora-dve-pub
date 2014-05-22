package com.tesora.dve.mysqlapi;

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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class MyLogEventConfig {
	String logMsgFile = null;
	String endLogFile = null;
	Long endLogPos = null;
	Set<String> dbFilter = new HashSet<String>();
	CountDownLatch latch;
	
	public MyLogEventConfig(CountDownLatch latch) {
		this.latch = latch;
	}
	
	public String getLogMsgFile() {
		return logMsgFile;
	}
	public void setLogMsgFile(String logMsgFile) {
		this.logMsgFile = logMsgFile;
	}
	public String getEndLogFile() {
		return endLogFile;
	}
	public void setEndLogFile(String endLogFile) {
		this.endLogFile = endLogFile;
	}
	public Long getEndLogPos() {
		return endLogPos;
	}
	public void setEndLogPos(Long endLogPos) {
		this.endLogPos = endLogPos;
	}
	public Set<String> getDbFilter() {
		return dbFilter;
	}
	public void setDbFilter(Set<String> dbFilter) {
		this.dbFilter = dbFilter;
	}
	
	public CountDownLatch getLatch() {
		return latch;
	}



}
