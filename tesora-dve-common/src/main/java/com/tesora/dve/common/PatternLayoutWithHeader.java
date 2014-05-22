package com.tesora.dve.common;

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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.PatternLayout;

public class PatternLayoutWithHeader extends PatternLayout {

	public PatternLayoutWithHeader() {
		super();
	}

	@Override
	public String getHeader() {
		Runtime runtime = Runtime.getRuntime();

		StringBuffer header = new StringBuffer();
		header.append(StringUtils.repeat("-", 80));
		header.append("\nLog Started : ").append(new Date().toString());
		header.append("\nBuild Info  : ").append(PELogUtils.getBuildVersionString(true));
		header.append("\nMemory      : max=").append(String.format("%,d", runtime.maxMemory())).append(" total=")
				.append(String.format("%,d", runtime.totalMemory())).append(" free=").append(String.format("%,d", runtime.freeMemory()));
		header.append("\nProcessors  : ").append(runtime.availableProcessors());
		try {
			header.append("\nHost        : ").append(InetAddress.getLocalHost());
		} catch (UnknownHostException e) {
			header.append("\nHost        : unknown");
		}
		header.append("\n");
		return header.toString();
	}
}
