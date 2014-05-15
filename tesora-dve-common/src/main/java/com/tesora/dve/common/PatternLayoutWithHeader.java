// OS_STATUS: public
package com.tesora.dve.common;

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
