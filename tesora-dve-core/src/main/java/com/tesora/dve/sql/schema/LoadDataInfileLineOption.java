// OS_STATUS: public
package com.tesora.dve.sql.schema;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.PEStringUtils;

public class LoadDataInfileLineOption {
	public static final String DEFAULT_STARTING_DELIMITER = ""; 
	public static final String DEFAULT_TERMINATED_DELIMITER = "\n"; 
	
	private String starting = DEFAULT_STARTING_DELIMITER;
	private String terminated = DEFAULT_TERMINATED_DELIMITER;
		
	public LoadDataInfileLineOption() {
	}
	
	public LoadDataInfileLineOption(String starting, String terminated) {
		if (!StringUtils.isBlank(starting)) this.starting = StringEscapeUtils.unescapeJava(PEStringUtils.dequote(starting));
		if (!StringUtils.isBlank(terminated)) this.terminated = StringEscapeUtils.unescapeJava(PEStringUtils.dequote(terminated));
	}

	@Override
	public String toString() {
		return "LoadDataInfileColOption [starting=" + starting + ", terminated=" + terminated + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((starting == null) ? 0 : starting.hashCode());
		result = prime * result + ((terminated == null) ? 0 : terminated.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LoadDataInfileLineOption other = (LoadDataInfileLineOption) obj;
		if (!StringUtils.equals(starting, other.getStarting()) || 
				!StringUtils.equals(terminated, other.getTerminated())
				)
			return false;
		return true;
	}

	public String getTerminated() {
		return terminated;
	}

	public void setTerminated(String terminated) {
		this.terminated = terminated;
	}

	public String getStarting() {
		return starting;
	}

	public void setStarting(String starting) {
		this.starting = starting;
	}
}
