package com.tesora.dve.sql.schema;

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

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.PEStringUtils;

public class LoadDataInfileColOption {
	public static final String DEFAULT_ENCLOSED_DELIMITER = ""; 
	public static final String DEFAULT_ESCAPED_DELIMITER = "\\"; 
	public static final String DEFAULT_TERMINATED_DELIMITER = "\t"; 

	private String enclosed = DEFAULT_ENCLOSED_DELIMITER;
	private String escaped = DEFAULT_ESCAPED_DELIMITER;
	private String terminated = DEFAULT_TERMINATED_DELIMITER;
		
	public LoadDataInfileColOption() {
	}
	
	public LoadDataInfileColOption(String terminated, String enclosed, String escaped) {
		if (!StringUtils.isBlank(escaped)) this.escaped = StringEscapeUtils.unescapeJava(PEStringUtils.dequote(escaped));
		if (!StringUtils.isBlank(enclosed)) this.enclosed = StringEscapeUtils.unescapeJava(PEStringUtils.dequote(enclosed));
		if (!StringUtils.isBlank(terminated)) this.terminated = StringEscapeUtils.unescapeJava(PEStringUtils.dequote(terminated));
	}

	@Override
	public String toString() {
		return "LoadDataInfileColOption [enclosed=" + enclosed + ", escaped=" + escaped + ", terminated=" + terminated + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((enclosed == null) ? 0 : enclosed.hashCode());
		result = prime * result + ((escaped == null) ? 0 : escaped.hashCode());
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
		LoadDataInfileColOption other = (LoadDataInfileColOption) obj;
		if (!StringUtils.equals(enclosed, other.getEnclosed()) || 
				!StringUtils.equals(escaped, other.getEscaped()) ||
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

	public String getEnclosed() {
		return enclosed;
	}

	public void setEnclosed(String enclosed) {
		this.enclosed = enclosed;
	}

	public String getEscaped() {
		return escaped;
	}

	public void setEscaped(String escaped) {
		this.escaped = escaped;
	}

}
