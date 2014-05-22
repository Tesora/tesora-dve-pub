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
