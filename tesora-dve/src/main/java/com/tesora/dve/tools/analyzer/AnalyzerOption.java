// OS_STATUS: public
package com.tesora.dve.tools.analyzer;

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

import com.tesora.dve.exceptions.PEException;

public class AnalyzerOption {

	private final String name;
	private Object value;
	private final Object defValue;
	private final String definition;

	public AnalyzerOption(String name, String definition, Object defVal) {
		this.name = name;
		this.definition = definition;
		this.defValue = defVal;
		this.value = defVal;
	}

	public String getName() {
		return name;
	}

	public String getDefinition() {
		return definition;
	}

	public Object getCurrentValue() {
		return value;
	}

	public void setValue(String in) throws PEException {
		try {
			if (defValue instanceof Integer) {
				value = Integer.parseInt(in);
			} else if (defValue instanceof Boolean) {
				value = Boolean.parseBoolean(in);
			} else if (defValue instanceof String) {
				value = in;
			}
		} catch (final Throwable t) {
			throw new PEException("Unable to set value of " + name, t);
		}
	}

	public void resetToDefault() {
		value = defValue;
	}
}
