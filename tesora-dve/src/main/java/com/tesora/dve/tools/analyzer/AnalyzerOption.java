// OS_STATUS: public
package com.tesora.dve.tools.analyzer;

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
