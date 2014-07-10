package com.tesora.dve.variables;

import com.tesora.dve.exceptions.PEException;

public class FloatingPointValueConverter extends ValueMetadata<Double> {

	public FloatingPointValueConverter() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Double convertToInternal(String in) throws PEException {
		try {
			return Double.valueOf(in);
		} catch (NumberFormatException nfe) {
			throw new PEException("Not a floating point value '" + "'");
		}
	}

	@Override
	public String convertToExternal(Double in) {
		return in.toString();
	}

}
