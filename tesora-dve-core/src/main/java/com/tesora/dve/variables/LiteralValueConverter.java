package com.tesora.dve.variables;

import com.tesora.dve.exceptions.PEException;

public class LiteralValueConverter extends ValueMetadata<String> {

	@Override
	public String convertToInternal(String in) throws PEException {
		return in;
	}

	@Override
	public String convertToExternal(String in) {
		return in;
	}

}
