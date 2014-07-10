package com.tesora.dve.variables;

import java.util.LinkedHashMap;

import com.tesora.dve.exceptions.PEException;

public class EnumValueConverter<E extends Enum<E>> extends ValueMetadata<E> {

	private final LinkedHashMap<String,E> universe;
		
	public EnumValueConverter(E[] values) {
		universe = new LinkedHashMap<String,E>();
		for(E e : values) {
			universe.put(e.name(), e);
		}
	}
	
	@Override
	public E convertToInternal(String in) throws PEException {
		E any = universe.get(in);
		if (any == null)
			throw new PEException("Invalid value '" + in + "'");
		return any;
	}

	@Override
	public String convertToExternal(E in) {
		return in.name();
	}

}
