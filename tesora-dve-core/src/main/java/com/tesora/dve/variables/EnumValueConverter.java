package com.tesora.dve.variables;

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

import java.util.LinkedHashMap;

import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.errmap.DVEErrors;
import com.tesora.dve.errmap.ErrorInfo;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;

public class EnumValueConverter<E extends Enum<E>> extends ValueMetadata<E> {

	private final LinkedHashMap<String,E> universe;
	
	public EnumValueConverter(E[] values) {
		universe = new LinkedHashMap<String,E>();
		for(E e : values) {
			universe.put(e.toString(), e);
		}
	}
	
	@Override
	public E convertToInternal(String varName, String in) throws PEException {
		String deq = PEStringUtils.dequote(in);
		E any = universe.get(deq);
		if (any == null) {
			throw new SchemaException(new ErrorInfo(DVEErrors.WRONG_VALUE_FOR_VARIABLE, varName, deq));
		}
		return any;
	}
	
	@Override
	public String convertToExternal(E in) {
		return String.format("'%s'", in.toString());
	}

	@Override
	public boolean isNumeric() {
		return false;
	}

	@Override
	public String getTypeName() {
		return "varchar";
	}

	@Override
	public String toRow(E in) {
		return in.toString();
	}

}
