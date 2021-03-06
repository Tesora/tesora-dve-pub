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

import com.tesora.dve.errmap.AvailableErrors;
import com.tesora.dve.errmap.ErrorInfo;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;

public class FloatingPointValueConverter extends ValueMetadata<Double> {

	public FloatingPointValueConverter() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Double convertToInternal(String varName, String in) throws PEException {
		try {
			return Double.valueOf(in);
		} catch (final NumberFormatException nfe) {
			throw new SchemaException(new ErrorInfo(AvailableErrors.WRONG_TYPE_FOR_VARIABLE, varName));
		}
	}

	@Override
	public String convertToExternal(Double in) {
		return in.toString();
	}

	@Override
	public boolean isNumeric() {
		return true;
	}

	@Override
	public String getTypeName() {
		return "numeric";
	}

	@Override
	public String toRow(Double in) {
		return in.toString();
	}

}
