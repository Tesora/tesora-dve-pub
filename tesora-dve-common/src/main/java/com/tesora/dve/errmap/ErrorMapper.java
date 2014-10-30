package com.tesora.dve.errmap;

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


import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.tesora.dve.exceptions.HasErrorInfo;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PEMappedRuntimeException;

public class ErrorMapper {

	private static final Map<ErrorCode,ErrorCodeFormatter> formatters = buildFormatters();

	public static void initialize() throws PEException {
		for(ErrorCode ec : AvailableErrors.universe) {
			ErrorCodeFormatter mapped = formatters.get(ec);
			if (mapped == null) {
				throw new PEException("Missing error mapper for error code: " + ec.getName());
			}
		}
	}

	// namespace
	public static final FormattedErrorInfo makeResponse(HasErrorInfo se) {
		ErrorInfo ex = se.getErrorInfo();
		ErrorCodeFormatter ecf = formatters.get(ex.getCode());
		if (ecf == null) return null;
		try {
			return ecf.buildResponse(ex.getParams(),ex.getLocation());
		} catch (Throwable t) {
			return null;
		}

	}

	private static final SQLException makeException(ErrorInfo ex) {
		ErrorCodeFormatter ecf = formatters.get(ex.getCode());
		if (ecf == null) return null;
		try {
			return ecf.buildException(ex.getParams(), ex.getLocation());
		} catch (Throwable t) {
			return null;
		}
	}
	
	public static final SQLException makeException(PEMappedRuntimeException se) {
		if (se.getErrorInfo() == null) return new SQLException(se);
		SQLException any = makeException(se.getErrorInfo());
		if (any == null) return new SQLException(se);
		return any;
	}
	
	private static Map<ErrorCode,ErrorCodeFormatter> buildFormatters() {
		HashMap<ErrorCode,ErrorCodeFormatter> out = new HashMap<ErrorCode,ErrorCodeFormatter>();
		for (ErrorCodeFormatter ecf : AvailableErrorMessages.getAll()) {
			for(ErrorCode ec : ecf.getHandledCodes())
				out.put(ec,ecf);
		}
		return out;
	}
	
}
