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

import java.sql.Types;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.BooleanUtils;

import com.tesora.dve.errmap.AvailableErrors;
import com.tesora.dve.errmap.ErrorInfo;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.collector.ResultCollector;
import com.tesora.dve.resultset.collector.ResultCollector.ResultCollectorFactory;
import com.tesora.dve.sql.SchemaException;

public class BooleanValueConverter extends ValueMetadata<Boolean> {
	
	public static enum BooleanToStringConverter {
		YES_NO_CONVERTER {
			@Override
			protected String convert(final boolean in) {
				return BooleanUtils.toStringYesNo(in);
			}
		},
		ON_OFF_CONVERTER {
			@Override
			protected String convert(final boolean in) {
				return BooleanUtils.toStringOnOff(in);
			}
		},
		BINARY_CONVERTER {
			@Override
			protected String convert(boolean in) {
				return String.valueOf(BooleanUtils.toInteger(in));
			}
		};
		
		public final String getStringValue(final Boolean in) {
			return convert(getNullSafeToBoolean(in)).toUpperCase();
		}

		protected abstract String convert(final boolean in);

		private boolean getNullSafeToBoolean(final Boolean in) {
			return BooleanUtils.toBoolean(in);
		}
	}

	@SuppressWarnings("serial")
	private final static Set<String> trueMap = new HashSet<String>(){{ add("true"); add("yes"); add("on"); add("1"); }};
	@SuppressWarnings("serial")
	private final static Set<String> falseMap = new HashSet<String>(){{ add("false"); add("no"); add("off"); add("0"); }};

	private final BooleanToStringConverter toStringConverter;

	public BooleanValueConverter(final BooleanToStringConverter converter) {
		this.toStringConverter = converter;
	}

	@Override
	public Boolean convertToInternal(String varName, String in) throws PEException {
		String lc = in.trim().toLowerCase();
		if (trueMap.contains(lc))
			return true;
		if (falseMap.contains(lc))
			return false;
		throw new SchemaException(new ErrorInfo(AvailableErrors.WRONG_VALUE_FOR_VARIABLE, varName, lc));
	}

	@Override
	public String convertToExternal(Boolean in) {
		return BooleanToStringConverter.BINARY_CONVERTER.getStringValue(in);
	}

	@Override
	public ResultCollector getValueAsResult(Boolean in) throws PEException {
		return ResultCollectorFactory.getInstance(Types.BOOLEAN, in);
	}

	@Override
	public boolean isNumeric() {
		return false;
	}

	@Override
	public String getTypeName() {
		return "boolean";
	}

	@Override
	public String toRow(Boolean in) {
		return toStringConverter.getStringValue(in);
	}

}
