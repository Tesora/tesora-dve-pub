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

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import com.tesora.dve.db.mysql.libmy.MyErrorResponse;
import com.tesora.dve.sql.parser.LexicalLocation;

public class MySQLErrors {
	
	public static final ErrorCodeFormatter internalFormatter = 
			new OneParamErrorCodeFormatter<String>(DVEErrors.INTERNAL,"Internal error: %s",99,"99999");
	public static final ErrorCodeFormatter missingTableFormatter = 
			new TwoParamErrorCodeFormatter<String,String>(DVEErrors.TABLE_DNE,
					"Table '%s.%s' doesn't exist",
					1146,"42S02");
	public static final ErrorCodeFormatter missingColumnFormatter =
			new TwoParamErrorCodeFormatter<String,LexicalLocation>(DVEErrors.COLUMN_DNE,
					"Unknown column '%s' in '%s'",
					1054,
					"42S22") {
		@Override
		public String format(Object[] params) {
			LexicalLocation ll = (LexicalLocation) params[1];
			
			return String.format(format,new Object[] { params[0], locationStrings.get(ll) }); 
		}

	};
	public static final ErrorCodeFormatter unknownTableFormatter =
			new OneParamErrorCodeFormatter<String>(DVEErrors.UNKNOWN_TABLE,
					"Unknown table '%s'",
					1051,"42S02");
	public static final ErrorCodeFormatter unknownUserFormatter =
			new TwoParamErrorCodeFormatter<String,String>(DVEErrors.UNKNOWN_USER,
					"Operation DROP USER failed for '%s'@'%s'",
					1396,"HY000");
	public static final ErrorCodeFormatter missingDatabaseFormatter =
			new ZeroParamErrorCodeFormatter(DVEErrors.NO_DATABASE_SELECTED,
					"No database selected",
					1046,"3D000");
	public static final ErrorCodeFormatter unknownDatabaseFormatter =
			new OneParamErrorCodeFormatter<String>(DVEErrors.UNKNOWN_DATABASE,
					"Unknown database '%s'",
					1049,
					"42000");
	public static final ErrorCodeFormatter incorrectParamCountFormatter =
			new OneParamErrorCodeFormatter<String>(DVEErrors.INCORRECT_PARAM_COUNT_FUNCTION_CALL,
					"Incorrect parameter count in the call to native function '%s'",
					1582,
					"42000");
	public static final ErrorCodeFormatter invalidDiscriminantUpdateFormatter =
			new TwoParamErrorCodeFormatter<String,String>(DVEErrors.INVALID_CONTAINER_DISCRIMINANT_COLUMN_UPDATE,
					"Invalid update: discriminant column '%s' of container base table '%s' cannot be updated",
					6000,
					"DVECO");
	public static final ErrorCodeFormatter invalidContainerDeleteFormatter =
			new OneParamErrorCodeFormatter<String>(DVEErrors.INVALID_CONTAINER_DELETE,
					"Invalid delete on container base table '%s'.  Not restricted by discriminant columns",
					6001,
					"DVECO");
			
	
	public static final ErrorCodeFormatter[] myFormatters = new ErrorCodeFormatter[] {
		missingTableFormatter,
		missingColumnFormatter,
		unknownTableFormatter,
		unknownUserFormatter,
		missingDatabaseFormatter,
		unknownDatabaseFormatter,
		incorrectParamCountFormatter,
		invalidDiscriminantUpdateFormatter,
		invalidContainerDeleteFormatter,
		internalFormatter

	};

	private static final EnumMap<LexicalLocation,String> locationStrings = buildLocationStrings();
	
	private static EnumMap<LexicalLocation,String> buildLocationStrings() {
		EnumMap<LexicalLocation,String> out = new EnumMap<LexicalLocation,String>(LexicalLocation.class);
		out.put(LexicalLocation.PROJECTION,"field-list");
		out.put(LexicalLocation.HAVINGCLAUSE,"having clause");
		out.put(LexicalLocation.GROUPBYCLAUSE,"group statement");
		out.put(LexicalLocation.ONCLAUSE,"on clause");
		out.put(LexicalLocation.WHERECLAUSE,"where clause");
		out.put(LexicalLocation.ORDERBYCLAUSE,"order clause");
		return out;
	}
			
}
