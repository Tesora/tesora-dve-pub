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



public final class MySQLErrors {
	
	public static final TwoParamErrorCodeFormatter<String, String> missingTableFormatter =
			new TwoParamErrorCodeFormatter<String,String>(AvailableErrors.TABLE_DNE,
					"Table '%s.%s' doesn't exist",
					1146,"42S02");
	public static final TwoParamErrorCodeFormatter<String, String> missingColumnFormatter =
			new TwoParamErrorCodeFormatter<String,String>(AvailableErrors.COLUMN_DNE,
					"Unknown column '%s' in '%s'",
					1054,
					"42S22");
	public static final OneParamErrorCodeFormatter<String> unknownTableFormatter =
			new OneParamErrorCodeFormatter<String>(AvailableErrors.UNKNOWN_TABLE,
					"Unknown table '%s'",
					1051,"42S02");
	public static final TwoParamErrorCodeFormatter<String, String> unknownUserFormatter =
			new TwoParamErrorCodeFormatter<String,String>(AvailableErrors.UNKNOWN_USER,
					"Operation DROP USER failed for '%s'@'%s'",
					1396,"HY000");
	public static final ZeroParamErrorCodeFormatter missingDatabaseFormatter =
			new ZeroParamErrorCodeFormatter(AvailableErrors.NO_DATABASE_SELECTED,
					"No database selected",
					1046,"3D000");
	public static final OneParamErrorCodeFormatter<String> unknownDatabaseFormatter =
			new OneParamErrorCodeFormatter<String>(AvailableErrors.UNKNOWN_DATABASE,
					"Unknown database '%s'",
					1049,
					"42000");
	public static final OneParamErrorCodeFormatter<String> incorrectParamCountFormatter =
			new OneParamErrorCodeFormatter<String>(AvailableErrors.INCORRECT_PARAM_COUNT_FUNCTION_CALL,
					"Incorrect parameter count in the call to native function '%s'",
					1582,
					"42000");
	public static final OneParamErrorCodeFormatter<String> unknownSysVarFormatter =
			new OneParamErrorCodeFormatter<String>(AvailableErrors.UNKNOWN_SYS_VAR,
					"Unknown system variable '%s'",
					1193,
					"HY000");
	public static final TwoParamErrorCodeFormatter<String, Long> tooLongTableCommentFormatter =
			new TwoParamErrorCodeFormatter<String, Long>(AvailableErrors.TOO_LONG_TABLE_COMMENT,
					"Comment for table '%s' is too long (max = %d).",
					1628,
					"HY000");
	public static final TwoParamErrorCodeFormatter<String, Long> tooLongTableFieldCommentFormatter =
			new TwoParamErrorCodeFormatter<String, Long>(AvailableErrors.TOO_LONG_TABLE_FIELD_COMMENT,
					"Comment for field '%s' is too long (max = %d).",
					1629,
					"HY000");
	public static final OneParamErrorCodeFormatter<String> nonUniqueTableFormatter =
			new OneParamErrorCodeFormatter<String>(AvailableErrors.NON_UNIQUE_TABLE,
					"Not unique table/alias: '%s'",
					1066,
					"42000");
	public static final OneParamErrorCodeFormatter<String> unknownCharacterSetFormatter =
			new OneParamErrorCodeFormatter<String>(AvailableErrors.UNKNOWN_CHARACTER_SET,
					"Unknown character set: '%s'",
					1115,
					"42000");
	public static final OneParamErrorCodeFormatter<String> unknownCollationFormatter =
			new OneParamErrorCodeFormatter<String>(AvailableErrors.UNKNOWN_COLLATION,
					"Unknown collation: '%s'",
					1273,
					"HY000");
	public static final TwoParamErrorCodeFormatter<String, String> wrongValueForVariable =
			new TwoParamErrorCodeFormatter<String, String>(AvailableErrors.WRONG_VALUE_FOR_VARIABLE,
					"Variable '%s' can't be set to the value of '%s'",
					1231,
					"42000");
	public static final OneParamErrorCodeFormatter<String> wrongTypeForVariable =
			new OneParamErrorCodeFormatter<String>(AvailableErrors.WRONG_TYPE_FOR_VARIABLE,
					"Incorrect argument type to variable '%s'",
					1232,
					"42000");
	public static final ZeroParamErrorCodeFormatter trgAlreadyExists =
			new ZeroParamErrorCodeFormatter(AvailableErrors.TRG_ALREADY_EXISTS,
					"Trigger already exists",
					1359,
					"HY000");
	public static final ZeroParamErrorCodeFormatter trgDoesNotExist =
			new ZeroParamErrorCodeFormatter(AvailableErrors.TRG_DOES_NOT_EXIST,
					"Trigger does not exist",
					1360,
					"HY000");
	public static final TwoParamErrorCodeFormatter<String, String> noSuchRowInTrg =
			new TwoParamErrorCodeFormatter<String, String>(AvailableErrors.NO_SUCH_ROW_IN_TRG,
					"There is no %s row in %s trigger",
					1363,
					"HY000");
			
	
	public static final ErrorCodeFormatter[] messages = new ErrorCodeFormatter[] {
		missingTableFormatter,
		missingColumnFormatter,
		unknownTableFormatter,
		unknownUserFormatter,
		missingDatabaseFormatter,
		unknownDatabaseFormatter,
		incorrectParamCountFormatter,
		unknownSysVarFormatter,
		tooLongTableCommentFormatter,
		tooLongTableFieldCommentFormatter,
		nonUniqueTableFormatter,
		unknownCharacterSetFormatter,
		unknownCollationFormatter,
		wrongValueForVariable,
		wrongTypeForVariable,
		trgAlreadyExists,
		trgDoesNotExist,
		noSuchRowInTrg
	};

			
}
