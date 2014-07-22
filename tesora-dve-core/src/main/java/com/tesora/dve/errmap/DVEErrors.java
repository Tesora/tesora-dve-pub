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

import com.tesora.dve.sql.parser.LexicalLocation;

public class DVEErrors {

	public static final OneParamErrorCode<String> INTERNAL = 
			new OneParamErrorCode<String>("INTERNAL",true);
	// first param is the dbname, second is the table name.
	public static final TwoParamErrorCode<String,String> TABLE_DNE = 
			new TwoParamErrorCode<String,String>("TABLE_DNE",false);
	// first param is the column name, second is the location
	public static final TwoParamErrorCode<String,LexicalLocation> COLUMN_DNE = 
			new TwoParamErrorCode<String,LexicalLocation>("COLUMN_DNE",false);
	// only param is table name
	public static final OneParamErrorCode<String> UNKNOWN_TABLE =
			new OneParamErrorCode<String>("UNKNOWN_TABLE",false);
	// first param is username, second is access spec
	// this will probably change
	public static final TwoParamErrorCode<String,String> UNKNOWN_USER =
			new TwoParamErrorCode<String,String>("UNKNOWN_USER",false);
	// no use database
	public static final ZeroParamErrorCode NO_DATABASE_SELECTED =
			new ZeroParamErrorCode("NO_DATABASE_SELECTED",false);
	// use on an unknown database/tenant
	public static final OneParamErrorCode<String> UNKNOWN_DATABASE =
			new OneParamErrorCode<String>("UNKNOWN_DATABASE",false);
	// single parameter is name of function
	public static final OneParamErrorCode<String> INCORRECT_PARAM_COUNT_FUNCTION_CALL =
			new OneParamErrorCode<String>("INCORRECT_PARAM_COUNT_FUNCTION_CALL",false);
	// container related errors
	public static final TwoParamErrorCode<String,String> INVALID_CONTAINER_DISCRIMINANT_COLUMN_UPDATE =
			new TwoParamErrorCode<String,String>("INVALID_CONTAINER_DISCRIMINANT_COLUMN_UPDATE",false);
	public static final OneParamErrorCode<String> INVALID_CONTAINER_DELETE =
			new OneParamErrorCode<String>("INVALID_CONTAINER_DELETE",false);
	
	public static final ErrorCode[] universe = new ErrorCode[] {
		TABLE_DNE,
		COLUMN_DNE,
		UNKNOWN_TABLE,
		UNKNOWN_USER,
		NO_DATABASE_SELECTED,
		UNKNOWN_DATABASE,
		INCORRECT_PARAM_COUNT_FUNCTION_CALL,
		
		
		INVALID_CONTAINER_DISCRIMINANT_COLUMN_UPDATE,
		INVALID_CONTAINER_DELETE,
		INTERNAL		
	};
}
