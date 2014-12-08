package com.tesora.dve.sql.parser;

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


public final class TokenTypes {

	public static final String[] tokenNames = PE.tokenNames;
	
	// used in FunctionName
	// these are all unsupported agg funs
	public static final int BIT_AND = PE.BIT_AND;
	public static final int BIT_OR = PE.BIT_OR;
	public static final int BIT_XOR = PE.BIT_XOR;
	
	// used in function name - test of various sorts
	public static final int AND = PE.AND;
	public static final int OR = PE.OR;
	public static final int XOR = PE.XOR;
	public static final int Equals_Operator = PE.Equals_Operator;
	public static final int Less_Or_Equals_Operator = PE.Less_Or_Equals_Operator;
	public static final int Less_Than_Operator = PE.Less_Than_Operator;
	public static final int Greater_Or_Equals_Operator = PE.Greater_Or_Equals_Operator;
	public static final int Greater_Than_Operator = PE.Greater_Than_Operator;
	public static final int IN = PE.IN;
	public static final int NOTIN = PE.NOTIN;
	public static final int NOT = PE.NOT;
	public static final int Not_Equals_Operator = PE.Not_Equals_Operator;
	public static final int MAX = PE.MAX;
	public static final int DATABASE = PE.DATABASE;
	public static final int SCHEMA = PE.SCHEMA;
	public static final int COUNT = PE.COUNT;
	public static final int AVG = PE.AVG;
	public static final int STD = PE.STD;
	public static final int STDDEV = PE.STDDEV;
	public static final int STDDEV_POP = PE.STDDEV_POP;
	public static final int STDDEV_SAMP = PE.STDDEV_SAMP;
	public static final int VAR_POP = PE.VAR_POP;
	public static final int VAR_SAMP = PE.VAR_SAMP;
	public static final int VARIANCE = PE.VARIANCE;
	public static final int SUM = PE.SUM;
	public static final int MIN = PE.MIN;
	public static final int IS = PE.IS;
	public static final int NOTIS = PE.NOTIS;
	public static final int NOTLIKE = PE.NOTLIKE;
	public static final int CAST = PE.CAST;
	public static final int CHAR = PE.CHAR;
	public static final int CONVERT = PE.CONVERT;
	public static final int BETWEEN = PE.BETWEEN;
	public static final int NOTBETWEEN = PE.NOTBETWEEN;
	public static final int LIKE = PE.LIKE;
	public static final int Concatenation_Operator = PE.Concatenation_Operator;
	public static final int Double_Ampersand = PE.Double_Ampersand;
	public static final int IFNULL = PE.IFNULL;
	public static final int GROUP_CONCAT = PE.GROUP_CONCAT;
	public static final int Slash = PE.Slash;
	public static final int Minus_Sign = PE.Minus_Sign;
	public static final int Plus_Sign = PE.Plus_Sign;
	public static final int Asterisk = PE.Asterisk;
	public static final int COALESCE = PE.COALESCE;
	public static final int EXISTS = PE.EXISTS;
	public static final int RAND = PE.RAND;
	public static final int POW = PE.POW;
	public static final int SQRT = PE.SQRT;
	
	// used in translatorutils, utils, others
	public static final int BOOLEAN = PE.BOOLEAN;
	public static final int GLOBAL = PE.GLOBAL;
	public static final int Character_String_Literal = PE.Character_String_Literal;
	public static final int TIMESTAMPDIFF = PE.TIMESTAMPDIFF;
	public static final int NULL = PE.NULL;
	public static final int Unsigned_Large_Integer = PE.Unsigned_Large_Integer;
	public static final int Regular_Identifier = PE.Regular_Identifier;
	public static final int Signed_Float = PE.Signed_Float;
	public static final int Unsigned_Float = PE.Unsigned_Float;
	public static final int Signed_Integer = PE.Signed_Integer;
	public static final int Unsigned_Integer = PE.Unsigned_Integer;
	public static final int Signed_Large_Integer = PE.Signed_Large_Integer;
	public static final int Percent = PE.Percent;
	public static final int Ampersand = PE.Ampersand;
	public static final int REGEXP = PE.REGEXP;
	public static final int RLIKE = PE.RLIKE;
	public static final int Vertical_Bar = PE.Vertical_Bar;
	public static final int TRUE = PE.TRUE;
	public static final int FALSE = PE.FALSE;
	public static final int National_Character_String_Literal = PE.National_Character_String_Literal;
	public static final int Bit_String_Literal = PE.Bit_String_Literal;
	public static final int Hex_String_Literal = PE.Hex_String_Literal;
	
	public static final int UUID = PE.UUID;
	public static final int CURRENT_USER = PE.CURRENT_USER;
	public static final int LAST_INSERT_ID = PE.LAST_INSERT_ID;
	
	public static final int IF = PE.IF;
	
	public static final int EOF = PE.EOF;
}
