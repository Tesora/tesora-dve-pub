// OS_STATUS: public
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

import java.sql.Types;

import com.tesora.dve.sql.util.ListSet;
import org.apache.commons.lang.StringUtils;

import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.IdentifierLiteralExpression;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.SchemaVariables;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;

/**
 * Value        	Nullable 	Default			On Update		Insert Set Timestamp	Update Set Timestamp
 * -----			--------	-------			---------		--------------------	--------------------
 * null				0			0				not specified	yes (1)					yes (1)
 * null				0			0				current_ts		yes (1)					yes (1)
 * null				1		    0				not specified	no						no
 * null				1		    0				current_ts		no 						no
 * null				0		   	not specified	not specified	yes (1)					yes (1)
 * null				0		   	not specified	current_ts		yes (1)					yes (1)
 * null				1		   	not specified	not specified	no 						no
 * null				1		   	not specified	current_ts		no 						no
 * null				0		   	literal			not specified	yes (1)					yes (1)
 * null				0		   	literal			current_ts		yes (1)					yes (1)
 * null				1		   	literal			not specified	no 						no
 * null				1		   	literal			current_ts		no 						no
 * null				0		   	current_ts		not specified	yes (1)					yes (1)
 * null				0		   	current_ts		current_ts		yes (1)					yes (1)
 * null				1		   	current_ts		not specified	no 						no
 * null				1		   	current_ts		current_ts		no 						no
 * null				0		   	null			not specified	yes (1)					yes (1)
 * null				0		   	null			current_ts		yes (1)					yes (1)
 * null				1		   	null			not specified	no 						no
 * null				1		   	null			current_ts		no 						no
 * 
 * literal			0			0				not specified	no						no
 * literal			0			0				current_ts		no						no
 * literal			1		    0				not specified	no						no
 * literal			1		    0				current_ts		no						no
 * literal			0		   	not specified	not specified	no						no
 * literal			0		   	not specified	current_ts		no						no
 * literal			1		   	not specified	not specified	no						no
 * literal			1		   	not specified	current_ts		no						no
 * literal			0		   	literal			not specified	no						no
 * literal			0		   	literal			current_ts		no						no
 * literal			1		   	literal			not specified	no						no
 * literal			1		   	literal			current_ts		no						no
 * literal			0		   	current_ts		not specified	no						no
 * literal			0		   	current_ts		current_ts		no						no
 * literal			1		   	current_ts		not specified	no						no
 * literal			1		   	current_ts		current_ts		no						no
 * literal			0		   	null			not specified	no						no
 * literal			0		   	null			current_ts		no						no
 * literal			1		   	null			not specified	no						no
 * literal			1		   	null			current_ts		no						no
 * 
 * current_ts		0			0				not specified	yes (2)					yes (2)
 * current_ts		0			0				current_ts		yes (2)					yes (2)
 * current_ts		1		    0				not specified	yes (2)					yes (2)
 * current_ts		1		    0				current_ts		yes (2)					yes (2)
 * current_ts		0		   	not specified	not specified	yes (2)					yes (2)
 * current_ts		0		   	not specified	current_ts		yes (2)					yes (2)
 * current_ts		1		   	not specified	not specified	yes (2)					yes (2)
 * current_ts		1		   	not specified	current_ts		yes (2)					yes (2)
 * current_ts		0		   	literal			not specified	yes (2)					yes (2)
 * current_ts		0		   	literal			current_ts		yes (2)					yes (2)
 * current_ts		1		   	literal			not specified	yes (2)					yes (2)
 * current_ts		1		   	literal			current_ts		yes (2)					yes (2)
 * current_ts		0		   	current_ts		not specified	yes (2)					yes (2)
 * current_ts		0		   	current_ts		current_ts		yes (2)					yes (2)
 * current_ts		1		   	current_ts		not specified	yes (2)					yes (2)
 * current_ts		1		   	current_ts		current_ts		yes (2)					yes (2)
 * current_ts		0		   	null			not specified	yes (2)					yes (2)
 * current_ts		0		   	null			current_ts		yes (2)					yes (2)
 * current_ts		1		   	null			not specified	yes (2)					yes (2)
 * current_ts		1		   	null			current_ts		yes (2)					yes (2)
 *     
 * not specified	0			0				not specified	no						no (8)
 * not specified	0			0				current_ts		no						yes (3) 
 * not specified	1		    0				not specified	no						no (8)
 * not specified	1		    0				current_ts		no						yes (3)
 * not specified	0		   	not specified	not specified	yes (4)					yes 
 * not specified	0		   	not specified	current_ts		no (6)					yes (3)
 * not specified	1		   	not specified	not specified	no						no (8)
 * not specified	1		   	not specified	current_ts		no (6)					yes (3)
 * not specified	0		   	literal			not specified	no						no (8)
 * not specified	0		   	literal			current_ts		no						yes (3)
 * not specified	1		   	literal			not specified	no						no (8)
 * not specified	1		   	literal			current_ts		no						yes (3)
 * not specified	0		   	current_ts		not specified	yes						no (8)
 * not specified	0		   	current_ts		current_ts		yes						yes (3)
 * not specified	1		   	current_ts		not specified	yes						no (8)
 * not specified	1		   	current_ts		current_ts		yes						yes (3)
 * not specified	0		   	null			not specified	no (5)					no (8)
 * not specified	0		   	null			current_ts		no (5)					yes (3)
 * not specified	1		   	null			not specified	no						no (8)
 * not specified	1		   	null			current_ts		no						yes (3)
 * 
 * 	  
 * 1. if value is null and column is NOT nullable then mysql will insert the current timestamp.
 * 2. now() or synonyms such as current_timestamp should set current_timestamp;
 * 3. With an ON UPDATE CURRENT_TIMESTAMP clause and a constant DEFAULT clause, the column is automatically updated to the current timestamp and has the given constant default value.
 * 4. With neither DEFAULT CURRENT_TIMESTAMP nor ON UPDATE CURRENT_TIMESTAMP, it is the same as specifying both DEFAULT CURRENT_TIMESTAMP and ON UPDATE CURRENT_TIMESTAMP
 * 5. syntax error
 * 6. With an ON UPDATE CURRENT_TIMESTAMP clause but no DEFAULT clause, the column is automatically updated to the current timestamp. The default is 0 unless the column is defined with the NULL attribute, in which case the default is NULL
 * 7. With a DEFAULT clause but no ON UPDATE CURRENT_TIMESTAMP clause, the column has the given default value and is not automatically updated to the current timestamp.
 * 8. With a constant, the default is the given value. In this case, the column has no automatic properties at all.
 *  
 */
public abstract class TimestampVariableUtils {
	public static final String TSFUNC_NOW = "now";
	public static final String TSFUNC_CURRENT_TIMESTAMP = "current_timestamp";
	public static final String TSFUNC_UNIX_TIMESTAMP = "unix_timestamp";
	public static final String TSFUNC_UTC_TIMESTAMP = "utc_timestamp";
		
	public static boolean setTimestampVariableForUnspecifiedColumn(SchemaContext sc, DMLStatement dmls, PEColumn column) {
		boolean ret = false;

		// only set the timestamp variable if this is a timestamp column
		if (column.getType().getBaseType().getDataType() != Types.TIMESTAMP) {
			return ret;
		}
		
		// for an update statement if the on update is set and 
		// the column is not specified then set the timestamp variable
		if ((dmls instanceof UpdateStatement) && column.isOnUpdated()) {
			ret = true;
			return ret;
		}
		
		
		boolean isNullable = column.isNullable();
		ExpressionNode defaultValue = column.getDefaultValue();
		if (defaultValue == null) {
			// no default value column modifier specified
			// now we need to know if the on update has also been set or not
			if (!column.isOnUpdated() && !isNullable) {
				// on update is not specified so default value becomes current timestamp
				ret = true;
			} 
//			else {
//				With an ON UPDATE CURRENT_TIMESTAMP clause but no DEFAULT clause, 
//				the column is automatically updated to the current timestamp. 
//				The default is 0 unless the column is defined with the NULL attribute, 
//				in which case the default is NULL.
//			}
		} else {
			if (dmls instanceof UpdateStatement) {
				if (column.isOnUpdated()) {
					ret = true;
				}
			} else {
				if (column.getDefaultValue() == null) {
					// null default value
					// do not set timestamp variable
				} else {
					Object o = column.getDefaultValue();
					if (o instanceof IdentifierLiteralExpression) {
						if (StringUtils.equals(((IdentifierLiteralExpression)o).asString(sc), "0")) {
							// do nothing
						} else {
							// for a timestamp column only other choice is current_timestamp
							ret = true;
						}
					} else if (o instanceof LiteralExpression) {
						// for literal default value (ie. 0 or 'yyyy-mm-dd hh:mm:ss') 
						// do not set timestamp variable
					}
				}
			}
		}
		
		return ret;
	}

	public static boolean setTimestampVariableForSpecifiedValue(PEColumn column, ExpressionNode value) {
		boolean ret = false;

		// only set the timestamp variable if this is a timestamp column
		if (column.getType().getBaseType().getDataType() != Types.TIMESTAMP) {
			return ret;
		}

		if (value instanceof LiteralExpression) {
			if (((LiteralExpression) value).isNullLiteral()) {
				// if value is null and column is NOT nullable 
				// then mysql will insert the current timestamp.
				if (!column.isNullable()) {
					ret = true;
				}
			} else {
				// user specified a literal (ie. 0 or 'yyyy-mm-dd hh:mm:ss') 
				// so don't set the timestamp variable
			}
		} else if (value instanceof IdentifierLiteralExpression ||
				value instanceof FunctionCall) {
			// for a timestamp column can only be current_timestamp
			ret = true;
		}
		
		return ret;
	}
	
	public static boolean isNowFunctionCallSpecified(ListSet<FunctionCall> functions) {
		boolean usesNowFunction = false;
		
		if (functions == null) {
			return usesNowFunction;
		}
		
		for(FunctionCall fc : functions) {
			if (isTimestampFunction(fc.getFunctionName().getUnquotedName().get())) {
				usesNowFunction = true;
				break;
			}
		}
		
		return usesNowFunction;
	}
	
	public static boolean isTimestampFunction(String name) {
		return (isFunctionCurrentTimestamp(name) ||
				isFunctionNow(name) ||
				isFunctionUnixTimestamp(name) ||
				isFunctionUTCTimestamp(name));
	}
	
	public static boolean isFunctionCurrentTimestamp(String name) {
		return StringUtils.equalsIgnoreCase(name, TSFUNC_CURRENT_TIMESTAMP);
	}
	
	public static boolean isFunctionNow(String name) {
		return StringUtils.equalsIgnoreCase(name, TSFUNC_NOW);
	}
	
	public static boolean isFunctionUnixTimestamp(String name) {
		return StringUtils.equalsIgnoreCase(name, TSFUNC_UNIX_TIMESTAMP);
	}
	
	public static boolean isFunctionUTCTimestamp(String name) {
		return StringUtils.equalsIgnoreCase(name, TSFUNC_UTC_TIMESTAMP);
	}
	
	public static long getCurrentUnixTime(SchemaContext sc) {
		Long ts = SchemaVariables.getReplTimestamp(sc);
		if (ts == null) {
			// we should be getting the local timezone of the mysql connection
			// but for now we will assume that the default is the same as the 
			// Java timezone
			ts = Long.valueOf((System.currentTimeMillis()/1000));
		}
		return ts;
	}

}
