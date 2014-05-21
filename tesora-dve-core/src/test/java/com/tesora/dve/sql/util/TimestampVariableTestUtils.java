// OS_STATUS: public
package com.tesora.dve.sql.util;

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

import org.apache.commons.lang.StringUtils;

public class TimestampVariableTestUtils {
	// Columns: Column Value, 
	//			Nullable, 
	//			Default Value, 
	//			On Update, 
	// 			Expected Insert Set Timestamp Variable, 
	//			Expected Update Set Timestamp Variable
	//			Ignore for sql execution
	static Object[][] testValues = {
			{"null",0,"0",0,1,1,0},
			{"null",0,"0",1,1,1,0},
			{"null",1,"0",0,0,0,0},
			{"null",1,"0",1,0,0,0},
			{"null",0,"",0,1,1,0},
			{"null",0,"",1,1,1,0},
			{"null",1,"",0,0,0,0},
			{"null",1,"",1,0,0,0},
			{"null",0,"'2000-01-01 01:02:03'",0,1,1,0},
			{"null",0,"'2000-01-01 01:02:03'",1,1,1,0},
			{"null",1,"'2000-01-01 01:02:03'",0,0,0,0},
			{"null",1,"'2000-01-01 01:02:03'",1,0,0,0},
			{"null",0,"current_timestamp",0,1,1,0},
			{"null",0,"current_timestamp",1,1,1,0},
			{"null",1,"current_timestamp",0,0,0,0},
			{"null",1,"current_timestamp",1,0,0,0},
			{"null",0,"null",0,1,1,1}, // Caused by: PESQLException: On statement: CREATE  TABLE ts16 (id INT , data VARCHAR (5) , ts TIMESTAMP NOT NULL DEFAULT NULL , PRIMARY KEY ( id ) ) ENGINE = InnoDB
			{"null",0,"null",1,1,1,1}, // Caused by: PESQLException: On statement: CREATE  TABLE ts16 (id INT , data VARCHAR (5) , ts TIMESTAMP NOT NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP , PRIMARY KEY ( id ) ) ENGINE = InnoDB
			{"null",1,"null",0,0,0,0},
			{"null",1,"null",1,0,0,0},
			{"'2000-01-01 01:02:03'",0,"0",0,0,0,0},
			{"'2000-01-01 01:02:03'",0,"0",1,0,0,0},
			{"'2000-01-01 01:02:03'",1,"0",0,0,0,0},
			{"'2000-01-01 01:02:03'",1,"0",1,0,0,0},
			{"'2000-01-01 01:02:03'",0,"",0,0,0,0},
			{"'2000-01-01 01:02:03'",0,"",1,0,0,0},
			{"'2000-01-01 01:02:03'",1,"",0,0,0,0},
			{"'2000-01-01 01:02:03'",1,"",1,0,0,0},
			{"'2000-01-01 01:02:03'",0,"'2000-01-01 01:02:03'",0,0,0,0},
			{"'2000-01-01 01:02:03'",0,"'2000-01-01 01:02:03'",1,0,0,0},
			{"'2000-01-01 01:02:03'",1,"'2000-01-01 01:02:03'",0,0,0,0},
			{"'2000-01-01 01:02:03'",1,"'2000-01-01 01:02:03'",1,0,0,0},
			{"'2000-01-01 01:02:03'",0,"current_timestamp",0,0,0,0},
			{"'2000-01-01 01:02:03'",0,"current_timestamp",1,0,0,0},
			{"'2000-01-01 01:02:03'",1,"current_timestamp",0,0,0,0},
			{"'2000-01-01 01:02:03'",1,"current_timestamp",1,0,0,0},
			{"'2000-01-01 01:02:03'",0,"null",0,0,0,1}, // Caused by: PESQLException: On statement: CREATE  TABLE ts34 (id INT , data VARCHAR (5) , ts TIMESTAMP NOT NULL DEFAULT NULL , PRIMARY KEY ( id ) ) ENGINE = InnoDB
			{"'2000-01-01 01:02:03'",0,"null",1,0,0,1}, // Caused by: PESQLException: On statement: CREATE  TABLE ts34 (id INT , data VARCHAR (5) , ts TIMESTAMP NOT NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP , PRIMARY KEY ( id ) ) ENGINE = InnoDB
			{"'2000-01-01 01:02:03'",1,"null",0,0,0,0},
			{"'2000-01-01 01:02:03'",1,"null",1,0,0,0},
			{"current_timestamp",0,"0",0,1,1,0},
			{"current_timestamp",0,"0",1,1,1,0},
			{"current_timestamp",1,"0",0,1,1,0},
			{"current_timestamp",1,"0",1,1,1,0},
			{"current_timestamp",0,"",0,1,1,0},
			{"current_timestamp",0,"",1,1,1,0},
			{"current_timestamp",1,"",0,1,1,0},
			{"current_timestamp",1,"",1,1,1,0},
			{"current_timestamp",0,"'2000-01-01 01:02:03'",0,1,1,0},
			{"current_timestamp",0,"'2000-01-01 01:02:03'",1,1,1,0},
			{"current_timestamp",1,"'2000-01-01 01:02:03'",0,1,1,0},
			{"current_timestamp",1,"'2000-01-01 01:02:03'",1,1,1,0},
			{"current_timestamp",0,"current_timestamp",0,1,1,0},
			{"current_timestamp",0,"current_timestamp",1,1,1,0},
			{"current_timestamp",1,"current_timestamp",0,1,1,0},
			{"current_timestamp",1,"current_timestamp",1,1,1,0},
			{"current_timestamp",0,"null",0,1,1,1}, // Caused by: PESQLException: On statement: CREATE  TABLE ts52 (id INT , data VARCHAR (5) , ts TIMESTAMP NOT NULL DEFAULT NULL , PRIMARY KEY ( id ) ) ENGINE = InnoDB
			{"current_timestamp",0,"null",1,1,1,1}, // Caused by: PESQLException: On statement: CREATE  TABLE ts52 (id INT , data VARCHAR (5) , ts TIMESTAMP NOT NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP , PRIMARY KEY ( id ) ) ENGINE = InnoDB
			{"current_timestamp",1,"null",0,1,1,0},
			{"current_timestamp",1,"null",1,1,1,0},
			{"",0,"0",0,0,0,1}, // Caused by: java.sql.SQLException: Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp
			{"",0,"0",1,0,1,1}, // Caused by: java.sql.SQLException: Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp
			{"",1,"0",0,0,0,1}, // Caused by: java.sql.SQLException: Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp
			{"",1,"0",1,0,1,1}, // Caused by: java.sql.SQLException: Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp
			// changed default value to current_timestamp here because we now normalize missing timestamp to the 0 value
			// {"",0,"current_timestamp",0,1,0,1}, 
			{"",0,"current_timestamp",0,1,0,1}, // Caused by: java.sql.SQLException: Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp
			{"",1,"",0,0,0,0},
			{"",1,"",1,0,1,0},
			{"",0,"'2000-01-01 01:02:03'",0,0,0,0},
			{"",0,"'2000-01-01 01:02:03'",1,0,1,0},
			{"",1,"'2000-01-01 01:02:03'",0,0,0,0},
			{"",1,"'2000-01-01 01:02:03'",1,0,1,0},
			{"",0,"current_timestamp",0,1,0,0},
			{"",0,"current_timestamp",1,1,1,0},
			{"",1,"current_timestamp",0,1,0,0},
			{"",1,"current_timestamp",1,1,1,0},
			{"",0,"null",0,0,0,1}, // Caused by: PESQLException: On statement: CREATE  TABLE ts8 (id INT , data VARCHAR (5) , ts TIMESTAMP NOT NULL DEFAULT NULL , PRIMARY KEY ( id ) ) ENGINE = InnoDB
			{"",0,"null",1,0,1,1}, // Caused by: PESQLException: On statement: CREATE  TABLE ts8 (id INT , data VARCHAR (5) , ts TIMESTAMP NOT NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP , PRIMARY KEY ( id ) ) ENGINE = InnoDB
			{"",1,"null",0,0,0,0},
			{"",1,"null",1,0,1,0}
			};

	public static String buildCreateTableSQL(String tableName, boolean nullable,
			String defaultValue, boolean onUpdate) {
		StringBuilder sql = new StringBuilder();
		sql.append("create table ");
		sql.append(tableName);
		sql.append(" (id int, data varchar(5), ts timestamp");
		sql.append(nullable ? " null " : " not null ");
		sql.append(StringUtils.isEmpty(defaultValue) ? "" : " default "
				+ defaultValue + " ");
		sql.append(onUpdate ? " on update current_timestamp " : "");
		sql.append(", primary key (id) ) BROADCAST DISTRIBUTE");

		return sql.toString();
	}

	public static String buildInsertTestSQL(String tableName, String value,
			int idValue, String dataValue) {
		StringBuilder sql = new StringBuilder();

		boolean colIsSpecified = !StringUtils.isEmpty(value);

		sql.append("insert into ");
		sql.append(tableName);

		if (colIsSpecified) {
			sql.append(" values (");
			sql.append(idValue);
			sql.append(", '");
			sql.append(dataValue);
			sql.append("',");
			sql.append(value);
		} else {
			sql.append(" (id, data) values (");
			sql.append(idValue);
			sql.append(", '");
			sql.append(dataValue);
			sql.append("'");
		}
		sql.append(")");

		return sql.toString();
	}

	public static String buildUpdateTestSQL(String tableName, String value,
			int idValue, String dataValue) {
		StringBuilder sql = new StringBuilder();

		boolean colIsSpecified = !StringUtils.isEmpty(value);

		sql.append("update ");
		sql.append(tableName);
		sql.append(" set data='");
		sql.append(dataValue);
		sql.append("'");

		if (colIsSpecified) {
			sql.append(", ts=");
			sql.append(value);
		}
		sql.append(" where id=");
		sql.append(idValue);

		return sql.toString();
	}

	public static Object[][] getTestValues() {
		return testValues;
	}
}
