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

import java.sql.Timestamp;

public class TimestampColumnChecker extends ColumnChecker {

	@Override
	protected String equalObjects(Object expected, Object actual) {
		String results = super.equalObjects(expected, actual);
		if (results == null) return null;
		Timestamp lt = (Timestamp) expected;
		Timestamp rt = (Timestamp) actual;
		return results + " (time " + lt.getTime() + "/" + rt.getTime() + ")";
	}
}
