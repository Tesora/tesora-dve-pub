// OS_STATUS: public
package com.tesora.dve.sql.node;

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

// migration exceptions are used to indicate code paths that
// haven't been migrated to whatever the new idiom is
public class MigrationException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public MigrationException() {
		// TODO Auto-generated constructor stub
	}

	public MigrationException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	public MigrationException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	public MigrationException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

}
