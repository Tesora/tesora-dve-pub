// OS_STATUS: public
package com.tesora.dve.sql;

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

public class TransformException extends ParserException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected TransformException() {
		super();
	}
	
	public TransformException(Pass p) {
		super(p);
	}

	public TransformException(Pass p, String message) {
		super(p, message);
	}

	public TransformException(Pass p, Throwable cause) {
		super(p, cause);
	}

	public TransformException(Pass p, String message, Throwable cause) {
		super(p, message, cause);
	}

}
