package com.tesora.dve.exceptions;

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


public class PESQLException extends PEException {

	protected PESQLException() {
		super();
	}
	
	public PESQLException(String m, Exception e) {
		super(m, e);
	}

	public PESQLException(String m, Throwable e) {
		super(m, e);
	}

	public PESQLException(Throwable throwable) {
		super(throwable);
	}

	public PESQLException(String m) {
		super(m);
	}

	private static final long serialVersionUID = 1L;

    public static PESQLException coerce(Throwable t){
        if (t instanceof PESQLException)
            return (PESQLException)t;
        else {
            PESQLException pesql = new PESQLException(t);
            pesql.fillInStackTrace();
            return pesql;
        }
    }
}
