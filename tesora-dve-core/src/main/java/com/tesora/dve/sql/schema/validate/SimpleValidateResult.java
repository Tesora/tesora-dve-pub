package com.tesora.dve.sql.schema.validate;

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

import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;

public class SimpleValidateResult extends ValidateResult {

	// true if it is an error, false if a warning.
	private boolean error;
	// and the message
	private String message;
	// and the subject - we use this to figure out what to do about bad fks after the fact
	private Persistable<?,?> subject;
	
	public SimpleValidateResult(Persistable<?,?> subj, boolean isbad, String mess) {
		error = isbad;
		message = mess;
		subject = subj;
	}
	
	public boolean isError() {
		return error;
	}

	public String getMessage(SchemaContext sc) {
		return message;
	}
	
	public Persistable<?,?> getSubject() {
		return subject;
	}
	

}
