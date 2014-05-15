// OS_STATUS: public
package com.tesora.dve.sql.schema.validate;

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
