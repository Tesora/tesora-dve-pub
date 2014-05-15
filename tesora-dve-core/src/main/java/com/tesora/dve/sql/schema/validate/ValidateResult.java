// OS_STATUS: public
package com.tesora.dve.sql.schema.validate;

import java.util.List;

import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;

public abstract class ValidateResult {

	public abstract boolean isError();

	public abstract String getMessage(SchemaContext sc);
	
	public abstract Persistable<?,?> getSubject();
	
	public static String buildMessage(final SchemaContext sc, List<ValidateResult> results, boolean all) {
		if (results.isEmpty()) return null;
		if (!all)
			return results.get(0).getMessage(sc);
		else
			return Functional.join(results, "; ", new UnaryFunction<String, ValidateResult>() {

				@Override
				public String evaluate(ValidateResult object) {
					return object.getMessage(sc);
				}
				
			});
	}
	
}
