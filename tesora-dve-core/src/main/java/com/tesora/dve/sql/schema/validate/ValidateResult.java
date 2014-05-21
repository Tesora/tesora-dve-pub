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
