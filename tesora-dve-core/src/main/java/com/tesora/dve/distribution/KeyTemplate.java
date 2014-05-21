// OS_STATUS: public
package com.tesora.dve.distribution;

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.UnaryFunction;

public class KeyTemplate extends ArrayList<UserColumn>{

	private static final long serialVersionUID = 1L;
	
	// see the patent for definition of comparable
	public boolean comparableDistributionVector(KeyTemplate other) {
		if (size() != other.size())
			return false;
		Iterator<UserColumn> liter = iterator();
		Iterator<UserColumn> riter = other.iterator();
		while(liter.hasNext() && riter.hasNext()) {
			UserColumn l = liter.next();
			UserColumn r = riter.next();
			if (!l.comparableType(r))
				return false;
		}
		return true;
	}

	public String getKeyTemplateString() {
		return Functional.apply(this, new UnaryFunction<String, UserColumn>() {
			@Override
			public String evaluate(UserColumn object) {
				return object.getNativeTypeName();
			}
		}).toString();
	}

	public List<String> asColumnList() {
		return Functional.apply(this, new UnaryFunction<String, UserColumn>() {
			@Override
			public String evaluate(UserColumn object) {
				return object.getName();
			}
		});
	}
}
