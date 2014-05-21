// OS_STATUS: public
package com.tesora.dve.variable;

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

import java.lang.reflect.Method;

import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PENotFoundException;

public class BeanVariableHandler {

	public static String callGetValue(Object obj, String name) throws PENotFoundException {
		try {
			Method get = obj.getClass().getMethod("get" + name, new Class[] {});
			if (get.getReturnType().equals(long.class))
				return Long.toString((Long) get.invoke(obj, new Object[] {}));

			if (get.getReturnType().equals(int.class))
				return Integer.toString((Integer) get.invoke(obj, new Object[] {}));

			if (get.getReturnType().equals(String.class))
				return (String) get.invoke(obj, new Object[] {});

			throw new PECodingException("Method " + obj.getClass().getSimpleName() + "." + get.getName()
					+ "() does not return a supported type");
		} catch (Exception e) {
			throw new PENotFoundException("Unable to get value for variable " + name, e);
		}
	}
	
	public static void callResetValue(Object obj, String name) throws PENotFoundException {
		try {
			Method reset = obj.getClass().getMethod("reset" + name, new Class[] {});
			
			reset.invoke(obj, new Object[] {});
		} catch (NoSuchMethodException e) {
			// If the reset method doesn't exist then move on
			// System.out.println("no reset method found for '" + name + "' on class '" + obj.getClass().getSimpleName() + "'");
		} catch (Exception e) {
			throw new PENotFoundException("Unable to reset value for variable " + name, e);
		}
	}
}
