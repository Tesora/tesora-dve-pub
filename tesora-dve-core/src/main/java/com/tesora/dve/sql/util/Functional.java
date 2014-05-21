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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Functional {

	public static <O, I> List<O> apply(Collection<I> collection, UnaryFunction<O, ? super I> function) {
		ArrayList<O> out = new ArrayList<O>();
		apply(collection, out, function);
		return out;
	}

	public static <O, I> void apply(Collection<I> collection, List<O> out, UnaryFunction<O, ? super I> function) {
		for(I el : collection) 
			out.add(function.evaluate(el));		
	}
	
	public static <I> void apply(Collection<I> collection, UnaryProcedure<? super I> proc) {
		for(I el : collection)
			proc.execute(el);
	}

	public static <I> List<I> select(Collection<I> in, UnaryPredicate<I> pred) {
		if (in == null)
			return null;
		ArrayList<I> out = new ArrayList<I>();
		for(I el : in) {
			if (pred.test(el))
				out.add(el);
		}
		return out;
	}
	
	public static <I> boolean any(Collection<I> collection, UnaryPredicate<I> pred) {
		for(I el : collection) {
			if (pred.test(el))
				return true;
		}
		return false;
	}
	
	public static <I> boolean all(Collection<I> collection, UnaryPredicate<I> pred) {
		for(I el : collection) {
			if (!pred.test(el))
				return false;
		}
		return true;
	}

	public static <I> void join(Collection<I> inputs, StringBuilder buffer, String elementSeparator, BinaryProcedure<I, StringBuilder> proc) {
		for(Iterator<I> iter = inputs.iterator(); iter.hasNext();) {
			I obj = iter.next();
			proc.execute(obj, buffer);
			if (iter.hasNext())
				buffer.append(elementSeparator);
		}
	}
		
	public static void join(Collection<String> strings, StringBuilder buffer, String separator) {
		for(Iterator<String> iter = strings.iterator(); iter.hasNext();) {
			buffer.append(iter.next());
			if (iter.hasNext())
				buffer.append(separator);
		}
	}

	public static String join(Collection<String> strings, String separator) {
		StringBuilder buf = new StringBuilder();
		join(strings, buf, separator);
		return buf.toString();
	}
	
	public static <I> String join(Collection<I> inputs, String separator, final UnaryFunction<String, I> getString) {
		StringBuilder buffer = new StringBuilder();
		join(inputs, buffer, separator, new BinaryProcedure<I, StringBuilder>() {

			@Override
			public void execute(I aobj, StringBuilder bobj) {
				bobj.append(getString.evaluate(aobj));
			}
			
		});
		return buffer.toString();
	}
	
	public static <I> String joinToString(Collection<I> inputs, String separator) {
		return join(inputs, separator, new UnaryFunction<String, I>() {

			@Override
			public String evaluate(I object) {
				return object.toString();
			}
			
		});
	}
	
	public static <K, V> Map<K, V> buildMap(Collection<V> in, UnaryFunction<K, V> byValue)  {
		LinkedHashMap<K, V> out = new LinkedHashMap<K, V>();
		for(V v : in) {
			K k = byValue.evaluate(v);
			out.put(k, v);
		}
		return out;
	}

	public static <T> List<T> toList(Collection<T> in) {
		return new ArrayList<T>(in);
	}
	
	public static Map<String, Object> buildMap(String[] keys, Object[] values) {
		LinkedHashMap<String,Object> out = new LinkedHashMap<String, Object>();
		if (keys == null && values == null) return out;
		if (keys == null)
			throw new IllegalArgumentException("null keys specified to buildMap");
		if (values == null)
			throw new IllegalArgumentException("null values specified to buildMap");
		if (keys.length != values.length)
			throw new IllegalArgumentException("Differing sizes of keys and values.  Found " + keys.length + " keys and " + values.length + " values");
		for(int i = 0; i< keys.length; i++)
			out.put(keys[i], values[i]);
		return out;
	}
}
