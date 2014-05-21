// OS_STATUS: public
package com.tesora.dve.distribution.compare;

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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.uuid.impl.UUIDUtil;
import com.google.common.primitives.SignedBytes;

public class ComparatorCache {
	
	public static final String DEFAULT_UUID_COMPARATOR = "com.tesora.dve.comparator.UUIDComparator";
	
	@SuppressWarnings("rawtypes")
	private static Map<String, Comparator> comparators = new HashMap<String, Comparator>();
	
	@SuppressWarnings("rawtypes")
	public static final Comparator<Comparable> DEFAULT_COMPARATOR = new Comparator<Comparable>() {
		@SuppressWarnings("unchecked")
		@Override
		public int compare(Comparable arg0, Comparable arg1) {
			return arg0.compareTo(arg1);
		}
	};	
	
	public static final Comparator<Object> BINARY_COMPARATOR = new Comparator<Object>() {
		@Override
		public int compare(Object o1, Object o2) {
			return SignedBytes.lexicographicalComparator().compare((byte[])o1, (byte[])o2);
		}
	};
	
	public static final Comparator<Object> UUID_COMPARATOR = new Comparator<Object>() {
		@Override
		public int compare(Object o1, Object o2) {
			String s1 = (o1 instanceof String) ? (String)o1 : new String((byte[])o1);
			String s2 = (o2 instanceof String) ? (String)o2 : new String((byte[])o2);
			return UUIDUtil.uuid(s1).compareTo(UUIDUtil.uuid(s2));
		}
	};

	protected ComparatorCache() {
	}

	@SuppressWarnings("rawtypes")
	public static Comparator get(String comparatorClassName) {
		if (comparators.containsKey(comparatorClassName)) {
			return comparators.get(comparatorClassName);
		} else {
			return DEFAULT_COMPARATOR;
		}
	}

	public static void add(String comparatorClassName) {
		// For now hard code our implementation of UUID to this comparator class name
		if (!comparators.containsKey(comparatorClassName)) {
			if (StringUtils.equalsIgnoreCase(comparatorClassName, DEFAULT_UUID_COMPARATOR)) {
				comparators.put(comparatorClassName, UUID_COMPARATOR);
			}
		}
		
		// try to dynamically load the jar file...
//		try {
//			if (!managers.containsKey(comparator)) {
//				Class<?> theClass = null;
//				try {
//					theClass = Class.forName(comparator);
//				} catch (Exception e) {
//					// new jar? look for new ones
//					Collection<File> files = FileUtils.listFiles(new File("D:\\Eclipse\\workspace\\general\\UUIDComparator\\target"),
//			                new String[] {"jar"},
//			                false);
//					List<URL> urls = new ArrayList<URL>();
//					for(File file : files) {
//				        URL u = file.toURI().toURL();
//				        urls.add(u);
//					}
////			        URLClassLoader sysloader = (URLClassLoader) ClassLoader.getSystemClassLoader();
////			        theClass = sysloader.loadClass(comparator);
//					URLClassLoader child = new URLClassLoader(urls.toArray(new URL[urls.size()]), ComparisonManager.class.getClassLoader());
//					Class classToLoad = Class.forName (comparator, true, child);
//					Comparator c = (Comparator)theClass.newInstance();
//					managers.put(comparator, c);
//				}
//				
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
	}
}
