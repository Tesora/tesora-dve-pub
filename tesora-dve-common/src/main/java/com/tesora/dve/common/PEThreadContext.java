package com.tesora.dve.common;

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

import org.apache.log4j.Logger;

public class PEThreadContext {

	private static final Logger LOGGER = Logger.getLogger(PEContext.class);

	private static boolean enabled = false;

	private static final ThreadLocal<PEContext> THREAD_CONTEXT = new ThreadLocal<PEContext>() {

		@Override
		protected PEContext initialValue() {
			return enabled ? new PEContext() : PEContext.NO_OP_CONTEXT;
		}

	};

    public static Ref pushFrame(Class<?> clazz) {
        context().push(clazz);
        return REF;
    }

	public static Ref pushFrame(String name) {
		context().push(name);
		return REF;
	}

	public static void popFrame() {
		context().pop();
	}

	public static String get(String key) {
		return context().get(key);
	}

	public static Ref put(String key, Object value) {
		context().put(key, value);
		return REF;
	}

	public static void remove(String key) {
		context().remove(key);
	}

	public static String asString() {
		return context().toString();
	}

	public static void logDebug() {
		if (enabled && LOGGER.isDebugEnabled())
			LOGGER.debug(asString());
	}

	public static PEContext copy() {
		return context().copy();
	}

	private static PEContext context() {
		return THREAD_CONTEXT.get();
	}

	public static void inherit(PEContext context) {
		THREAD_CONTEXT.set(context);
        if (context != PEContext.NO_OP_CONTEXT){
            if (!Thread.currentThread().getName().equals(context.getSourceThread()))
                pushFrame("ThreadSwitch")
                    .put("from", context.getSourceThread())
                    .put("to", Thread.currentThread().getName());
        }
	}

	public static void clear() {
		THREAD_CONTEXT.remove();
	}

	/*
	 * This is just syntactic sugar to allow chaining push/put calls.
	 *
	 * For example: PEThreadContext.push("foo").put("a", "1")
	 */
	private static final Ref REF = new Ref();

	public static final class Ref {

		public Ref put(String key, Object value) {
			return PEThreadContext.put(key, value);
		}

		public Ref pushFrame(String name) {
			return PEThreadContext.pushFrame(name);
		}

		public void logDebug() {
			PEThreadContext.logDebug();
		}

	}

	public static void setEnabled(boolean enable) {
		enable = enable || LOGGER.isDebugEnabled();
		PEThreadContext.enabled = enable;
		THREAD_CONTEXT.set(enable ? new PEContext() : PEContext.NO_OP_CONTEXT);
	}

}
