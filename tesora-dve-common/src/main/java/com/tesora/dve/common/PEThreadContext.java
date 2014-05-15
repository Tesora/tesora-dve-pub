// OS_STATUS: public
package com.tesora.dve.common;

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
		if (!Thread.currentThread().getName().equals(context.getSourceThread()))
			pushFrame("ThreadSwitch")
				.put("from", context.getSourceThread())
				.put("to", Thread.currentThread().getName());
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
