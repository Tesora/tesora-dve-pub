// OS_STATUS: public
package com.tesora.dve.common;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicBoolean;

public class PEContext {

	public static final String LOG_PREFIX = "--- " + PEContext.class.getSimpleName() + " ---";

	private final Stack<Frame> frames = new Stack<Frame>();

	// thread on which this context was created or copied
	final String sourceThread = Thread.currentThread().getName();

	PEContext() {
	}

	PEContext(Stack<Frame> frames) {
		for (Frame frame : frames) {
			this.frames.push(frame.copy());
		}
	}

	public PEContext copy() {
		return new PEContext(frames);
	}

	void push(String name) {
		frames.push(new Frame(name));
	}

	void pop() {
		frames.pop();
	}

	void put(String key, Object value) {
		put(key, value != null ? value.toString() : "null");
	}

	void put(String key, String value) {
		if (frames.isEmpty()) {
			push("");
		}
		frames.peek().put(key, value);
	}

	void remove(String key) {
		frames.peek().remove(key);
	}

	/** Returns most-local frame value for the current key. */
	String get(String key) {
		String value = null;
		for (int i = frames.size(); --i >= 0;) {
			value = frames.get(i).get(key);
			if (value != null) {
				break;
			}
		}
		return value;
	}

	int depth() {
		return frames.size();
	}

	public boolean isEmpty() {
		return frames.isEmpty() ||
				(frames.size() == 1 && frames.peek().isEmpty());
	}

	public String getSourceThread() {
		return sourceThread;
	}

	public String toFormattedString() {
		StringBuffer sb = new StringBuffer(LOG_PREFIX);
		for (Frame frame : frames) {
			sb.append("\n\t").append(frame.getName()).append(": ");
			Iterator<Entry<String, String>> iter = frame.map.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, String> entry = iter.next();
				sb.append(entry.getKey());
				sb.append(" = ");
				sb.append(entry.getValue());
				if (iter.hasNext()) {
					sb.append(";  ");
				}
			}
		}
		sb.append("\n --- End ").append(getClass().getSimpleName()).append(" ---");
		return sb.toString();
	}

	public String toString() {
		return getClass().getSimpleName() + frames.toString();
	}

	public boolean isEnabled() {
		return true;
	}

	static class Frame {

		private final String name;
		private final AtomicBoolean shared = new AtomicBoolean(false);

		private Map<String, String> map = new LinkedHashMap<String, String>();

		Frame(String name) {
			this.name = name;
		}

		private Frame(String name, Map<String, String> map) {
			this(name);
			this.shared.set(true);
			this.map = map;
		}

		String getName() {
			return name;
		}

		String get(String key) {
			return map.get(key);
		}

		void put(String key, String value) {
			copyOnWrite();
			map.put(key, value);
		}

		void remove(String key) {
			copyOnWrite();
			map.remove(key);
		}

		boolean isEmpty() {
			return map.isEmpty();
		}

		Frame copy() {
			shared.set(true);
			return new Frame(name, map);
		}

		public String toString() {
			return name + (map == null ? "[]" : map.toString());
		}

		private synchronized void copyOnWrite() {
			if (shared.compareAndSet(true, false)) {
				map = new LinkedHashMap<String, String>(map);
			}
		}
	}

	// used when debug context is disabled
	static final PEContext NO_OP_CONTEXT = new PEContext() {

		@Override
		public PEContext copy() { return this; }

		@Override
		void push(String name) { }

		@Override
		void pop() { }

		@Override
		void put(String key, Object value) { }

		@Override
		void put(String key, String value) { }

		@Override
		void remove(String key) { }

		@Override
		String get(String key) { return null; }

		@Override
		int depth() { return 0; }

		@Override
		public boolean isEmpty() { return true; }

		@Override
		public String getSourceThread() { return null; }

		@Override
		public String toFormattedString() {
			return this.toString();
		}

		public boolean isEnabled() {
			return false;
		}

	};

}
