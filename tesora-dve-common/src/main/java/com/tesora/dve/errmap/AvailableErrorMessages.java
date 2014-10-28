package com.tesora.dve.errmap;

import org.apache.commons.lang.ArrayUtils;

public final class AvailableErrorMessages {

	private final static ErrorCodeFormatter[] messages = buildAllMessageArray();

	private static ErrorCodeFormatter[] buildAllMessageArray() {
		return (ErrorCodeFormatter[]) ArrayUtils.addAll(getMySQLNative(), getDVEInternal());
	}

	public static ErrorCodeFormatter[] getAll() {
		return messages;
	}

	public static ErrorCodeFormatter[] getMySQLNative() {
		return MySQLErrors.messages;
	}

	public static ErrorCodeFormatter[] getDVEInternal() {
		return InternalErrors.messages;
	}

}
