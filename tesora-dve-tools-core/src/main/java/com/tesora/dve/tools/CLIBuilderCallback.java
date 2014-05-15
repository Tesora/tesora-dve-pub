// OS_STATUS: public
package com.tesora.dve.tools;

public interface CLIBuilderCallback {

	public abstract void printlnIndent(String message);

	public abstract void printlnDots(String message);

	public abstract void println(String message);

	public abstract void println();
}