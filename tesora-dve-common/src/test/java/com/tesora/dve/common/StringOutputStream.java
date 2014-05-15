// OS_STATUS: public
package com.tesora.dve.common;

import java.io.IOException;
import java.io.OutputStream;

public class StringOutputStream extends OutputStream {

	private final StringBuilder buffer = new StringBuilder();
	  
	@Override
	public void write(int b) throws IOException {
	  buffer.append((char) b);
	}

	@Override
	public String toString() {
	  return buffer.toString();
	}
}