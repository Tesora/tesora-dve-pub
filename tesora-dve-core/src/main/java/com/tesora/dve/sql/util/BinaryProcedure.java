// OS_STATUS: public
package com.tesora.dve.sql.util;

public abstract class BinaryProcedure<A, B> {

	public abstract void execute(A aobj, B bobj);
	
}
