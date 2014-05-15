// OS_STATUS: public
package com.tesora.dve.sql.util;

public interface MirrorExceptionHandler {

	void onException(TestResource res, TestName currentTest,Exception e) throws Throwable;

}
