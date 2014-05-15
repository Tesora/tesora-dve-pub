// OS_STATUS:  public
package com.tesora.dve.sql.util;

import com.tesora.dve.sql.parser.ParserInvoker.LineInfo;

public class MirrorFunction extends MirrorApply {

	private boolean unorderedCompare;
	
	public MirrorFunction(LineInfo info, String stmt, MirrorExceptionHandler parent, TestName ctest, boolean unordered, boolean ignoreResults) {
		super(info,stmt, parent, ctest, ignoreResults);
		unorderedCompare = unordered;
	}

	@Override
	public void execute(TestResource check, TestResource sys)
			throws Throwable {
		String message = "Failure at line " + info.getLineNumber() + "; stmt='" + stmt + "':";
		ResourceResponse checkResponse = null;
		ResourceResponse sysResponse = null;
		Exception checkException = null;
		Exception sysException = null;
		if (check != null) try {
			checkResponse = check.getConnection().fetch(info, stmt);
		} catch (Exception e) {
			checkException = e;
		}
		if (sys != null) try {
			sysResponse = sys.getConnection().fetch(info, stmt);
		} catch (Exception e) {
			sysException = e;
		}
		if (!checkResponse(message, check, sys, checkResponse, checkException, sysResponse, sysException))
			return;
		// from now on, only if we have both
		if (checkResponse == null || sysResponse == null || ignoreResponse)
			return;
		checkResponse.assertEqualResults(message, unorderedCompare, false, sysResponse);
	}		
}