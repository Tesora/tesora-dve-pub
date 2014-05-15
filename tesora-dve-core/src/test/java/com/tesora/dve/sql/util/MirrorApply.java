// OS_STATUS: public
package com.tesora.dve.sql.util;

import com.tesora.dve.sql.parser.ParserInvoker.LineInfo;
import com.tesora.dve.sql.util.ConnectionResource.ExceptionClassification;

public class MirrorApply extends MirrorTest {

	protected String stmt;
	protected LineInfo info;
	protected boolean ignoreResponse;
	protected MirrorExceptionHandler exceptionHandler;
	protected TestName currentTest;
	
	public MirrorApply(LineInfo info, String stmt, MirrorExceptionHandler exHandler, TestName ctest, boolean justExecute) {
		super();
		this.stmt = stmt;
		this.info = info;
		this.exceptionHandler = exHandler;
		this.currentTest = ctest;
		this.ignoreResponse = justExecute;
	}
	
	protected ExceptionClassification classifyException(String message, TestResource res, Exception in) throws Throwable {
		Throwable any = in;
		if (any != null) {
			Throwable c = any;
			while(c != null) {
				ExceptionClassification ec = res.getConnection().classifyException(c);
				if (ec != null) return ec;
				c = c.getCause();
			}
			if (exceptionHandler != null)
				exceptionHandler.onException(res, currentTest, new Exception(message + ": expected exception", any));
			else
				// some other kind of exception - likely a failure
				throw new Exception(message + ": expected exception",any);
			// if we get here, it's an unknown exception
			return ExceptionClassification.UNKNOWN;
		}
		return null;
	}
	
	protected boolean checkResponse(String message, TestResource check, TestResource sys, 
			ResourceResponse checkResp, Exception checkEx, ResourceResponse sysResp, Exception sysEx) throws Throwable {
		if (ignoreResponse)
			return true;
		ExceptionClassification checkClassified = null;
		ExceptionClassification sysClassified = null;
		if (check != null)
			checkClassified = classifyException(message, check, checkEx);
		if (sys != null)
			sysClassified = classifyException(message, sys, sysEx);
		if (check != null && sys != null) {
			if (checkClassified != null && (checkClassified == sysClassified))
				return false;
			if (checkResp == null && sysResp == null)
				return true;
			else if ((checkResp == null && sysResp != null) || (checkResp != null && sysResp == null))
				return false;
			checkResp.assertEqualResponse(message, sysResp);
		} else if (check != null) {
			return checkClassified != null;
		} else if (sys != null) {
			return sysClassified != null;
		}
		return true;
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
			checkResponse = check.getConnection().execute(info, stmt); 
		} catch (Exception e) {
			checkException = e;
		}
		if (sys != null) try {
			sysResponse = sys.getConnection().execute(info, stmt);
		} catch (Exception e) {
			sysException = e;
		}
		checkResponse(message, check, sys, checkResponse, checkException, sysResponse, sysException);
	}

}