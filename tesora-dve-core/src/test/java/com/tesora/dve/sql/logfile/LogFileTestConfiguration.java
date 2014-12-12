package com.tesora.dve.sql.logfile;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.tesora.dve.sql.util.TestName;

@Target({java.lang.annotation.ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface LogFileTestConfiguration {

	// which test to run
	public abstract TestName enabled();
	// if it's failing, a reason
	// also, if this is set, the test won't be run
	public abstract String failureReason() default "none";

	// build kinds this test is enabled for
	public abstract LogFileTestBuildKind[] builds() default { LogFileTestBuildKind.NONE };
	
}
