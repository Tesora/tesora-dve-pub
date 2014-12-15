package com.tesora.dve.sql.logfile;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.Runner;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.ParentRunner;
import org.junit.runners.model.InitializationError;

import com.tesora.dve.sql.util.TestName;

public class LogFileSuiteRunner extends ParentRunner<Runner> {

	private final List<Runner> tests;
	
	public LogFileSuiteRunner(Class<?> testClass)
			throws InitializationError {
		super(testClass);
		tests = new ArrayList<Runner>();
		// we make two passes across the classes - the first to figure out if anything will run
		// and the second to actually construct the runners.  we need to do this so that we don't
		// run startup/teardown if nothing is configured, and we use the LogFileTestRunner to run
		// the before/after class....well, if no pe tests are configured
		int nconfigured = 0;
		LinkedHashMap<Class<?>, List<TestName>> configured = new LinkedHashMap<Class<?>,List<TestName>>();
		TreeMap<String, Class<?>> sortedClasses = new TreeMap<String, Class<?>>();
		for(Class<?> k : testClass.getClasses())
			sortedClasses.put(k.getSimpleName(), k);
		for(Class<?> k : sortedClasses.values()) {
			List<TestName> runnable = LogFileTestRunner.findRunnable(k);
			if (runnable.isEmpty()) continue;
			for(TestName tn : runnable) {
				if (!TestName.NATIVE.equals(tn))
					nconfigured++;
			}
			configured.put(k, runnable);
		}
//		if (nconfigured == 0)
//			return;
		boolean first = true;
		for(Iterator<Map.Entry<Class<?>,List<TestName>>> iter = configured.entrySet().iterator(); iter.hasNext();) {
			Map.Entry<Class<?>,List<TestName>> me = iter.next();
			tests.add(new LogFileTestRunner(me.getKey(),me.getValue(),(nconfigured == 0 ? false :first),(nconfigured == 0 ? false : !iter.hasNext())));
			first = false;
		}		
	}
	
	@Override
	protected List<Runner> getChildren() {
		return tests;
	}

	@Override
	protected Description describeChild(Runner child) {
		return child.getDescription();
	}

	@Override
	protected void runChild(Runner child, RunNotifier notifier) {
		EchoingListener el = new EchoingListener((LogFileTestRunner)child);
		try {
			notifier.addListener(el);
			child.run(notifier);
		} finally {
			notifier.removeListener(el);
		}
	}

	private static double toSeconds(long diff) {
		return diff/1000000000.0;
	}
	
	private static class EchoingListener extends RunListener {

		private long lastTime = 0;
		private LogFileTestRunner runner;
		
		public EchoingListener(LogFileTestRunner p) {
			runner = p;
		}
		
		@Override
		public void testRunStarted(Description description) {
			System.out.println("Starting: " + description.getClassName());
		}
		
		@Override
		public void testRunFinished(Result result) {
			System.out.println(String.format("Tests run: %d, Failures: %d, Time elapsed: %f sec",
					result.getRunCount(),result.getFailureCount(), toSeconds(result.getRunTime())));
		}
		
		@Override
		public void testStarted(Description description) {
			System.out.println("   Starting test: " + description.getMethodName() + ", " + runner.getStmtCount() + " stmts");
			lastTime = System.nanoTime();
		}

		@Override
		public void testFinished(Description description) throws Exception {
			System.out.println(String.format("   Finished test: %s, Time elapsed: %f sec",
					description.getMethodName(), toSeconds(System.nanoTime() - lastTime)));
		}

		@Override
		public void testFailure(Failure failure) throws Exception {
		}

	}
	
}
