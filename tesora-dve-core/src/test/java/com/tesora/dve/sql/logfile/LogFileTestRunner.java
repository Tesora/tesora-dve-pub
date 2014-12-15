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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.TestName;
import com.tesora.dve.sql.util.UnaryFunction;

public class LogFileTestRunner extends BlockJUnit4ClassRunner {

	private final Class<?> on;
	private final List<TestName> runnable;
	private final boolean runBeforeClass;
	private final boolean runAfterClass;
	private final boolean suited;
	private final long stmtCount;
	
	public LogFileTestRunner(Class<?> klass) throws InitializationError {
		this(klass,true,true);
	}
	
	public LogFileTestRunner(Class<?> klass, boolean rbc, boolean rac) throws InitializationError {
		super(klass);
		on = klass;
		runnable = findRunnable(klass);
		runBeforeClass = rbc;
		runAfterClass = rac;
		suited = false;
		stmtCount = findStmtCount(klass);
	}
	
	public LogFileTestRunner(Class<?> klass, List<TestName> toRun, boolean rbc, boolean rac) throws InitializationError {
		super(klass);
		on = klass;
		runnable = toRun;
		runBeforeClass = rbc;
		runAfterClass = rac;
		suited = true;
		stmtCount = findStmtCount(klass);
	}
	
	protected static List<TestName> findRunnable(Class<?> klass) throws InitializationError {
		// add a spike - if the static method returns false we won't run anything
		Method spike = findMethodOn(klass,"hasEnvironment");
		
		if (spike != null) try {
			Boolean result = (Boolean) spike.invoke(null, new Object[] {});
			if (Boolean.FALSE.equals(result)) {
				// we don't have the environment, don't bother running
				return Collections.emptyList();
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}
		try {
			return LogFileTest.runnableTests(klass);
		} catch (Throwable t) {
			throw new InitializationError(t);
		}
	}
	
	protected static long findStmtCount(Class<?> klass) throws InitializationError {
		try {
			LogFileTestFileConfiguration tfc = LogFileTest.findConfig(klass);
			if (tfc == null) return -1;
			return tfc.stmts();
		} catch (Throwable t) {
			throw new InitializationError(t);
		}
	}
	
	@Override
	protected void collectInitializationErrors(List<Throwable> errors) {
		// ignore - if you screwed up the config, we just won't run anything
	}
	
	@Override
	protected List<FrameworkMethod> getChildren() {
		// children is pretty easy now - just find the appropriate method based on the test name
		List<String> names = Functional.apply(runnable, new UnaryFunction<String, TestName>() {

			@Override
			public String evaluate(TestName object) {
				return object.getMethodName();
			}
			
		});
		return findMethods(on,names);
	}

	protected static List<FrameworkMethod> findMethods(Class<?> klass, Collection<String> names) {
		ArrayList<FrameworkMethod> methods = new ArrayList<FrameworkMethod>();
		for(String n : names) {
			FrameworkMethod fm = findFrameworkMethodOn(klass,n);
			if (fm == null) continue;
			methods.add(fm);
		}
		return methods;
	}
	
	protected static FrameworkMethod findFrameworkMethodOn(Class<?> klass, String name) {
		Method m = findMethodOn(klass, name);
		if (m == null) return null;
		return new FrameworkMethod(m);
	}
	
	protected static Method findMethodOn(Class<?> klass, String name) {
		Class<?>[] noargs = new Class[] {};
		try {
			return klass.getMethod(name, noargs);
		} catch (Throwable t) {
			return null;
		}
		
	}
	
	@Override
	protected Statement withBeforeClasses(Statement statement) {
		// if this is an independent run, we can add the before/after class
		// but if not, not so much
		if (runBeforeClass) {
			return super.withBeforeClasses(statement);
		}
		return statement;
	}
	
	@Override
	protected Statement withAfterClasses(Statement statement) {
		if (runAfterClass) {
			return super.withAfterClasses(statement);
		}
		return statement;
	}
	
	@Override
	@SuppressWarnings("deprecation")
	protected Statement withBefores(FrameworkMethod method, Object target,
			Statement statement) {
		if (runnable.isEmpty())
			return statement;
		return super.withBefores(method, target, statement); 
	}
	
	@Override
	@SuppressWarnings("deprecation")
	protected Statement withAfters(FrameworkMethod method, Object target,
			Statement statement) {
		if (runnable.isEmpty())
			return statement;
		return super.withAfters(method, target, statement); 
	}
	
	@Override
	public Description describeChild(FrameworkMethod method) {
		return super.describeChild(method);
	}

	@Override
	public void runChild(FrameworkMethod method, RunNotifier notifier) {
		super.runChild(method, notifier);
	}
	
	@Override
	public void run(final RunNotifier notifier) {
		Description me = getDescription();
		if (suited)
			notifier.fireTestRunStarted(me);
		super.run(notifier);
	}
	
	public long getStmtCount() {
		return stmtCount;
	}
}
