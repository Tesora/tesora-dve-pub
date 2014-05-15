// OS_STATUS: public
package com.tesora.dve.common;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.tesora.dve.concurrent.NamedTaskExecutorService;
import com.tesora.dve.concurrent.PEDefaultThreadFactory;
import com.tesora.dve.concurrent.PEThreadPoolExecutor;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEContextAwareException;
import com.tesora.dve.exceptions.PEException;

public class PEThreadContextTest {

	Throwable threadErr;

	@Before
	public void setup() {
		PEThreadContext.setEnabled(true);
	}
	
	@After
	public void tearDown() {
		PEThreadContext.clear();
		PEThreadContext.setEnabled(true);
		threadErr = null;
	}

	/**
	 * Test basic copy behavior of the thread context.
	 */
	@Test
	public void testCopyThreadContext() throws Exception {

		PEThreadContext.put("a1", "one");
		PEThreadContext.put("b2", "two");

		PEContext copy = PEThreadContext.copy();
		assertEquals("one", copy.get("a1"));
		assertEquals("two", copy.get("b2"));

		PEThreadContext.put("c3", "three");
		PEThreadContext.pushFrame("nested-main");
		assertNull(copy.get("c3"));
		assertThat(copy.toString(), not(containsString("nested-main")));
		copy.put("a1", "newOne");
		copy.push("nested-copy");
		assertEquals("one", PEThreadContext.get("a1"));
		assertThat(PEThreadContext.asString(), not(containsString("nested-copy")));
	}

	/**
	 * Test that PE exceptions automatically capture the thread context at the
	 * time they are created.
	 */
	@Test
	public void testExceptionContext() throws Exception {

		PEThreadContext.put("a1", "one");
		PEThreadContext.put("b2", "two");
		PEContextAwareException excp = new PEException("Test");

		PEThreadContext.put("a1", "newOne");
		assertThat(excp.getContext().toString(), allOf(
				containsString("a1=one"),
				containsString("b2=two"),
				not(containsString("newOne"))));

		PEThreadContext.pushFrame("Nested1");
		PEThreadContext.pushFrame("Nested2");
		PEThreadContext.put("c3", "three");

		excp = new PECodingException("Test");
		PEThreadContext.popFrame();
		PEThreadContext.popFrame();
		assertThat(excp.getContext().toString(), allOf(
				containsString("a1=newOne"),
				containsString("Nested1"),
				containsString("Nested2"),
				containsString("c3=three")));
	}

	/**
	 * Test that thread context is automatically propagated when using a
	 * PEThreadPoolExecutor.
	 */
	@Test
	public void testPropagateContext() throws Throwable {

		final ExecutorService svc = new PEThreadPoolExecutor("Test");

		PEThreadContext.put("a1", "one");

		svc.submit(new Runnable() {

			@Override
			public void run() {
				try {
					Assert.assertEquals("one", PEThreadContext.get("a1"));
					Assert.assertNull(PEThreadContext.get("b2"));
					PEThreadContext.put("a1", "two");
					PEThreadContext.put("b2", "two");
					svc.submit(new Runnable() {

						@Override
						public void run() {
							try {
								Assert.assertEquals("two", PEThreadContext.get("a1"));
								Assert.assertEquals("two", PEThreadContext.get("b2"));
								PEThreadContext.put("a1", "three");
								PEThreadContext.put("b2", "three");
							} catch (Throwable err) {
								threadErr = err;
							}
						}

					}).get();
					Assert.assertEquals("two", PEThreadContext.get("a1"));
					Assert.assertEquals("two", PEThreadContext.get("b2"));
					PEThreadContext.pushFrame("Nested");
					PEThreadContext.put("c3", "two");
					svc.submit(new Runnable() {

						@Override
						public void run() {
							try {
								Assert.assertEquals("two", PEThreadContext.get("a1"));
								Assert.assertEquals("two", PEThreadContext.get("b2"));
								Assert.assertEquals("two", PEThreadContext.get("c3"));
								PEThreadContext.popFrame();
								PEThreadContext.popFrame();
								Assert.assertNull(PEThreadContext.get("c3"));
							} catch (Throwable err) {
								threadErr = err;
							}
						}

					}).get();
					Assert.assertEquals("two", PEThreadContext.get("c3"));

				} catch (Throwable err) {
					threadErr = err;
				}
			}

		}).get();
		Assert.assertEquals("one", PEThreadContext.get("a1"));
		Assert.assertNull(PEThreadContext.get("b2"));

		svc.shutdown();
		svc.awaitTermination(30, TimeUnit.SECONDS);
		if (threadErr != null)
			throw threadErr;
	}

	/**
	 * Test chaining push/put calls.
	 */
	public void testChainedCalls() throws Throwable {
		PEThreadContext.pushFrame("Test").put("a1", "one").put("b2", "two");
		PEThreadContext.pushFrame("First").pushFrame("Second").put("foo", "bar");

		assertThat(PEThreadContext.asString(), allOf(
				containsString("Test"),
				containsString("a1=one"),
				containsString("b2=two"),
				containsString("First"),
				containsString("Second"),
				containsString("foo=bar")));
	}

	/**
	 * Test that executor threads get renamed for the currently executing task.
	 */
	@Test
	public void testNamedTask() throws Throwable {
		
		Logger logger = Logger.getLogger(PEThreadPoolExecutor.class);
		Level orig = logger.getLevel();
		logger.setLevel(Level.DEBUG);
		
		try {
		// single-thread executor
		NamedTaskExecutorService svc = new PEThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>(), new PEDefaultThreadFactory());

		for (int i = 0; i < 3; i++) {
			final String name = "test" + i;
			svc.submit(name, new Runnable() {

				@Override
				public void run() {
					try {
						Assert.assertThat(Thread.currentThread().getName(), containsString(name));
					} catch (Throwable err) {
						threadErr = err;
					}
				}

			});
		}

		svc.shutdown();
		svc.awaitTermination(30, TimeUnit.SECONDS);
		if (threadErr != null)
			throw threadErr;
		} finally {
			logger.setLevel(orig);
		}
	}

	/**
	 * Test debug context can be disabled.
	 */
	@Test
	public void testDisableContext() throws Throwable {

		Logger logger = Logger.getLogger(PEContext.class);
		Level orig = logger.getLevel();
		logger.setLevel(Level.INFO);

		try {
			PEThreadContext.setEnabled(true);
			PEThreadContext.pushFrame("Test");
			PEThreadContext.put("a1", "one");
			assertThat(PEThreadContext.asString(), containsString("a1=one"));

			PEThreadContext.setEnabled(false);
			PEThreadContext.pushFrame("Test2");
			PEThreadContext.put("b2", "two");
			assertThat(PEThreadContext.asString(), not(anyOf(
					containsString("Test"),
					containsString("a1=one"),
					containsString("Test2"),
					containsString("b2=two"))));

			final ExecutorService svc = new PEThreadPoolExecutor("Test");
			svc.submit(new Runnable() {

				@Override
				public void run() {
					try {
						PEThreadContext.put("foo", "bar");
						assertNull(PEThreadContext.get("foo"));
						PEThreadContext.popFrame();
						PEThreadContext.popFrame();
					} catch (Throwable err) {
						threadErr = err;
					}
				}
			});
		} finally {
			logger.setLevel(orig);
		}
	}
}
