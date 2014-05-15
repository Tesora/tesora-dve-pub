// OS_STATUS: public
package com.tesora.dve.tools.analyzer;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.tesora.dve.common.PEBaseTest;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.TestHost;
import com.tesora.dve.standalone.PETest;
import com.tesora.dve.tools.analyzer.sources.FileSource;

public class AnalyzerTest extends PEBaseTest {

	private static final String[] SUPPORTED_INPUT_FILE_TYPES = { "plain", "mysql" };

	/**
	 * Initialize a Host required by the Analyzer.
	 */
	@BeforeClass
	public static void setUp() throws Exception {
		TestHost.startServicesTransient(PETest.class);
	}

	@AfterClass
	public static void teardown() throws Exception {
		TestHost.stopServices();
	}

	@SuppressWarnings({ "unused" })
	@Test
	public void testAnalyzer() throws Exception {
		new Emulator(new AnalyzerOptions(), new AnalysisResultHandler());

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Exception {
				new Emulator(null, new AnalysisResultHandler());
			}
		}.assertException(IllegalArgumentException.class);

		new ExpectedExceptionTester() {
			@Override
			public void test() throws Exception {
				new Emulator(new AnalyzerOptions(), null);
			}
		}.assertException(IllegalArgumentException.class);
	}

	@Test
	public void testAnalyze() throws Throwable {
		final Emulator analyzer = new Emulator(new AnalyzerOptions(), new AnalysisResultHandler());
		try (final InputStream tempInputStream = new FileInputStream(File.createTempFile(this.toString(), null, null))) {
			for (final String supportedInputType : SUPPORTED_INPUT_FILE_TYPES) {
				@SuppressWarnings("resource")
				final AnalyzerSource src = new FileSource(supportedInputType, tempInputStream);
				src.analyze(analyzer);
			}

			new ExpectedExceptionTester() {
				@Override
				public void test() throws Throwable {
					try (final AnalyzerSource src = new FileSource(null, tempInputStream)) {
						src.analyze(analyzer);
					}
				}
			}.assertException(PEException.class, "Must specify file type");

			new ExpectedExceptionTester() {
				@Override
				public void test() throws Throwable {
					try (final AnalyzerSource src = new FileSource(SUPPORTED_INPUT_FILE_TYPES[0], (InputStream) null)) {
						src.analyze(analyzer);
					}
				}
			}.assertException(PEException.class, "Must specify the analysis input file");

			new ExpectedExceptionTester() {
				@Override
				public void test() throws Throwable {
					try (final AnalyzerSource src = new FileSource("not supported", tempInputStream)) {
						src.analyze(analyzer);
					}
				}
			}.assertException(PEException.class, "Unknown file type: not supported");
		}
	}

	private static class AnalysisResultHandler extends AnalyzerCallback {
		@Override
		public void onResult(final AnalyzerResult analyzerResult) {
			// Not needed for this test.
		}
	}
}
