package com.tesora.dve.tools;

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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;

import com.google.common.collect.ImmutableSet;
import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.DBType;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.common.PEUrl;
import com.tesora.dve.common.PEXmlUtils;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.bootstrap.Host;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.template.jaxb.ModelType;
import com.tesora.dve.sql.template.jaxb.TableTemplateType;
import com.tesora.dve.sql.template.jaxb.Template;
import com.tesora.dve.tools.aitemplatebuilder.AiTemplateBuilder;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats.TableStats;
import com.tesora.dve.tools.aitemplatebuilder.TemplateModelItem;
import com.tesora.dve.tools.analyzer.Analyzer;
import com.tesora.dve.tools.analyzer.AnalyzerCallback;
import com.tesora.dve.tools.analyzer.AnalyzerOption;
import com.tesora.dve.tools.analyzer.AnalyzerOptions;
import com.tesora.dve.tools.analyzer.AnalyzerPlanningError;
import com.tesora.dve.tools.analyzer.AnalyzerPlanningNotice;
import com.tesora.dve.tools.analyzer.AnalyzerPlanningResult;
import com.tesora.dve.tools.analyzer.AnalyzerResult;
import com.tesora.dve.tools.analyzer.AnalyzerSource;
import com.tesora.dve.tools.analyzer.Emulator;
import com.tesora.dve.tools.analyzer.StatementCounter;
import com.tesora.dve.tools.analyzer.StaticAnalyzer;
import com.tesora.dve.tools.analyzer.jaxb.AnalyzerType;
import com.tesora.dve.tools.analyzer.jaxb.DatabasesType.Database;
import com.tesora.dve.tools.analyzer.jaxb.DbAnalyzerCorpus;
import com.tesora.dve.tools.analyzer.jaxb.DbAnalyzerReport;
import com.tesora.dve.tools.analyzer.jaxb.TablesType.Table;
import com.tesora.dve.tools.analyzer.sources.FileSource;
import com.tesora.dve.tools.analyzer.sources.FrequenciesSource;
import com.tesora.dve.tools.analyzer.sources.JdbcSource;
import com.tesora.dve.tools.analyzer.stats.BasicStatsVisitor;
import com.tesora.dve.tools.analyzer.stats.StatementAnalyzer;

public class DVEAnalyzerCLI extends CLIBuilder {
	public static final String REPORT_FILE_EXTENSION = ".report";
	public static final String TEMPLATE_FILE_EXTENSION = ".template";
	public static final String CORPUS_FILE_EXTENSION = ".corpus";
	private static final String TEMP_FILE_EXTENSION = ".temp";
	private static final String ERROR_FILE_EXTENSION = ".error";
	private static final String DEFAULT_REPORT_NAME = "analyzer" + REPORT_FILE_EXTENSION;

	private static final DbAnalyzerCorpus EMPTY_FREQUENCY_CORPUS = new DbAnalyzerCorpus();

	private String url = PEConstants.MYSQL_URL_3307;
	private String user = PEConstants.ROOT;
	private String password = PEConstants.PASSWORD;

	private final DBNative dbNative = DBNative.DBNativeFactory.newInstance(DBType.MYSQL);

	private final List<String> databases = new ArrayList<String>();
	private final Map<String, Template> templates = new HashMap<String, Template>();
	private final AnalyzerOptions options = new AnalyzerOptions();

	private DbAnalyzerReport report = null;

	public DVEAnalyzerCLI(String[] args) throws PEException {
		super(args, "Analyzer Tool");

		registerCommand(new CommandType(new String[] { "connect" }, "<url> <user> <password>",
				"Connect to the specified database (default=" + PEConstants.MYSQL_URL + " " + PEConstants.ROOT + " "
						+ PEConstants.PASSWORD + ")."));

		registerCommand(new CommandType(new String[] { "set", "database" }, "<database> [<database>]...",
				"Set the name of the database to use for analysis."));

		registerCommand(new CommandType(
				new String[] { "static" },
				"[<analyze>]",
				"Extract static analysis information from the database."
						+ "If TRUE <analyze> executes 'ANALYZE TABLE' command before collecting table statistics (preferred, but causes update to the INFORMATION_SCHEMA and requires INSERT privileges)."));

		registerCommand(new CommandType(new String[] { "open", "report" }, "[<filename>]",
				"Open an existing static analysis file (default filename = " + DEFAULT_REPORT_NAME + ")."));
		registerCommand(new CommandType(new String[] { "save", "report" }, "[<filename>]",
				"Write static analysis data to the specified file (default filename = " + DEFAULT_REPORT_NAME + ")."));
		registerCommand(new CommandType(new String[] { "display", "report" }, "Display the current analysis."));
		registerCommand(new CommandType(
				new String[] { "update", "row", "counts" },
				"<filename>",
				"Replace row counts in the current static report by values from a given tab-delimited file obtained by 'select * from information_schema.tables;' (must include column headers)."));

		registerCommand(new CommandType(
				new String[] { "generate", "broadcast", "templates" },
				"Generate all-broadcast templates."));
		registerCommand(new CommandType(
				new String[] { "generate", "random", "templates" },
				"Generate all-random templates."));
		registerCommand(new CommandType(
				new String[] { "generate", "basic", "templates" },
				"<cutoff cardinality> [<base template>]",
				"Generate broadcast/random templates with a hard broadcast cardinality cutoff."));
		registerCommand(new CommandType(
				new String[] { "generate", "guided", "templates" },
				"<cutoff cardinality> <fk> <safe> [<corpus file>] [<base template>]",
				"Generate templates with a hard broadcast cardinality cutoff. See \"generate templates\" command for the definition of other parameters."));
		registerCommand(new CommandType(
				new String[] { "generate", "templates" },
				"<fk> <safe> [<corpus file>] [<base template>]",
				"Generate templates from a given frequency corpus."
						+ " If TRUE the <fk> switch forces the generator to coolocate the foreign keys."
						+ " If TRUE the <safe> switch forces the generator to generate only ranges backed-up by auto-increment columns."
						+ " Note that colocating foreign keys alone generally requires heavy broadcasting,"
						+ " turning both <fk> and <safe> switches ON at the same time may easily lead to unacceptable broadcasting in the template."
						+ " Use with caution."
						+ " A corpus file provides important information on statements executed agains the schema."
						+ " The corpus file should ideally cover majority of the schema tables (> 60%)."
						+ " It is generally necessary for a good template, but is not strictly required,"
						+ " as table cardinalities and FK relationships can be resolved from the static report alone."));

		registerCommand(new CommandType(new String[] { "open", "template" }, "<filename>",
				"Open a template from the specified file."));
		registerCommand(new CommandType(new String[] { "open", "templates" }, "[<folder>]",
				"Open all template files in the specified folder (default folder = current working directory)."));
		registerCommand(new CommandType(new String[] { "save", "template" }, "<template name> [<filename>]",
				"Write the specified template to a file (default filename = <template name>" + TEMPLATE_FILE_EXTENSION + ")."));
		registerCommand(new CommandType(new String[] { "save", "templates" }, "[<folder>]",
				"Write all templates to the specified folder (default folder = current working directory)."));
		registerCommand(new CommandType(new String[] { "display", "template" }, "[<template name>]",
				"Display the specified template or all templates if name isn't specified."));
		registerCommand(new CommandType(new String[] { "compare", "templates" },
				"<template 1> <template 2> [<output file>]", "Compare given template files."));
		registerCommand(new CommandType(new String[] { "advanced", "compare", "templates" },
				"<corpus file> <template 1> <template 2> [<output file>]",
				"Compare given template files. Includes table statistics in the output. This comparator requires a static report and frequency corpus."));

		registerCommand(new CommandType(
				new String[] { "dynamic", "table" },
				"<connection-url> <username> <password> <log table name> [<output file>]",
				"Perform dynamic analysis using specified mysql query log table on specified connection."));
		registerCommand(new CommandType(new String[] { "dynamic", "plain" }, "<query log file> [<output file>]",
				"Perform dynamic analysis using the specified file of queries (one per line)."));
		registerCommand(new CommandType(new String[] { "dynamic", "mysql" }, "<mysql log file> [<output file>]",
				"Perform dynamic analysis using specified mysql query log."));
		registerCommand(new CommandType(new String[] { "dynamic", "corpus" }, "<corpus file> [<output file>]",
				"Perform dynamic analysis on the specified frequency corpus file."));

		registerCommand(new CommandType(
				new String[] { "frequencies", "table" },
				"<corpus file> <connection-url> <username> <password> <log table name> [<log table name> ...]",
				"Perform frequency analysis using specified mysql query log table on specified connection, writing the output to the specified <corpus file>."));
		registerCommand(new CommandType(
				new String[] { "frequencies", "plain" },
				"<corpus file> <query log file> [<query log file> ...]",
				"Perform frequency analysis on specified query log files (one per line), writing the output to the specified <corpus file>."));
		registerCommand(new CommandType(
				new String[] { "frequencies", "mysql" },
				"<corpus file> <mysql log file> [<mysql log file> ...]",
				"Perform frequency analysis on specified mysql log files, writing the output to the specified <corpus file>."));

		registerCommand(new CommandType(new String[] { "analyze", "corpus" }, "<corpus file>",
				"Analyze statements in the specified corpus file."));

		registerCommand(new CommandType(new String[] { "option" }, "<key> <value>", "Set analysis option"));
		registerCommand(new CommandType(new String[] { "options" },
				"Display available options, along with current values and a description."));

		registerCommand(new CommandType(new String[] { "status" }, "Show the current tool status and configuration."));
		registerCommand(new CommandType(new String[] { "reset" }, "Reset to initial state."));

		// Create a dummy Host with minimum configuration
		final Properties props = new Properties();
		props.put(DBHelper.CONN_DRIVER_CLASS, PEConstants.MYSQL_DRIVER_CLASS);
		new Host(props, false);
		super.setDebugMode(true); //  Toggle the debug mode on for the Analyzer.
	}

	/**
	 * Specify the connection information to use when connecting to a database.
	 * This information is used when the 'static' command is executed.
	 * Note: this information is persisted to and read back from the report file
	 * 
	 * @param scanner
	 * @throws PEException
	 */
	public void cmd_connect(Scanner scanner) throws PEException {
		if (scanner.hasNext()) {
			url = scan(scanner, "jdbcURL");
			user = scan(scanner, "user");
			password = scan(scanner, "password");
		}

		url = PEUrl.fromUrlString(url).getURL();

		println("... Connection set to '" + url.toString() + "'");
	}

	/**
	 * Reset the analyzer to initial default settings
	 */
	public void cmd_reset() {
		report = null;
		clearDatabases();
		clearTemplates();

		url = PEConstants.MYSQL_URL_3307;
		user = PEConstants.ROOT;
		password = PEConstants.PASSWORD;

		options.reset();

		println("... Reset to initial settings");
	}

	/**
	 * Dump information to the console about the current configuration of the
	 * tool
	 */
	public void cmd_status() {
		println("Current Settings:");
		println("    URL       = " + url);
		println("    User      = " + user);
		println("    Password  = " + password);
		println("    Databases = " + databasesToString());
		println("    Options:");
		for (final AnalyzerOption ao : options.getOptions()) {
			println("        " + ao.getName() + " = " + ao.getCurrentValue());
		}
	}

	/**
	 * Perform static analysis of a database. In order to run the database(s)
	 * must have been set.
	 * 
	 * @throws PEException
	 * @throws SQLException
	 */
	public void cmd_static(Scanner scanner) throws PEException, SQLException {
		checkConnection();
		checkDatabase();

		final Boolean analyze = BooleanUtils.toBoolean(scan(scanner));

		println("... Connecting to " + url + " as " + user + "/" + password);
		final DBHelper dbHelper = new DBHelper(url, user, password).connect();

		println("... Starting static analysis for '" + databasesToString() + "'");
		report = StaticAnalyzer.doStatic(dbNative, options, url, user, password, databases, dbHelper, analyze);

		println("... Static anlysis complete for '" + databasesToString() + "'");
	}

	/**
	 * Open the specified report file and use it to configure the analyzer
	 * 
	 * @param scanner
	 * @throws PEException
	 */
	public void cmd_open_report(Scanner scanner) throws PEException {
		File reportFile = scanFile(scanner);

		if (reportFile == null) {
			reportFile = new File(DEFAULT_REPORT_NAME);
		}

		println("... Reading analyzer report from file '" + reportFile.getAbsolutePath() + "'");

		report = PEXmlUtils.unmarshalJAXB(reportFile, DbAnalyzerReport.class);

		// Setup the list of databases based on those represented in the
		// report
		clearDatabases();
		for (final Database db : report.getDatabases().getDatabase()) {
			databases.add(db.getName());
		}
		println("... Using databases '" + databasesToString() + "'");

		// Fill in other global settings
		final AnalyzerType.Connection c = report.getAnalyzer().getConnection();
		if (c != null) {
			user = c.getUser();
			password = c.getPassword();
			url = c.getUrl();
		}

		final AnalyzerType.Options os = report.getAnalyzer().getOptions();
		if (os != null) {
			for (final AnalyzerType.Options.Option o : os.getOption()) {
				try {
					options.setOption(o.getKey(), o.getValue());
				} catch (final PEException e) {
					println("WARNING: " + e.getMessage() + ". Will be ignored.");
				}
			}
		}
	}

	/**
	 * Save the analyzer report file to the specified filename
	 * 
	 * @param scanner
	 * @throws PEException
	 */
	public void cmd_save_report(Scanner scanner) throws PEException {
		checkReport();

		// Filename is optional - if not set then pick a default
		File file = scanFile(scanner);

		if (file == null) {
			file = new File(DEFAULT_REPORT_NAME);
		}

		println("... Writing analyzer report to file '" + file.getAbsolutePath() + "'");
		writeToFileOrScreen(file, PEXmlUtils.marshalJAXB(report));
	}

	/**
	 * Dump the contents of the analyzer report to screen or to a file
	 * 
	 * @throws PEException
	 */
	public void cmd_display_report() throws PEException {
		checkReport();

		writeToFileOrScreen(null, PEXmlUtils.marshalJAXB(report));
	}

	/**
	 * Update row counts in a given static report.
	 * 
	 * @throws PEException
	 * @throws FileNotFoundException
	 */
	public void cmd_update_row_counts(Scanner scanner) throws PEException, FileNotFoundException {
		checkDatabase();
		checkReport();

		final File file = scanFile(scanner, "row counts file");

		println("... Reading row counts from file '" + file.getAbsolutePath() + "'");

		final Map<QualifiedName, Integer> rowCounts = getRowCountsFromFile(file);

		for (final Database staticReportDb : this.report.getDatabases().getDatabase()) {
			for (final Table staticReportTable : staticReportDb.getTables().getTable()) {
				final QualifiedName tableName = new QualifiedName(new UnqualifiedName(staticReportDb.getName()),
						new UnqualifiedName(staticReportTable.getName()));
				final Integer newCount = rowCounts.get(tableName);
				if (newCount != null) {
					staticReportTable.setRowCount(newCount);
				}
			}
		}
	}

	/**
	 * Run dynamic analysis using the specified general log table
	 * 
	 * @param scanner
	 * @throws Throwable
	 */
	@SuppressWarnings({ "synthetic-access", "resource" })
	public void cmd_dynamic_table(Scanner scanner) throws Throwable {
		final String gltUrl = scan(scanner, "general log table url");
		final String gltUsername = scan(scanner, "general log table username");
		final String gltPassword = scan(scanner, "general log table password");
		final String gltLogTable = scan(scanner, "general log table");
		final File outputFile = scanFile(scanner);

		executeDynamicCmdOnAnalyzerSources(Collections.singleton(new JdbcSource(gltUrl, gltUsername, gltPassword, gltLogTable)), outputFile,
				new DynamicAnalyzerCallback());
	}

	/**
	 * Run dynamic analysis using the specified plain log file
	 * 
	 * @param scanner
	 * @throws Throwable
	 */
	@SuppressWarnings({ "synthetic-access", "resource" })
	public void cmd_dynamic_plain(Scanner scanner) throws Throwable {
		final File logFile = scanFile(scanner, "plain log file");
		final File outputFile = scanFile(scanner);

		executeDynamicCmdOnAnalyzerSources(Collections.singleton(new FileSource("plain", logFile)), outputFile, new DynamicAnalyzerCallback());
	}

	/**
	 * Run dynamic analysis using the specified general log file
	 * 
	 * @param scanner
	 * @throws Throwable
	 */
	@SuppressWarnings({ "synthetic-access", "resource" })
	public void cmd_dynamic_mysql(Scanner scanner) throws Throwable {
		final File logFile = scanFile(scanner, "mysql log file");
		final File outputFile = scanFile(scanner);

		executeDynamicCmdOnAnalyzerSources(Collections.singleton(new FileSource("mysql", logFile)), outputFile, new DynamicAnalyzerCallback());
	}

	/**
	 * Run dynamic analysis using the specified frequency corpus
	 * 
	 * @param scanner
	 * @throws Throwable
	 */
	@SuppressWarnings("resource")
	public void cmd_dynamic_corpus(Scanner scanner) throws Throwable {
		final File corpusFile = scanFile(scanner, "corpus file");
		final File output = scanFile(scanner);

		final DbAnalyzerCorpus corpus = loadCorpus(corpusFile);

		executeDynamicCmdOnAnalyzerSources(Collections.singleton(new FrequenciesSource(corpus)), output, new DynamicAnalyzerCallback());
	}

	private DbAnalyzerCorpus loadCorpus(File file) throws PEException {
		println("... Reading corpus from file '" + file.getAbsolutePath() + "'");
		return PEXmlUtils.unmarshalJAXB(file, DbAnalyzerCorpus.class);
	}

	public void cmd_frequencies_table(Scanner scanner) throws Throwable {
		final File corpusFile = scanFile(scanner, "corpus file");

		final String gltUrl = scan(scanner, "general log table url");
		final String gltUsername = scan(scanner,
				"general log table username");
		final String gltPassword = scan(scanner,
				"general log table password");

		final Set<JdbcSource> sources = this.scanJdbcSources(scanner, gltUrl, gltUsername, gltPassword);
		this.executeFrequenciesCmdOnAnalyzerSources(sources, corpusFile);
	}

	public void cmd_frequencies_plain(Scanner scanner) throws Throwable {
		this.executeFrequenciesCmdOnFileSources(scanner, "plain");
	}

	public void cmd_frequencies_mysql(Scanner scanner) throws Throwable {
		this.executeFrequenciesCmdOnFileSources(scanner, "mysql");
	}

	private StatementCounter getInitializedStatementCounter(final File corpusFile)
			throws Throwable {
		checkDatabase();
		checkReport();

		final File reportFile = new File(corpusFile.getAbsolutePath() + TEMP_FILE_EXTENSION);
		final File errorFile = new File(corpusFile.getAbsolutePath() + ERROR_FILE_EXTENSION);

		final StatementCounter sc = new StatementCounter(options, corpusFile,
				reportFile, options.getFrequencyAnalysisCheckpointInterval(),
				errorFile, this.getPrintStream());

		/*
		 * Here we generate initial all-broadcast templates, so that the command
		 * "never fails" and allows the user to proceed with advanced template
		 * generation.
		 */
		generateAllBroadcastTemplates();

		try {
			loadSchema(sc);
		} catch (final Throwable e) {
			clearTemplates();

			throw e;
		}

		return sc;
	}

	private Emulator getInitializedEmulator(final AnalyzerCallback callback, final File outputFile)
			throws Throwable {
		checkDatabase();
		checkReport();
		checkTemplate();

		final Emulator em = new Emulator(options, callback);

		loadSchema(em);

		if (outputFile != null) {
			try {
				callback.setOutputStream(new PrintStream(outputFile));
				println("... Results saved to file '" + outputFile.getAbsolutePath() + "'");
			} catch (final Exception e) {
				throw new PEException("Failed to write to file '" + outputFile.getAbsolutePath() + "'", e);
			}
		}

		return em;
	}

	/**
	 * Set the value of a specified analyzer option
	 * 
	 * @param scanner
	 * @throws PEException
	 */
	public void cmd_option(Scanner scanner) throws PEException {
		final String key = scan(scanner, "option key");
		final String value = scan(scanner, "option value");
		options.setOption(key, value);
	}

	/**
	 * Output the current set of options and their values
	 */
	public void cmd_options() {
		for (final AnalyzerOption ao : options.getOptions()) {
			println(ao.getName() + " = " + ao.getCurrentValue() + ".  " + ao.getDefinition());
		}
	}

	/**
	 * Open a template from the specified file. The template name must match on
	 * of the database we are analyzing
	 * 
	 * @param scanner
	 * @throws PEException
	 */
	public void cmd_open_template(Scanner scanner) throws PEException {
		final File templateFile = scanFile(scanner, "filename");
		addTemplate(templateFile);
	}

	/**
	 * Open all templates from the specified folder or current working
	 * directory. Templates are identified by their file extension
	 * 
	 * @param scanner
	 * @throws PEException
	 */
	public void cmd_open_templates(Scanner scanner) throws PEException {
		final File folder = scanFile(scanner, "folder");

		if (!folder.isDirectory()) {
			throw new PEException("'" + folder.getAbsolutePath() + "' is not a valid directory");
		}

		final File[] templateFiles = folder.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(final File dir, final String name) {
				return name.toLowerCase().endsWith(TEMPLATE_FILE_EXTENSION);
			}
		});

		for (final File templateFile : templateFiles) {
			addTemplate(templateFile);
		}
	}

	/**
	 * Write the specified template out to a file. The filename is either
	 * specified or based on the template name
	 * 
	 * @param scanner
	 * @throws PEException
	 */
	public void cmd_save_template(Scanner scanner) throws PEException {
		checkTemplate();

		final String name = scan(scanner, "template");
		File file = scanFile(scanner);

		if (file == null) {
			file = new File(name + TEMPLATE_FILE_EXTENSION);
		}

		println("... Writing template to file '" + file + "'");

		writeToFileOrScreen(file, PEXmlUtils.marshalJAXB(checkTemplate(name)));
	}

	/**
	 * Write all templates to the specified folder or current working directory
	 * if folder not specified
	 * 
	 * @param scanner
	 * @throws PEException
	 */
	public void cmd_save_templates(Scanner scanner) throws PEException {
		checkTemplate();

		final File folder = scanFile(scanner);
		if ((folder != null) && !folder.isDirectory()) {
			throw new PEException("'" + folder.getAbsolutePath() + "' is not a valid directory.");
		}

		for (final Template t : this.templates.values()) {
			final File file = new File(folder, t.getName() + TEMPLATE_FILE_EXTENSION);

			println("... Writing template to file '" + file.getAbsolutePath() + "'");

			writeToFileOrScreen(file, PEXmlUtils.marshalJAXB(t));
		}
	}

	/**
	 * Display one or more templates to the console
	 * 
	 * @throws PEException
	 */
	public void cmd_display_template(Scanner scanner) throws PEException {
		checkTemplate();

		final String name = scan(scanner);

		if (name == null) {
			for (final Template t : templates.values()) {
				writeToFileOrScreen(null, PEXmlUtils.marshalJAXB(t));
			}
		} else {
			writeToFileOrScreen(null, PEXmlUtils.marshalJAXB(checkTemplate(name)));
		}
	}

	/**
	 * Generate all-broadcast templates for selected databases.
	 */
	public void cmd_generate_broadcast_templates() throws PEException {
		checkReport();
		checkDatabase();

		generateAllBroadcastTemplates();
	}

	/**
	 * Generate all-random templates for selected databases.
	 */
	public void cmd_generate_random_templates() throws PEException {
		checkReport();
		checkDatabase();

		generateAllRandomTemplates();
	}

	/**
	 * Generate broadcast/random templates with a hard broadcast cardinality
	 * cutoff.
	 */
	public void cmd_generate_basic_templates(final Scanner scanner) throws Exception {
		checkReport();
		checkDatabase();

		final long broadcastCardinalityCutoff = scanLong(scanner, "cutoff cardinality");
		final File baseTemplateFile = scanFile(scanner, null);

		generateBroadcastCutoffTemplates(broadcastCardinalityCutoff, baseTemplateFile);
	}

	/**
	 * Generate guided templates with a hard broadcast cardinality cutoff.
	 * @throws Throwable 
	 */
	public void cmd_generate_guided_templates(final Scanner scanner) throws Throwable {
		checkReport();
		checkDatabase();

		final long broadcastCardinalityCutoff = scanLong(scanner, "cutoff cardinality");
		final Boolean followForeignKeys = scanBoolean(scanner, "fk");
		final Boolean isSafeMode = scanBoolean(scanner, "safe");

		final Map<Class<?>, File> optionalInputFiles = scanFilesByTypeOptionalSingle(scanner,
				ImmutableSet.<Class<?>> of(DbAnalyzerCorpus.class, Template.class));
		final File corpusFile = optionalInputFiles.get(DbAnalyzerCorpus.class);
		final File baseTemplateFile = optionalInputFiles.get(Template.class);

		generateCorpusBasedTemplates(broadcastCardinalityCutoff, followForeignKeys, isSafeMode, corpusFile, baseTemplateFile);
	}

	/**
	 * Automatically generate templates from a given corpus.
	 * @throws Throwable 
	 */
	public void cmd_generate_templates(final Scanner scanner) throws Throwable {
		checkReport();
		checkDatabase();

		final Boolean followForeignKeys = scanBoolean(scanner, "fk");
		final Boolean isSafeMode = scanBoolean(scanner, "safe");

		final Map<Class<?>, File> optionalInputFiles = scanFilesByTypeOptionalSingle(scanner,
				ImmutableSet.<Class<?>> of(DbAnalyzerCorpus.class, Template.class));
		final File corpusFile = optionalInputFiles.get(DbAnalyzerCorpus.class);
		final File baseTemplateFile = optionalInputFiles.get(Template.class);

		generateCorpusBasedTemplates(null, followForeignKeys, isSafeMode, corpusFile, baseTemplateFile);
	}

	/**
	 * Compare given templates.
	 */
	public void cmd_compare_templates(Scanner scanner) throws PEException {
		final File baseTemplateFile = scanFile(scanner, "template 1");
		final File comparedTemplateFile = scanFile(scanner, "template 2");
		final File outputFile = scanFile(scanner);

		final Template baseTemplate = PEXmlUtils.unmarshalJAXB(baseTemplateFile, Template.class);
		final Template comparedTemplate = PEXmlUtils.unmarshalJAXB(comparedTemplateFile, Template.class);

		compareTemplates(baseTemplate, comparedTemplate, null, outputFile);
	}

	public void cmd_advanced_compare_templates(Scanner scanner) throws PEException {
		checkReport();
		checkDatabase();

		final File corpusFile = scanFile(scanner, "corpus file");
		final File baseTemplateFile = scanFile(scanner, "template 1");
		final File comparedTemplateFile = scanFile(scanner, "template 2");
		final File outputFile = scanFile(scanner);

		final DbAnalyzerCorpus corpus = loadCorpus(corpusFile);
		final CorpusStats corpusStats = new CorpusStats(corpus.getDescription(), this.options.getCorpusScaleFactor());
		final Template baseTemplate = PEXmlUtils.unmarshalJAXB(baseTemplateFile, Template.class);
		final Template comparedTemplate = PEXmlUtils.unmarshalJAXB(comparedTemplateFile, Template.class);

		try {

			/* Generate initial "safe" templates for schema loading. */
			generateAllBroadcastTemplates();

			/* Analyze the static report. */
			final StatementAnalyzer analyzer = new StatementAnalyzer(this.options, corpusStats);
			loadSchema(analyzer);
			loadCorpusStats(analyzer, corpusStats);

			/* Analyze the corpus. */
			try (final FrequenciesSource frequencies = new FrequenciesSource(corpus)) {
				frequencies.analyze(analyzer);
			}

		} catch (final Throwable e) {
			clearTemplates();
			throw new PEException("Unable to analyze corpus '" + corpusFile.getAbsolutePath() + "'", e);
		}

		compareTemplates(baseTemplate, comparedTemplate, corpusStats, outputFile);
	}

	private void compareTemplates(final Template baseTemplate, final Template comparedTemplate, final CorpusStats corpusStats, final File outputFile)
			throws PEException {
		final Map<String, String> templateDiffs = compareTemplates(baseTemplate, comparedTemplate);
		final List<String> results = new ArrayList<String>(templateDiffs.size());
		for (final Map.Entry<String, String> item : templateDiffs.entrySet()) {
			final String tableName = item.getKey();
			final String itemDiff = item.getValue();

			final ColorStringBuilder entry = new ColorStringBuilder();
			if (corpusStats != null) {
				for (final Database db : getSelectedDatabases()) {
					final TableStats table = corpusStats.findTable(new QualifiedName(db.getName().concat(QualifiedName.PART_SEPARATOR).concat(tableName)));
					entry.append(table.toString());
				}
			} else {
				entry.append(tableName);
			}

			results.add(entry.append(": ").append(itemDiff, (itemDiff.equals("MATCH") ? ConsoleColor.GREEN : ConsoleColor.DEFAULT)).toString());
		}

		if (outputFile == null) {
			printCollection(results, System.out);
		} else {
			try (final PrintStream outputWriter = new PrintStream(outputFile)) {
				printCollection(results, outputWriter);
			} catch (final FileNotFoundException e) {
				throw new PEException("Could not open the output file '" + outputFile + "'");
			}
		}
	}

	public static Map<String, String> compareTemplates(final Template baseTemplate, final Template comparedTemplate) {
		final Comparator<TableTemplateType> orderByMatch = new Comparator<TableTemplateType>() {
			@Override
			public int compare(TableTemplateType a, TableTemplateType b) {
				if (a.getMatch().equals(".*")) {
					return 0;
				}
				return a.getMatch().compareTo(b.getMatch());
			}
		};

		final Set<TableTemplateType> baseItems = new TreeSet<TableTemplateType>(orderByMatch);
		final Set<TableTemplateType> comparedItems = new TreeSet<TableTemplateType>(orderByMatch);

		baseItems.addAll(baseTemplate.getTabletemplate());
		comparedItems.addAll(comparedTemplate.getTabletemplate());

		final Set<TableTemplateType> addedItems = getDifference(comparedItems, baseItems, orderByMatch);
		final Set<TableTemplateType> removedItems = getDifference(baseItems, comparedItems, orderByMatch);
		final Set<TableTemplateType> intersectionItems = getIntersection(comparedItems, baseItems, orderByMatch);

		final String arrow = " -> ";
		final String columnSeparator = ", ";
		final Map<String, String> results = new TreeMap<String, String>();
		for (final TableTemplateType item : intersectionItems) {
			final TableTemplateType baseItem = getFromCollection(baseItems, item);
			final TableTemplateType comparedItem = getFromCollection(comparedItems, item);

			final ModelType baseModel = baseItem.getModel();
			final ModelType itemModel = comparedItem.getModel();

			if (!baseModel.equals(itemModel)) {
				results.put(item.getMatch(), baseModel.value().concat(arrow).concat(itemModel.value()));
			} else {
				if (baseModel.equals(ModelType.RANGE)) {
					final List<String> baseColumns = baseItem.getColumn();
					final List<String> itemColumns = comparedItem.getColumn();

					if (!baseColumns.containsAll(itemColumns) || !itemColumns.containsAll(baseColumns)) {
						results.put(item.getMatch(), ("(" + ModelType.RANGE + ") <")
								.concat(StringUtils.join(baseColumns, columnSeparator)).concat(arrow)
								.concat(StringUtils.join(itemColumns, columnSeparator)).concat(">"));
						continue;
					}
				}
				results.put(item.getMatch(), "MATCH");
			}
		}

		for (final TableTemplateType item : addedItems) {
			results.put(item.getMatch(), "ADDED");
		}

		for (final TableTemplateType item : removedItems) {
			results.put(item.getMatch(), "REMOVED");
		}

		return results;
	}

	public void cmd_analyze_corpus(Scanner scanner) throws PEException {
		final File corpusFile = scanFile(scanner, "corpus file");

		final DbAnalyzerCorpus corpus = loadCorpus(corpusFile);

		final BasicStatsVisitor bsv = new BasicStatsVisitor();

		try {

			/* Generate "safe" templates for schema loading. */
			generateAllBroadcastTemplates();

			final StatementAnalyzer analyzer = new StatementAnalyzer(options, bsv);
			loadSchema(analyzer);

			try (final FrequenciesSource frequencies = new FrequenciesSource(corpus)) {
				frequencies.analyze(analyzer);
			}

			bsv.display(System.out);
		} catch (final Throwable e) {
			clearTemplates();
			throw new PEException("Unable to analyze corpus '" + corpusFile.getAbsolutePath() + "'", e);
		}
	}

	public void cmd_set_database(Scanner scanner) throws PEException {
		String database = scan(scanner, "database");

		while (database != null) {
			databases.add(database);
			database = scan(scanner);
		}

		println("... Databases set to '" + this.databasesToString() + "'");
	}

	private static class DynamicAnalyzerCallback extends AnalyzerCallback {
		@Override
		public void onResult(final AnalyzerResult bar) {
			@SuppressWarnings("resource")
			// This should be handled by the owner.
			final PrintStream output = getOutputStream();

			bar.printStatement(output);
			if (bar instanceof AnalyzerPlanningError) {
				output.println("... Analysis FAILED");
			}
			bar.printTables(output);
			if (bar instanceof AnalyzerPlanningResult) {
				((AnalyzerPlanningResult) bar).printPlan(output);
			} else if (bar instanceof AnalyzerPlanningError) {
				((AnalyzerPlanningError) bar).getFault().printStackTrace(output);
			} else if (bar instanceof AnalyzerPlanningNotice) {
				output.println(((AnalyzerPlanningNotice) bar).getMessage());
			}
		}
	}

	protected void writeToFileOrScreen(File file, String content) throws PEException {
		if (file != null) {
			PEFileUtils.writeToFile(file, content, true);
		} else {
			println(content);
		}
	}

	private void executeDynamicCmdOnAnalyzerSources(final Collection<? extends AnalyzerSource> sources, final File output,
			final AnalyzerCallback analyzerCallback)
			throws Throwable {
		try {
			final Emulator analyzer = getInitializedEmulator(analyzerCallback, output);

			for (final AnalyzerSource source : sources) {
				println("... Starting dynamic analysis of '" + source.getDescription() + "'");
				source.analyze(analyzer);
			}
			println("... Dynamic analysis complete");
		} catch (final Throwable e) {
			throw new PEException("Dynamic analysis has failed.", e);
		} finally {
			this.closeAnalyzerSources(sources);
			if (output != null) {
				analyzerCallback.close();
			}
		}
	}

	private void executeFrequenciesCmdOnFileSources(final Scanner scanner, final String sourceType) throws Throwable {
		final File corpusFile = scanFile(scanner, "corpus file");
		final Set<FileSource> sources = this.scanFileSources(scanner, sourceType);
		this.executeFrequenciesCmdOnAnalyzerSources(sources, corpusFile);
	}

	private void executeFrequenciesCmdOnAnalyzerSources(final Collection<? extends AnalyzerSource> sources, final File output) throws Throwable {
		final StatementCounter sc = getInitializedStatementCounter(output);

		try {
			for (final AnalyzerSource source : sources) {
				println("... Starting frequency analysis of '" + source.getDescription() + "'");
				source.analyze(sc);
			}
			println("... Frequency analysis complete");
		} catch (final Exception e) {
			throw new PEException("Frequency analysis has failed.", e);
		} finally {
			this.closeAnalyzerSources(sources);
			sc.close();
			clearTemplates();
		}
	}

	@SuppressWarnings("resource")
	private Set<FileSource> scanFileSources(final Scanner scanner, final String sourceType) throws Exception {

		/*
		 * Scan given log files. There must be at least one.
		 */
		final Set<FileSource> analyzerSources = new LinkedHashSet<FileSource>();
		try {
			File userLogFile = scanFile(scanner, sourceType + " log file");
			while (userLogFile != null) {
				analyzerSources.add(new FileSource(sourceType, userLogFile));
				userLogFile = scanFile(scanner, null);
			}

			return analyzerSources;
		} catch (final Exception e) {
			this.closeAnalyzerSources(analyzerSources);

			throw e;
		}
	}

	@SuppressWarnings("resource")
	private Set<JdbcSource> scanJdbcSources(final Scanner scanner, final String scanUrl, final String scanUser, final String scanPassword) throws Throwable {

		/*
		 * Scan given log files. There must be at least one.
		 */
		final Set<JdbcSource> analyzerSources = new LinkedHashSet<JdbcSource>();
		try {
			String logTable = scan(scanner, "log table");
			while (logTable != null) {
				analyzerSources.add(new JdbcSource(scanUrl, scanUser, scanPassword, logTable));
				logTable = scan(scanner, null);
			}

			return analyzerSources;
		} catch (final Throwable e) {
			this.closeAnalyzerSources(analyzerSources);

			throw e;
		}
	}

	private void closeAnalyzerSources(final Collection<? extends AnalyzerSource> sources) {
		for (final AnalyzerSource source : sources) {
			source.close();
		}
	}

	private void checkConnection() throws PEException {
		if (url == null) {
			throw new PEException("Not connected");
		}
	}

	private void checkTemplate() throws PEException {
		if (templates == null) {
			throw new PEException("No templates are available. You can load an existing templates or generate a new ones.");
		}
	}

	private Template checkTemplate(String name) throws PEException {
		checkTemplate();

		final Template t = templates.get(name);

		if (t != null) {
			return t;
		}

		throw new PEException("Template '" + name + "' is not available");
	}

	private void checkReport() throws PEException {
		if (report == null) {
			throw new PEException("You need a static report to proceed. You can load an existing report from file or generate a new one.");
		}
	}

	private void checkDatabase() throws PEException {
		if (databases.isEmpty()) {
			throw new PEException("Database list has not been configured");
		}
	}

	private String checkDatabase(String name) throws PEException {
		checkDatabase();

		for (final String db : databases) {
			if (name.equals(db)) {
				return db;
			}
		}
		throw new PEException("Database '" + name + "' is not in the list of databases");
	}

	private String databasesToString() {
		if (!databases.isEmpty()) {
			return StringUtils.join(databases, ", ");
		}

		return StringUtils.EMPTY;
	}

	private void addTemplate(final File templateFile) throws PEException {
		println("... Reading template from file '" + templateFile.getAbsolutePath() + "'");

		final Template template = PEXmlUtils.unmarshalJAXB(templateFile, Template.class);

		addTemplate(template);
	}

	private void reloadTemplates(final List<Template> reloadTemplates) throws PEException {
		clearTemplates();
		for (final Template template : reloadTemplates) {
			addTemplate(template);
		}
	}

	private void addTemplate(final Template template) throws PEException {
		checkDatabase(template.getName());

		templates.put(template.getName(), template);

		println("... Template '" + template.getName() + "' added to catalog");
	}

	private void clearDatabases() {
		println("... Removing current databases");
		databases.clear();
	}

	private void clearTemplates() {
		println("... Removing current templates");
		templates.clear();
	}

	private static <T extends TableTemplateType> T getFromCollection(final Collection<T> items, final T value) {
		for (final T item : items) {
			if (isMatch(item, value)) {
				return item;
			}
		}
		return null;
	}

	private static <T extends TableTemplateType> Set<T> getDifference(final Set<T> a, final Set<T> b,
			final Comparator<? super T> comparator) {
		final Set<T> difference = new TreeSet<T>(comparator);
		difference.addAll(a);
		for (final T aItem : a) {
			for (final T bItem : b) {
				if (isMatch(aItem, bItem)) {
					difference.remove(aItem);
				}
			}
		}
		return difference;
	}

	private static <T extends TableTemplateType> Set<T> getIntersection(final Set<T> a, final Set<T> b,
			final Comparator<? super T> comparator) {
		final Set<T> intersection = new TreeSet<T>(comparator);
		intersection.addAll(a);
		final Set<T> difference = getDifference(intersection, b, comparator);
		intersection.removeAll(difference);
		return intersection;
	}

	private static <T extends TableTemplateType> boolean isMatch(final T a, final T b) {
		return a.getMatch().matches(b.getMatch()) || b.getMatch().matches(a.getMatch());
	}

	private static <T> void printCollection(final Collection<T> collection, final PrintStream output) {
		for (final T item : collection) {
			output.println(item.toString());
		}
	}

	private Map<QualifiedName, Integer> getRowCountsFromFile(final File input) throws FileNotFoundException, PEException {
		final Map<QualifiedName, Integer> rowCounts = new HashMap<QualifiedName, Integer>();
		try (final Scanner fileScanner = new Scanner(input)) {
			boolean isHeaderLine = true;
			final Integer[] metaColumnIndices = { -1, -1, -1 };
			while (fileScanner.hasNextLine()) {
				final String line = fileScanner.nextLine();
				try (@SuppressWarnings("resource")
				final Scanner lineScanner = new Scanner(line).useDelimiter("\t")) {
					if (isHeaderLine) {
						int headerColumnCounter = 0;
						while (lineScanner.hasNext()) {
							final String columnHeader = lineScanner.next();
							if (columnHeader.equalsIgnoreCase("TABLE_SCHEMA")) {
								metaColumnIndices[0] = headerColumnCounter;
							} else if (columnHeader.equalsIgnoreCase("TABLE_NAME")) {
								metaColumnIndices[1] = headerColumnCounter;
							} else if (columnHeader.equalsIgnoreCase("TABLE_ROWS")) {
								metaColumnIndices[2] = headerColumnCounter;
							}
							++headerColumnCounter;
						}

						if (Collections.min(Arrays.asList(metaColumnIndices)) < 0) {
							throw new PEException("Could not find all required column ('TABLE_SCHEMA', 'TABLE_NAME' and 'TABLE_ROWS').");
						}

						isHeaderLine = false;
					} else {

						String databaseName = null;
						String tableName = null;
						int tableRowCount = 0;

						int columnCounter = 0;
						while (lineScanner.hasNext()) {
							if (columnCounter == metaColumnIndices[0]) {
								databaseName = lineScanner.next();
							} else if (columnCounter == metaColumnIndices[1]) {
								tableName = lineScanner.next();
							} else if (columnCounter == metaColumnIndices[2]) {
								try {
									tableRowCount = lineScanner.nextInt();
								} catch (final InputMismatchException e) {
									tableRowCount = 0; // Treat NULL and invalid count values as zero.
								}
							} else {
								lineScanner.next();
							}
							++columnCounter;
						}

						final QualifiedName qualifiedTableName = new QualifiedName(new UnqualifiedName(databaseName),
								new UnqualifiedName(tableName));
						rowCounts.put(qualifiedTableName, tableRowCount);
					}
				}
			}
		}

		return rowCounts;
	}

	private void loadSchema(final Analyzer analyzer) throws PEException {
		checkReport();
		checkDatabase();
		checkTemplate();

		for (final Database database : getSelectedDatabases()) {
			analyzer.loadSchema(checkTemplate(database.getName()), database, this.dbNative);
		}
	}

	private void loadCorpusStats(final Analyzer analyzer, final CorpusStats stats) throws PEException {
		checkReport();
		checkDatabase();

		analyzer.resolveSchemaObjects(getSelectedDatabases(), stats);
	}

	private List<Database> getSelectedDatabases() throws PEException {
		checkReport();
		checkDatabase();

		final List<Database> selectedDatabases = new ArrayList<Database>();
		for (final Database database : this.report.getDatabases().getDatabase()) {
			if (this.databases.contains(database.getName())) {
				selectedDatabases.add(database);
			}
		}

		return selectedDatabases;
	}

	private void generateAllBroadcastTemplates() throws PEException {
		reloadTemplates(AiTemplateBuilder.buildAllBroadcastTemplates(databases));
	}

	private void generateAllRandomTemplates() throws PEException {
		reloadTemplates(AiTemplateBuilder.buildAllRandomTemplates(databases));
	}

	private void generateBroadcastCutoffTemplates(final long broadcastCardinalityCutoff, final File baseTemplateFile) throws Exception {
		final CorpusStats corpusStats = new CorpusStats(
				EMPTY_FREQUENCY_CORPUS.getDescription(),
				this.options.getCorpusScaleFactor());
		final Template baseTemplate = (baseTemplateFile != null) ? PEXmlUtils.unmarshalJAXB(baseTemplateFile, Template.class) : null;
		try {

			/* Generate "safe" templates for schema loading. */
			generateAllBroadcastTemplates();

			final StatementAnalyzer analyzer = new StatementAnalyzer(this.options, corpusStats);
			loadSchema(analyzer);
			loadCorpusStats(analyzer, corpusStats);
			final TemplateModelItem fallbackModel = this.options.getGeneratorDefaultFallbackModel();
			final boolean isRowWidthWeightingEnabled = this.options.isRowWidthWeightingEnabled();
			final AiTemplateBuilder templateBuilder = new AiTemplateBuilder(corpusStats, baseTemplate, fallbackModel, this.getPrintStream());
			templateBuilder.setWildcardsEnabled(this.options.isTemplateWildcardsEnabled());
			templateBuilder.setVerbose(this.options.isVerboseGeneratorEnabled());
			templateBuilder.setForeignKeysAsJoins(this.options.isForeignKeysAsJoinsEnabled());
			templateBuilder.setUseIdentTuples(this.options.isIdentTuplesEnabled());

			reloadTemplates(templateBuilder.buildBroadcastCutoffTemplates(databases, broadcastCardinalityCutoff, isRowWidthWeightingEnabled));

		} catch (final Throwable e) {
			clearTemplates();
			throw new PEException("Unable to generate templates.", e);
		}
	}

	private void generateCorpusBasedTemplates(final Long broadcastCardinalityCutoff, final boolean followForeignKeys, final boolean isSafeMode,
			final File corpusFile, final File baseTemplateFile) throws Throwable {

		/* If there is no corpus file, use an empty corpus instead. */
		final DbAnalyzerCorpus corpus = (corpusFile != null) ? loadCorpus(corpusFile) : EMPTY_FREQUENCY_CORPUS;
		final Template baseTemplate = (baseTemplateFile != null) ? PEXmlUtils.unmarshalJAXB(baseTemplateFile, Template.class) : null;

		try {

			/* Generate initial "safe" templates for schema loading. */
			generateAllBroadcastTemplates();

			/* Analyze the static report. */
			final CorpusStats corpusStats = new CorpusStats(
					corpus.getDescription(),
					this.options.getCorpusScaleFactor());
			final StatementAnalyzer analyzer = new StatementAnalyzer(this.options, corpusStats);
			loadSchema(analyzer);
			loadCorpusStats(analyzer, corpusStats);

			/* Analyze the corpus. */
			try (final FrequenciesSource frequencies = new FrequenciesSource(corpus)) {
				frequencies.analyze(analyzer);
			}

			/*
			 * Build templates for selected databases.
			 * Use a user defined Broadcast cardinality cutoff or try to
			 * determine optimal distribution models automatically.
			 */
			final TemplateModelItem fallbackModel = this.options.getGeneratorDefaultFallbackModel();
			final boolean isRowWidthWeightingEnabled = this.options.isRowWidthWeightingEnabled();
			final AiTemplateBuilder ai = new AiTemplateBuilder(corpusStats, baseTemplate, fallbackModel, this.getPrintStream());
			ai.setWildcardsEnabled(this.options.isTemplateWildcardsEnabled());
			ai.setVerbose(this.options.isVerboseGeneratorEnabled());
			ai.setForeignKeysAsJoins(this.options.isForeignKeysAsJoinsEnabled());
			ai.setUseIdentTuples(this.options.isIdentTuplesEnabled());
			final List<Template> builtTemplates = ai.buildTemplates(this.databases, broadcastCardinalityCutoff, followForeignKeys, isSafeMode,
					isRowWidthWeightingEnabled);

			/* Load the generated templates while trashing the old ones. */
			reloadTemplates(builtTemplates);

		} catch (final Throwable e) {
			clearTemplates();
			throw new PEException("Unable to generate templates.", e);
		}

		/* Validate the generated templates by loading the schema. */
		println("... Validating the templates");

		try {
			final Emulator validator = new Emulator(this.options, new AnalyzerCallback() {
				@Override
				public void onResult(AnalyzerResult bar) {
					// Not needed.
				}
			});
			loadSchema(validator);
		} catch (final Exception e) {
			throw new PEException("Template validation has failed.", e);
		}
	}

	public static void main(String[] args) {
		try {
			new DVEAnalyzerCLI(args).start();
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}

}
