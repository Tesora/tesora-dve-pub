package com.tesora.dve.tools.analyzer;

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.PECharsetUtils;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.ParserException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.ParserInvoker;
import com.tesora.dve.sql.parser.ParserInvoker.LineInfo;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEForeignKey;
import com.tesora.dve.sql.schema.PEForeignKeyColumn;
import com.tesora.dve.sql.schema.PEKeyColumnBase;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.PETemplate;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.cache.SchemaSourceFactory;
import com.tesora.dve.sql.schema.validate.ValidateResult;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.session.UseDatabaseStatement;
import com.tesora.dve.sql.statement.session.UseStatement;
import com.tesora.dve.sql.statement.session.UseTenantStatement;
import com.tesora.dve.sql.template.jaxb.Template;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.tools.aitemplatebuilder.CorpusStats;
import com.tesora.dve.tools.analyzer.jaxb.DatabasesType.Database;
import com.tesora.dve.tools.analyzer.jaxb.DbAnalyzerReport;
import com.tesora.dve.tools.analyzer.jaxb.TablesType.Table;

public abstract class Analyzer {

	private static final int TRANSIENT_SITE_NUM = 2;
	private static final String TRANSIENT_SITE_USER = "root";
	private static final String TRANSIENT_SITE_PASS = "password";

	protected final TransientExecutionEngine tee;
	private final AnalyzerOptions options;
	protected final AnalyzerInvoker invoker;
	protected AnalyzerSource currentSource;
	protected String primaryDB;

	protected DbAnalyzerReport parentReport;

	public Analyzer(AnalyzerOptions opts) throws Throwable {
		if (opts == null) {
			throw new IllegalArgumentException();
		}
		SchemaSourceFactory.reset();
		// Build at least 2-site storage group so we guarantee we require redistribution.
		this.tee = buildExecutionEngine(TRANSIENT_SITE_NUM);
		this.options = opts;
		this.invoker = new AnalyzerInvoker(this);
		this.currentSource = null;
	}

	public void loadSchema(Template template, Database db, DBNative dbNative) throws PEException {
		final List<String> tableCreates = AnalyzerUtils.buildCreateTableStatements(db, dbNative);
		// Build view statements separately as they can depend on each other.
		final List<String> viewCreates = AnalyzerUtils.buildCreateViewStatements(db, dbNative);
		final List<String> tableNames = AnalyzerUtils.getTableNames(db);

		primaryDB = db.getName();

		try {

			final List<String> dbload = new ArrayList<String>();
			dbload.add("create template " + template.getName() + " xml='" + PETemplate.build(template) + "'");
			dbload.add("create database " + primaryDB + " default persistent group g1 using template " + primaryDB + " strict");
			dbload.add("use " + primaryDB);
			dbload.add("set foreign_key_checks=0");
			dbload.addAll(tableCreates);
			dbload.add("set foreign_key_checks=1");

			tee.parse(dbload.toArray(new String[dbload.size()]), true);
			final ParserOptions opts = ParserOptions.TEST.setResolve().setIgnoreMissingUser();
			while (!viewCreates.isEmpty()) {
				final int before = viewCreates.size();
				for (final Iterator<String> iter = viewCreates.iterator(); iter.hasNext();) {
					final String sql = iter.next();
					final SchemaContext pc = tee.getPersistenceContext();
					pc.refresh(true);
					try {
						final List<Statement> stmts = InvokeParser.parse(InvokeParser.buildInputState(sql, pc), opts, pc).getStatements();
						for (final Statement s : stmts) {
							tee.dispatch(s);
						}
						iter.remove();
					} catch (final SchemaException e) {
						if (e.getMessage().startsWith("No such table")) {
							continue;
						}

						throw new PEException("TEE: unable to parse '" + sql + "': " + e.getMessage(), e);
					} catch (final Exception e) {
						throw new PEException("TEE: unable to parse '" + sql + "': " + e.getMessage(), e);
					}
				}
				final int after = viewCreates.size();
				if (after == before) {
					throw new PEException("TEE: unable to load views. Circular reference?");
				}
			}
			tee.getPersistenceContext().forceMutableSource();

			resolveDanglingFKs(db);
			if (options.isValidateFKsEnabled()) {
				validateFKs(primaryDB, tableNames);
			}

		} catch (final Throwable t) {
			throw new PEException("Unable to load schema '" + t.getMessage() + "'", t);
		}
	}

	private void resolveDanglingFKs(final Database db) throws PEException {
		final SchemaContext context = tee.getPersistenceContext();

		final UnqualifiedName dbName = new UnqualifiedName(db.getName());
		final PEDatabase peDb = context.findPEDatabase(dbName);

		if (peDb != null) {
			for (final Table table : db.getTables().getTable()) {
				if (table.isView() != Boolean.TRUE) {
					final UnqualifiedName tableName = new UnqualifiedName(table.getName());
					final TableInstance ti = peDb.getSchema().buildInstance(context, tableName, null);
					if (ti != null) {
						final PETable peTable = ti.getAbstractTable().asTable();
						final List<PEForeignKey> fks = peTable.getForeignKeys(context);
						for (final PEForeignKey fk : fks) {
							if (fk.isForward() && (fk.getTargetTable(context) == null)) {
								final Name targetTableName = fk.getTargetTableName(context);
								final TableInstance tti = peDb.getSchema().buildInstance(context, targetTableName.getUnqualified(), null);
								if (tti != null) {
									final PETable targetTable = tti.getAbstractTable().asTable();
									fk.setTargetTable(context, targetTable);
									for (final PEKeyColumnBase keyColumn : fk.getKeyColumns()) {
										final PEForeignKeyColumn fkColumn = (PEForeignKeyColumn) keyColumn;
										final Name targetColumnName = fkColumn.getTargetColumnName();
										final PEColumn targetColumn = targetTable.lookup(context, targetColumnName);
										if (targetColumn != null) {
											fkColumn.setTargetColumn(targetColumn);
										} else {
											final QualifiedName fullColumnName = new QualifiedName(targetTableName.getUnqualified(),
													targetColumnName.getUnqualified());
											throw new PEException("TEE: target column '" + fullColumnName.getSQL() + "' not found in the schema");
										}
									}
								} else {
									final QualifiedName fullTableName = new QualifiedName(dbName, targetTableName.getUnqualified());
									throw new PEException("TEE: target table '" + fullTableName.getSQL() + "' not found in the schema");
								}
							}
						}
					} else {
						final QualifiedName fullTableName = new QualifiedName(dbName, tableName);
						throw new PEException("TEE: table '" + fullTableName.getSQL() + "' does not exist");
					}
				}
			}
		} else {
			throw new PEException("TEE: database '" + dbName.getSQL() + "' does not exist");
		}
	}

	private void validateFKs(final String dbName, List<String> tableNames) {
		final SchemaContext sc = tee.getPersistenceContext();
		final PEDatabase db = sc.findPEDatabase(new UnqualifiedName(dbName));
		if (db == null) {
			return;
		}
		final ArrayList<ValidateResult> results = new ArrayList<ValidateResult>();
		for (final String tn : tableNames) {
			final TableInstance ti = db.getSchema().buildInstance(sc, new UnqualifiedName(tn), null);
			if (ti == null) {
				continue;
			}
			final PEAbstractTable<?> pet = ti.getAbstractTable();
			if (pet.isView()) {
				continue;
			}
			final List<PEForeignKey> fks = pet.asTable().getForeignKeys(sc);
			for (final PEForeignKey pefk : fks) {
				results.addAll(pefk.validate(sc, false));
			}
		}
		if (results.isEmpty()) {
			return;
		}

		final StringBuilder errorMessage = new StringBuilder();
		for (final ValidateResult vr : results) {
			errorMessage.append(vr.getMessage(sc)).append(PEConstants.LINE_SEPARATOR);
		}
		throw new SchemaException(Pass.PLANNER, errorMessage.toString());
	}

	public void resolveSchemaObjects(final List<Database> databases, final CorpusStats corpusStats) {
		final SchemaContext context = tee.getPersistenceContext();

		/* Load all tables from given static report databases. */
		for (final Database staticReportDb : databases) {
			final String databaseName = staticReportDb.getName();
			for (final Table staticReportTable : staticReportDb.getTables().getTable()) {
				final String tableName = staticReportTable.getName();
				final int tableRowCount = staticReportTable.getRowCount();
				final Long tableDataLength = staticReportTable.getDataLength();
				final String engine = staticReportTable.getEngine();
				corpusStats.addTable(corpusStats.new TableStats(databaseName,
						tableName, tableRowCount, tableDataLength, engine));
			}
		}

		/* Resolve additional information if available. */
		for (final CorpusStats.TableStats table : corpusStats.getStatistics()) {
			final UnqualifiedName databaseName = new UnqualifiedName(table.getSchemaName());
			final UnqualifiedName tableName = new UnqualifiedName(table.getTableName());
			final PEDatabase peDb = context.findPEDatabase(databaseName);
			if (peDb != null) {
				// TODO: handle views in the analyzer
				final TableInstance ti = peDb.getSchema().buildInstance(context, tableName, null);
				final PEAbstractTable<?> tabular = ti.getAbstractTable();
				if (tabular.isView() != Boolean.TRUE) {
					final PETable peTable = tabular.asTable();
					if (peTable != null) {
						corpusStats.resolveTableColumns(peTable, context);
						corpusStats.resolveTableForeignKeys(peTable, context);
					}
				}
			}
		}
	}

	public abstract void onStatement(String sql, SourcePosition sp, Statement s) throws Throwable;

	public abstract void onException(String sql, SourcePosition sp, Throwable t);

	public abstract void onNotice(String sql, SourcePosition sp, String message);

	public SourcePosition convert(LineInfo li, AnalyzerSource src) {
		if (src != null) {
			return src.convert(li);
		}

		return new SourcePosition(li);
	}

	private TransientExecutionEngine buildExecutionEngine(final int numPersistentSites) throws Throwable {
		final TransientExecutionEngine engine = new TransientExecutionEngine("atemp");

		final List<String> persistentSiteNames = new ArrayList<String>();
		final List<String> persistentDeclarations = new ArrayList<String>();
		for (int i = 1; i <= numPersistentSites; ++i) {
			final String siteName = "site" + i;
			final String siteHostUrl = "jdbc:mysql://s" + i + "/db" + i;
			persistentDeclarations.add("create persistent site " + siteName + " url='" + siteHostUrl + "' user='" + TRANSIENT_SITE_USER + "' password='"
					+ TRANSIENT_SITE_PASS + "'");
			persistentSiteNames.add(siteName);
		}
		persistentDeclarations.add("create persistent group g1 add " + StringUtils.join(persistentSiteNames, ','));

		engine.parse(persistentDeclarations.toArray(new String[] {}));

		return engine;
	}

	public void refresh() {
		tee.getPersistenceContext().refresh(true);
	}

	public AnalyzerOptions getOptions() {
		return options;
	}

	public ParserInvoker getInvoker() {
		return invoker;
	}

	public void setSource(AnalyzerSource as) {
		currentSource = as;
	}

	public AnalyzerSource getCurrentSource() {
		return currentSource;
	}

	public String getPrimaryDatabase() {
		return primaryDB;
	}

	@SuppressWarnings("unused")
	public void onFinished() throws PEException {
		// does nothing by default
	}

	protected static class AnalyzerInvoker extends ParserInvoker {

		protected Analyzer sink;
		private final Map<Integer, String> cdb;
		private Integer lastConn;

		public AnalyzerInvoker(Analyzer a) {
			super(ParserOptions.NONE);
			sink = a;
			cdb = new HashMap<Integer, String>();
			lastConn = null;
		}

		@SuppressWarnings("unused")
		public boolean omit(LineInfo info, String line) {
			return false;
		}

		@Override
		public String parseOneLine(LineInfo info, String line) throws Throwable {
			final String trimmed = line.trim();
			if (trimmed.equalsIgnoreCase("Connect")) {
				return line;
			}
			if (trimmed.equalsIgnoreCase("Quit")) {
				cdb.remove(info.getConnectionID());
				return line;
			}
			if ((lastConn != null) && (lastConn.intValue() != info.getConnectionID())) {
				// we have to execute a use first
				final String db = cdb.get(info.getConnectionID());
				if (db != null) {
					sink.tee.parse(new String[] { "use " + db });
				}
			}
			lastConn = info.getConnectionID();

			if (omit(info, line)) {
				return line;
			}

			final byte[] bytes = PECharsetUtils.getBytes(line, PECharsetUtils.ISO_8859_1);

			sink.refresh();
			try {
				final List<Statement> stmts = InvokeParser.parse(bytes, sink.tee.getPersistenceContext(), PECharsetUtils.ISO_8859_1).getStatements();
				for (final Statement s : stmts) {
					if (s instanceof UseStatement) {
						if (s instanceof UseDatabaseStatement) {
							final UseDatabaseStatement us = (UseDatabaseStatement) s;
							cdb.put(info.getConnectionID(), us.getDatabase(sink.tee.getPersistenceContext()).getName().get());
							sink.tee.dispatch(s);
						} else if (s instanceof UseTenantStatement) {
							final UseTenantStatement us = (UseTenantStatement) s;
							cdb.put(info.getConnectionID(), us.getTenant().getExternalID());
							sink.tee.dispatch(s);
						}
					}
					sink.onStatement(line, sink.convert(info, sink.getCurrentSource()), s);
				}
			} catch (final ParserException se) {
				sink.onException(line, sink.convert(info, sink.getCurrentSource()), se);
			}
			return line;
		}

	}
}
