// OS_STATUS: public
package com.tesora.dve.tools.analyzer;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.tools.analyzer.jaxb.AnalyzerType;
import com.tesora.dve.tools.analyzer.jaxb.AnalyzerType.Options.Option;
import com.tesora.dve.tools.analyzer.jaxb.ColumnsType;
import com.tesora.dve.tools.analyzer.jaxb.ColumnsType.Column;
import com.tesora.dve.tools.analyzer.jaxb.DatabaseInformationType;
import com.tesora.dve.tools.analyzer.jaxb.DatabasesType;
import com.tesora.dve.tools.analyzer.jaxb.DatabasesType.Database;
import com.tesora.dve.tools.analyzer.jaxb.DbAnalyzerReport;
import com.tesora.dve.tools.analyzer.jaxb.IndexType;
import com.tesora.dve.tools.analyzer.jaxb.IndexesType;
import com.tesora.dve.tools.analyzer.jaxb.IndexesType.Index;
import com.tesora.dve.tools.analyzer.jaxb.KeysType;
import com.tesora.dve.tools.analyzer.jaxb.KeysType.ForeignKey;
import com.tesora.dve.tools.analyzer.jaxb.KeysType.PrimaryKey;
import com.tesora.dve.tools.analyzer.jaxb.ProceduresType;
import com.tesora.dve.tools.analyzer.jaxb.ProceduresType.Procedure;
import com.tesora.dve.tools.analyzer.jaxb.TablesType;
import com.tesora.dve.tools.analyzer.jaxb.TablesType.Table;

public class StaticAnalyzer {

	public static final String PROCEDURE_NAME = "PROCEDURE_NAME";
	public static final String TABLE_CAT = "TABLE_CAT";
	public static final String TABLE_NAME = "TABLE_NAME";
	public static final String TABLE_TYPE = "TABLE_TYPE";
	public static final String COLUMN_NAME = "COLUMN_NAME";
	public static final String TYPE_NAME = "TYPE_NAME";
	public static final String COLUMN_SIZE = "COLUMN_SIZE";
	public static final String COLUMN_DEF = "COLUMN_DEF";
	public static final String NULLABLE = "NULLABLE";
	public static final String IS_AUTOINCREMENT = "IS_AUTOINCREMENT";
	public static final String PKTABLE_NAME = "PKTABLE_NAME";
	public static final String PKCOLUMN_NAME = "PKCOLUMN_NAME";
	public static final String FKCOLUMN_NAME = "FKCOLUMN_NAME";
	public static final String KEY_SEQ = "KEY_SEQ";
	public static final String PK_NAME = "PK_NAME";
	public static final String FK_NAME = "FK_NAME";
	public static final String INDEX_NAME = "INDEX_NAME";
	public static final String ORDINAL_POSITION = "ORDINAL_POSITION";
	public static final String ASC_OR_DESC = "ASC_OR_DESC";
	public static final String NON_UNIQUE = "NON_UNIQUE";
	public static final String CARDINALITY = "CARDINALITY";
	public static final String INDEX_TYPE = "TYPE";

	public static DbAnalyzerReport doStatic(final DBNative dbNative, final AnalyzerOptions options, final String dbUrl, final String dbUser,
			final String dbPassword, final List<String> forDatabases, final DBHelper dbHelper) throws PEException, SQLException {

		final DbAnalyzerReport dbaReport = setup(options, new DbAnalyzerReport(), dbUrl, dbUser, dbPassword);

		final DatabaseMetaData dbmd = dbHelper.getConnection().getMetaData();

		dbaReport.getDatabaseInformation().setMajorVersion(dbmd.getDatabaseMajorVersion());
		dbaReport.getDatabaseInformation().setMinorVersion(dbmd.getDatabaseMinorVersion());
		dbaReport.getDatabaseInformation().setProductName(dbmd.getDatabaseProductName());
		dbaReport.getDatabaseInformation().setProductVersion(dbmd.getDatabaseProductVersion());
		dbaReport.getDatabaseInformation().setDefaultTxnIsolation(
				isolationLevelToString(dbmd.getDefaultTransactionIsolation()));

		final List<Database> localDatabases = dbaReport.getDatabases().getDatabase();

		for (final String dbName : forDatabases) {
			// Check that the specified database is actually present
			boolean found = false;
			try (final ResultSet rs = dbmd.getCatalogs()) {
				while (rs.next()) {
					if (rs.getString(TABLE_CAT).equals(dbName)) {
						found = true;
						break;
					}
				}
			}
			if (!found) {
				throw new PEException("Database '" + dbName + "' not found");
			}

			localDatabases.add(setup(new Database(), dbName));
		}

		for (final Database database : localDatabases) {
			final String dbName = database.getName();

			// Handle stored procedures
			try (final ResultSet rs = dbmd.getProcedures(dbName, null, null)) {
				while (rs.next()) {
					final Procedure procedure = new Procedure();
					procedure.setName(rs.getString(PROCEDURE_NAME));

					if (database.getProcedures() == null) {
						database.setProcedures(new ProceduresType());
					}
					database.getProcedures().getProcedure().add(procedure);
				}
			}

			final List<Table> tables = database.getTables().getTable();

			// Obtain list of all tables for this database
			try (final ResultSet rs = dbmd.getTables(dbName, null, null, new String[] { "TABLE", "VIEW" })) {
				while (rs.next()) {
					tables.add(setup(new Table(), rs.getString(TABLE_NAME), rs.getString(TABLE_TYPE)));
				}
			}

			dbHelper.executeQuery("USE " + dbName);

			// Iterate over all the tables
			for (final Table table : tables) {
				final String tableName = table.getName();

				// Get count of rows in this table
				if (!table.isView()) {
					dbHelper.executeQuery("SELECT COUNT(*) FROM " + dbNative.quoteIdentifier(tableName));
					try (final ResultSet rs = dbHelper.getResultSet()) {
						rs.next();
						table.setRowCount(rs.getInt(1));
					}
				}
				dbHelper.executeQuery("SHOW CREATE TABLE " + dbNative.quoteIdentifier(tableName));
				try (final ResultSet rs = dbHelper.getResultSet()) {
					rs.next();
					table.setScts(rs.getString(2));
				}

				// Read column metadata for the table
				try (final ResultSet rs = dbmd.getColumns(dbName, null, tableName, null)) {
					final List<Column> columns = table.getColumns().getColumn();
					while (rs.next()) {
						final Column column = new Column();
						column.setName(rs.getString(COLUMN_NAME));
						column.setType(rs.getString(TYPE_NAME));
						column.setSize(rs.getInt(COLUMN_SIZE));
						column.setDefVal(rs.getString(COLUMN_DEF));
						if (DatabaseMetaData.columnNoNulls == rs.getInt(NULLABLE)) {
							column.setNullable(false);
						}
						if (rs.getString(IS_AUTOINCREMENT).equalsIgnoreCase("yes")) {
							column.setAutoIncr(true);
						}

						columns.add(column);
					}
				}

				// Get indexes
				try (final ResultSet rs = dbmd.getIndexInfo(dbName, null, tableName, false, false)) {
					while (rs.next()) {
						final Index index = new Index();
						index.setName(rs.getString(INDEX_NAME));
						index.setSequence(rs.getInt(ORDINAL_POSITION));
						index.setColumn(rs.getString(COLUMN_NAME));
						index.setNonUnique(rs.getBoolean(NON_UNIQUE));
						index.setAscending(rs.getString(ASC_OR_DESC) != null ? rs.getString(ASC_OR_DESC).equalsIgnoreCase(
								"A") : true);
						index.setCardinality(rs.getInt(CARDINALITY));
						switch (rs.getInt(INDEX_TYPE)) {
						case DatabaseMetaData.tableIndexStatistic:
							index.setType(IndexType.STATISTIC);
							break;
						case DatabaseMetaData.tableIndexClustered:
							index.setType(IndexType.CLUSTERED);
							break;
						case DatabaseMetaData.tableIndexHashed:
							index.setType(IndexType.HASHED);
							break;
						default:
							index.setType(IndexType.OTHER);
							break;
						}

						if (table.getIndexes() == null) {
							table.setIndexes(new IndexesType());
						}

						table.getIndexes().getIndex().add(index);
					}
				}

				// Get primary keys
				try (final ResultSet rs = dbmd.getPrimaryKeys(dbName, null, tableName)) {
					while (rs.next()) {
						final PrimaryKey key = new PrimaryKey();
						key.setColumn(rs.getString(COLUMN_NAME));
						key.setName(rs.getString(PK_NAME));
						key.setSequence(rs.getInt(KEY_SEQ));

						if (table.getKeys() == null) {
							table.setKeys(new KeysType());
						}

						table.getKeys().getPrimaryKey().add(key);
					}
				}

				// Get Foreign Keys
				try (final ResultSet rs = dbmd.getImportedKeys(dbName, null, tableName)) {
					while (rs.next()) {
						final ForeignKey key = new ForeignKey();
						key.setRefTable(rs.getString(PKTABLE_NAME));
						key.setRefColumn(rs.getString(PKCOLUMN_NAME));
						key.setColumn(rs.getString(FKCOLUMN_NAME));
						key.setSequence(rs.getInt(KEY_SEQ));
						key.setName(rs.getString(FK_NAME));

						if (table.getKeys() == null) {
							table.setKeys(new KeysType());
						}

						table.getKeys().getForeignKey().add(key);
					}
				}
			}
			database.setTableCount(tables.size());
		}

		return dbaReport;
	}

	private static Database setup(Database db, String name) {
		db.setName(name);
		db.setTables(new TablesType());
		return db;
	}

	private static Table setup(Table table, String name, String type) {
		table.setName(name);
		table.setColumns(new ColumnsType());
		if ("TABLE".equals(type)) {
			table.setView(false);
		} else if ("VIEW".equals(type)) {
			table.setView(true);
		}
		return table;
	}

	private static String isolationLevelToString(int isolationLevel) {
		switch (isolationLevel) {
		case Connection.TRANSACTION_NONE:
			return "none";
		case Connection.TRANSACTION_READ_COMMITTED:
			return "read_committed";
		case Connection.TRANSACTION_READ_UNCOMMITTED:
			return "read_uncommitted";
		case Connection.TRANSACTION_REPEATABLE_READ:
			return "repeatable_read";
		case Connection.TRANSACTION_SERIALIZABLE:
			return "serializable";
		default:
			return "unknown";
		}
	}

	private static DbAnalyzerReport setup(final AnalyzerOptions options, final DbAnalyzerReport report, final String dbUrl, final String dbUser,
			final String dbPassword) {
		report.setDatabaseInformation(new DatabaseInformationType());
		report.setDatabases(new DatabasesType());

		final AnalyzerType.Connection c = new AnalyzerType.Connection();
		c.setUrl(dbUrl);
		c.setUser(dbUser);
		c.setPassword(dbPassword);

		final AnalyzerType.Options os = new AnalyzerType.Options();
		final List<Option> lo = os.getOption();
		for (final AnalyzerOption option : options.getOptions()) {
			final AnalyzerType.Options.Option o = new AnalyzerType.Options.Option();
			o.setKey(option.getName());
			o.setValue(option.getCurrentValue().toString());
			lo.add(o);
		}

		final AnalyzerType at = new AnalyzerType();
		at.setConnection(c);
		at.setOptions(os);
		report.setAnalyzer(at);

		return report;
	}

}
