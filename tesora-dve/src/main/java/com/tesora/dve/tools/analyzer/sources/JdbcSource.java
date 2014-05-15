// OS_STATUS: public
package com.tesora.dve.tools.analyzer.sources;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.tesora.dve.sql.parser.ParserInvoker;
import com.tesora.dve.sql.parser.ParserInvoker.LineInfo;
import com.tesora.dve.tools.analyzer.Analyzer;
import com.tesora.dve.tools.analyzer.AnalyzerSource;
import com.tesora.dve.tools.analyzer.SourcePosition;

public class JdbcSource extends AnalyzerSource {

	private final Connection connection;
	private final String tblName;

	private final String descr;

	public JdbcSource(String url, String username, String password, String tblName) throws Throwable {
		final Properties props = new Properties();
		props.put("user", username);
		props.put("password", password);
		connection = DriverManager.getConnection(url, props);
		this.tblName = tblName;
		this.descr = "JdbcSource using URL:" + url + " Table:" + tblName;
	}

	@Override
	public String getDescription() {
		return descr;
	}

	@Override
	public void analyze(Analyzer analyzer) throws Throwable {
		analyzer.setSource(this);
		final ParserInvoker invoker = analyzer.getInvoker();

		final boolean isRdsFormat = analyzer.getOptions().isRdsFormat();

		// TODO this falls over if there is more than one server, leave that alone for now
		final String sql = "select user_host, command_type, thread_id, argument from " + tblName;

		try (final Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			stmt.setFetchSize(Integer.MIN_VALUE);
			if (stmt.execute(sql)) {
				try (final ResultSet rs = stmt.getResultSet()) {
					int rowcount = 0;
					while (rs.next()) {
						final String userHost = rs.getString(1);
						final String ctype = rs.getString(2);
						final int tid = rs.getInt(3);
						String logSQL = rs.getString(4);
						if (isRdsFormat) {
							// if we are in RDS mode, skip any statements coming from localhost
							if (userHost.contains("localhost")) {
								continue;
							}
						}
						if (ctype.equals("Connect") || ctype.equals("Init DB")) {
							logSQL = "use " + analyzer.getPrimaryDatabase();
						} else if (ctype.equals("Quit")) {
							logSQL = ctype;
						}
						rowcount++;
						invoker.parseOneLine(new LineInfo(rowcount, null, tid), logSQL);
					}
					analyzer.onFinished();
					analyzer.setSource(null);
					System.out.println("Processed " + rowcount + " rows");
				}
			}
		}
	}

	@Override
	public void closeSource() throws SQLException {
		connection.close();
	}

	@Override
	public SourcePosition convert(LineInfo li) {
		return new SourcePosition(li);
	}

}
