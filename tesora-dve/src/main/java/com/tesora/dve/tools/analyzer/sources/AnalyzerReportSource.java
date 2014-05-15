// OS_STATUS: public
package com.tesora.dve.tools.analyzer.sources;

import com.tesora.dve.sql.parser.ParserInvoker;
import com.tesora.dve.sql.parser.ParserInvoker.LineInfo;
import com.tesora.dve.tools.analyzer.Analyzer;
import com.tesora.dve.tools.analyzer.AnalyzerSource;
import com.tesora.dve.tools.analyzer.SourcePosition;
import com.tesora.dve.tools.analyzer.jaxb.DatabasesType.Database;
import com.tesora.dve.tools.analyzer.jaxb.DbAnalyzerReport;
import com.tesora.dve.tools.analyzer.jaxb.TablesType.Table;

public class AnalyzerReportSource extends AnalyzerSource {

	private final DbAnalyzerReport report;
	private final String description;

	public AnalyzerReportSource(final DbAnalyzerReport report) {
		this.report = report;
		this.description = "Schema metadata on '" + report.getAnalyzer().getConnection().getUrl() + "' using static report.";
	}

	@Override
	public void analyze(Analyzer a) throws Throwable {
		a.setSource(this);

		final ParserInvoker pi = a.getInvoker();
		int counter = 0;
		for (final Database staticReportDb : this.report.getDatabases().getDatabase()) {
			pi.parseOneLine(new LineInfo(++counter, null, 1), "USE " + staticReportDb.getName());
			for (final Table staticReportTable : staticReportDb.getTables().getTable()) {
				final String createStatement = staticReportTable.getScts();
				final LineInfo li = new LineInfo(++counter, null, 1);
				pi.parseOneLine(li, createStatement);
			}
		}

		a.onFinished();
		a.setSource(null);
	}

	@Override
	public void closeSource() {
		// noop
	}

	@Override
	public SourcePosition convert(LineInfo li) {
		return new SourcePosition(li);
	}

	@Override
	public String getDescription() {
		return this.description;
	}

}
