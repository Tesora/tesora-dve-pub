// OS_STATUS: public
package com.tesora.dve.resultset.collector;

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


import java.util.concurrent.atomic.AtomicLong;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultChunk;
import com.tesora.dve.resultset.ResultColumn;
import com.tesora.dve.resultset.ResultRow;

public class ResultCollectorStatic implements ResultCollector {

	static Logger logger = Logger.getLogger( ResultCollectorStatic.class );

	static AtomicLong nextId = new AtomicLong(0);
	
	 ColumnSet metaData;
	 boolean hasResults;
	 long rowCount = 0;
	 long updateCount = 0;
	 ResultChunk resultAggregator = null;

	 ResultCollectorStatic() throws PEException {
		 init(null, null);
	}
	 
	 ResultCollectorStatic(ColumnSet cs, ResultChunk rChunk) {
		 init(cs, rChunk);
	 }

	public ResultCollectorStatic(int rowCount, int updateCount) {
		init();
		this.rowCount = rowCount;
		this.updateCount = updateCount;
	}

	public ResultCollector init( ColumnSet cs, ResultChunk reschunk) {
		if ( cs == null || reschunk == null ) {
			// if either parameter is null, init as no result set and return
			return init();
		}
		this.metaData = cs;
		this.resultAggregator = reschunk;
		this.hasResults = true;	
		this.rowCount = reschunk.size();
		return this;
	}

	public ResultCollector init() {
		this.hasResults = false;
		this.rowCount = 0;
		resetResultAggregator();
		return this;
	}

	public void reset() throws PEException {
		metaData = null;
		hasResults = false;
		rowCount = 0;
		updateCount = 0;
	}
	
	@Override
	public void close() {};
	
	@Override
	public long getRowCount() {
		return rowCount;
	}

	@Override
	public long getUpdateCount() {
		return updateCount;
	}
	
	@Override
	public boolean hasResults() throws PEException {
		return hasResults;
	}

	@Override
	public ColumnSet getMetadata() {
		return metaData;
	}

	@Override
	public boolean noMoreData() throws PEException {
		return resultAggregator.size() == 0;
	}

	@Override
	public void printRows() throws PEException {
		StringBuffer line = new StringBuffer();
		for (ColumnMetadata cm : getMetadata().getColumnList()) {
			line.append(cm.getName()).append(", ");
		}
		logger.debug(line);
		line = new StringBuffer();
		while (!noMoreData()) {
			for (ResultRow row : getChunk().getRowList()) {
				for (ResultColumn col : row.getRow()) {
					line.append(col.getColumnValue().toString()).append(", ");
				}
				logger.debug(line);
				line = new StringBuffer();
			}
		}
	}

	 ResultChunk getResultAggregator() {
		if (resultAggregator == null)
			resetResultAggregator();
		return resultAggregator;
	}

	 void resetResultAggregator() {
         resultAggregator = new ResultChunk(Singletons.require(HostService.class).getProperties(), "ResultCollector");
	}

	@Override
	public ResultChunk getChunk() {
		ResultChunk chunk = getResultAggregator();
		resetResultAggregator();
		return chunk;
	}
	
	public void setUpdateCount(int n) {
		updateCount = n;
	}
}
