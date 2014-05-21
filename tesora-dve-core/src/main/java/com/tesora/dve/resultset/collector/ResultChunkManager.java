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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.resultset.ResultChunk;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.messaging.SQLCommand;

/**
 * Implements server side handling of the <code>ResultChunk</code> object.
 * Provided with a ResultSet, will return the result set in ResultChunks based
 * on the space available in the <code>ResultChunk</code> Also, returns the
 * metadata in <code>ColumnSet</code>.
 * 
 */
public class ResultChunkManager {

	/**
	 * This enum is used to ensure that the Next and Get methods are executed in
	 * the proper order
	 */
	private enum chunkMgrState {
		Initialized, Next, Get;
	}

	/**
	 * The ResultSet passed in via the constructor
	 */
	private ResultSet resultSet;

	/**
	 * The <code>ColumnSet</code> generated from the ResultSetMetaData. Is only
	 * generated once per instantiation of this object.
	 */
	private ColumnSet rsColumnSet = null;

	/**
	 * The ResultSetMetaData returned from the JDBC driver
	 */
	private ResultSetMetaData rsmd;

	/**
	 * The next <code>ResultChunk</code> available to the caller that was
	 * retrieved via .nextChunk()
	 */
	private ResultChunk nextRC;

	/**
	 * Tracks the state of interactions with the manager. State will be cycled
	 * between Next and Get as nextChunk() and getChunk() are called. State must
	 * be Next in order to call getChunk()
	 */
	private chunkMgrState chunkState;

	/**
	 * Properties object and prefix passed into constructor
	 */
	private Properties props;
	private String prefix;

	/**
	 * Projection metadata overrides
	 */
	private ProjectionInfo projectionMetadata = null;
	
	/**
	 * 
	 * @param rs
	 *            - <code>ResultSet</code> object returned from JDBC
	 *            <code>Statement.execute()</code>
	 * @param props
	 *            - <code>Property</code> object containing any
	 *            <code>ResultChunk</code> properties
	 * @param prefix
	 *            - String containing prefix to use to lookup properties in
	 *            <code>props</code>
	 * @throws SQLException
	 *             - thrown if there is a problem retrieving metadata from
	 *             <code>ResultSet</code>
	 */
	public ResultChunkManager(ResultSet rs, Properties props, String prefix, SQLCommand sqlCommand)
			throws SQLException {
		this.resultSet = rs;
		this.props = props;
		this.prefix = prefix;

		this.rsmd = this.resultSet.getMetaData();
		this.chunkState = chunkMgrState.Initialized;
		
		this.projectionMetadata = sqlCommand.getProjection();
	}
	
	/**
	 * Used to determine if a <code>ResultChunk</code> is available and if so,
	 * makes it available to the next call to <code>getChunk</code>.
	 * 
	 * @return <code>true</code> if the new current chunk is valid;
	 *         <code>false</code> if there are no more chunks
	 * @throws SQLException
	 */
	public boolean nextChunk() throws SQLException {
		this.chunkState = chunkMgrState.Next;

		// allocate a new ResultChunk
		nextRC = new ResultChunk(this.getMetaData(), this.props, this.prefix);

		// if the ResultChunk has available space and there is a row in the
		// resultSet, add it to the chunk
		while (nextRC.hasSpace() && this.resultSet.next()) {
			nextRC.addResultRow(createResultRow());
		}

		// handle the case where not even one row would fit in the chunk
		// no matter what the max size of the chunk is we will always put one
		// row in it
		if (nextRC.size() == 0) {
			if (this.resultSet.next())
				nextRC.addResultRow(createResultRow());
		}

		return nextRC.size() > 0;
	}

	/**
	 * If a <code>ResultChunk</code> was made available via
	 * <code>nextChunk</code>, return it to the caller
	 * 
	 * @return the next <code>ResultChunk</code>
	 * @throws PEException 
	 *             <code>nextChunk</code> must be called before calling
	 *             <code>getChunk</code>
	 * 
	 */
	public ResultChunk getChunk() throws PEException  {
		if (this.chunkState != chunkMgrState.Next) {
			throw new PEException(
					"ResultChunkManager.nextChunk must be called before getChunk");
		}
		this.chunkState = chunkMgrState.Get;

		return nextRC;
	}
	
	private ResultRow createResultRow() throws SQLException {
		ResultRow newRR = new ResultRow();
		for (int colIdx = 1; colIdx <= this.rsmd.getColumnCount(); colIdx++) {
			newRR.addResultColumn(this.resultSet.getObject(colIdx));
		}
		return newRR;
	}

	/**
	 * Returns the metadata associated with the chunk in a
	 * <code>ColumnSet</code>
	 * 
	 * @return <code>ColumnSet</code> representing the result set metadata
	 * @throws SQLException
	 *             as returned from JDBC <code>getMetaData()</code>
	 */
	public ColumnSet getMetaData() throws SQLException {
		// rsColumnSet object is only created once per ResultChunkManager
		// instantiation
		if (rsColumnSet == null) 
			rsColumnSet = MetadataUtils.buildColumnSet(rsmd, projectionMetadata); 

		return this.rsColumnSet;
	}
	
	/**
	 * @throws SQLException 
	 * Closes the underlying ResultSet instance.
	 * 
	 * @throws SQLException as per <code>ResultSet.close()</code>
	 */
	public void close() throws SQLException {
		resultSet.close();
	}

}
