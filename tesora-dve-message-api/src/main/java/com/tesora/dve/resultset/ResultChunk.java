// OS_STATUS: public
package com.tesora.dve.resultset;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import com.tesora.dve.exceptions.PEException;

/**
 * Tracks a list of <code>ResultRow</code> objects representing data going to or
 * coming from a database. A <code>ResultChunk</code> can have a maximum size
 * (in bytes).
 * 
 */
public class ResultChunk implements Serializable {
	private static final long serialVersionUID = 1L;

	/**
	 * Constant for the default result chunk size in bytes
	 */
	private static final String RESULT_CHUNK_MAX_SIZE_DEFAULT = "50000";

	/**
	 * A List of ResultRow objects representing the chunk
	 */
	protected List<ResultRow> rowList;

	/**
	 * The column metadata associated with this chunk of data
	 */
	protected ColumnSet columnSet;
	
	/**
	 * The size of each ResultRow in bytes - not the size of the "Object" but
	 * the width of columns in aggregate
	 */
	protected long rowSize;

	/**
	 * The maximum size this ResultChunk is configured to be. Can be set 3 ways:
	 * - The default is specified by the constant RESULT_CHUNK_MAX_SIZE_DEFAULT
	 * - Retrieved from properties file with prefix.ResultChunkMaxSize key -
	 * Overriden using the setMaxChunkSize() method
	 * 
	 * 
	 */
	protected long maxChunkSize;

	/**
	 * Default Constructor - will not have metadata or any properties
	 */
	public ResultChunk() {
		this.init(null);
	};

	/**
	 * 
	 * @param cs
	 *            A <code>ColumnSet</code> object containing the metadata for
	 *            this chunk
	 * @param props
	 *            A <Property> object
	 * @param prefix
	 *            the prefix to be used when looking up items in props.
	 */
	public ResultChunk(ColumnSet cs, Properties props, String prefix) {
		this.init(cs);
		this.setMaxSize(props, prefix);
	}

	/**
	 * 
	 * @param props
	 *            A <Property> object
	 * @param prefix
	 *            the prefix to be used when looking up items in props.
	 */
	public ResultChunk(Properties props, String prefix) {
		setMaxSize(props, prefix);
		init(null);
	}

	public ResultChunk(List<ResultRow> rows) {
		setRowList(rows);
	}

	/*
	 * Builds a new ResultChunk comprised of the first <rowCount> rows
	 * from rowSource.  The returned rows are removed from <rowSource>.
	 */
	public ResultChunk(ResultChunk rowSource, int rowCount) {
		setRowList(rowSource.rowList.subList(0, rowCount));
		rowSource.rowList = rowSource.rowList.subList(rowCount,  rowSource.rowList.size());
	}

	void setMaxSize(Properties props, String prefix) {
		setMaxChunkSize(Long.parseLong(props.getProperty(prefix
				+ ".ResultChunkMaxSize", RESULT_CHUNK_MAX_SIZE_DEFAULT)));
	}

	private void init(ColumnSet cs) {
		if (cs != null) {
			this.rowSize = cs.calculateRowSize();
		} else {
			this.rowSize = 0;
		}
		this.rowList = new LinkedList<ResultRow>();
		this.columnSet = cs;
	}

	/**
	 * 
	 * @return long - the current max chunk size
	 */
	public long getMaxChunkSize() {
		return maxChunkSize;
	}

	/**
	 * set the max chunk size
	 * 
	 * @param maxChunkSize
	 */
	public void setMaxChunkSize(long maxChunkSize) {
		this.maxChunkSize = maxChunkSize;
	}

	public long getRowSize() {
		return rowSize;
	}

	public void setRowSize(long rowSize) {
		this.rowSize = rowSize;
	}

	/**
	 * Adds a <code>ResultRow</code> to the chunk
	 * 
	 * @param row
	 *            a <code>ResultRow</code> object
	 */
	public ResultChunk addResultRow(ResultRow row) {
		this.rowList.add(row);
		return this;
	}

	/**
	 * Returns the current chunk as a List of rows
	 * 
	 * @return List<ResultRow>
	 */
	public List<ResultRow> getRowList() {
		return this.rowList;
	}

	/**
	 * Returns a single ResultColumn from the ResultChunk
	 * 
	 * @param rowIndex - index of the row to get the column value from (this is 1 based)
	 * @param columnIndex - index of the column within the row (this is 1 based)
	 * @return ResultColumn
	 * @throws PEException
	 */
	public ResultColumn getSingleValue(int rowIndex, int columnIndex) throws PEException {
		int zeroBasedRow = rowIndex - 1;
		if ( rowIndex > size() )
			throw new PEException( "Row index of " + rowIndex + " is out of range. Result Chunk has " + size() + " rows");
		if ( columnIndex > columnSet.size() )
			throw new PEException( "Column index of " + columnIndex + " is out of range. Result Chunk has " + columnSet.size() + " columns");
		
		return rowList.get(zeroBasedRow).getResultColumn(columnIndex);
	}
	
	/**
	 * Set the current chunk. Note this will overwrite any rows already present
	 * in the ResultChunk.
	 * 
	 * @param chunk
	 *            LinkedList<ResultRow>
	 */
	public void setRowList(List<ResultRow> chunk) {
		this.rowList = chunk;
	}

	/**
	 * Adds a ResultChunk to the current chunk
	 * 
	 * @param chunk
	 *            ResultChunk
	 */
	public void addChunk(ResultChunk chunk) {
		this.rowSize = chunk.getRowSize();
		this.rowList.addAll(chunk.getRowList());
	}

	/**
	 * get the number of ResultRow objects in the current chunk
	 * 
	 * @return int
	 */
	public int size() {
		return rowList.size();
	}
	
	public ColumnSet getColumnSet() {
		return columnSet;
	}
	
	public void setColumnSet(ColumnSet columnSet) {
		this.columnSet = columnSet;
	}

	/**
	 * Returns whether or not the current chunk has space for another row.
	 * Calculated based on the row size provided by the ColumnSet and the
	 * current maxChunkSize
	 * 
	 * @return boolean
	 */
	public boolean hasSpace() {
		if (this.maxChunkSize == 0)
			return true; // no maximum size
		else
			return (this.maxChunkSize - ((this.size()+1) * this.rowSize)) > 0;
	}
	
	public void sort(Comparator<ResultRow> comparator) {
		Collections.sort(rowList, comparator);
	}

	@Override
	public String toString() {
		return new StringBuilder().append("ResultChunk{").append("# Rows=")
				.append(this.size()).append(", rowSize=").append(this.rowSize)
				.append(", hasSpace=").append(this.hasSpace())
				.append(", max chunk size=").append(this.maxChunkSize)
				.append("}").toString();

	}
}
