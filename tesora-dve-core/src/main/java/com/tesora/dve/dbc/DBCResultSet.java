// OS_STATUS: public
package com.tesora.dve.dbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import com.tesora.dve.db.NativeResultHandler;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.worker.MysqlTextResultChunkProvider;

public class DBCResultSet implements ResultSet {

	private DBCResultSetMetaData rsMetaData;
	private boolean isClosed = false;
	protected NativeResultHandler resultHandler;
	protected Map<String, Integer> colNameLookup;
	protected SQLWarning warningChain = null;

	protected boolean wasNull=false;
	
	MysqlTextResultChunkProvider resultConsumer = null;
	int rowIndex = 0;

	public DBCResultSet(MysqlTextResultChunkProvider resultConsumer, ServerDBConnection conn) {
		this.resultConsumer = resultConsumer;
		this.rsMetaData = new DBCResultSetMetaData(conn, resultConsumer.getColumnSet());
	}

	protected void initialize() {
		rsMetaData = null;
	}

	protected void checkIfClosed() throws SQLException {
		if ( isClosed() ) {
			throw new SQLException("Result Set is closed and can't used");
		}
	}
	
	protected int getColumnIndexFromName(String columnName) throws SQLException {
		// TODO using Locale.ENGLISH for column names could be problematic 
		if (colNameLookup == null) {
			// lazy load colNameLookup map
			colNameLookup = new HashMap<String, Integer>(rsMetaData.getColumnCount());
			for (int index = rsMetaData.getColumnCount(); index > 0; index--) {
				colNameLookup.put(rsMetaData.getColumnName(index).toLowerCase(Locale.ENGLISH), index);
			}
		}

		Integer colIndex = colNameLookup.get(columnName.toLowerCase(Locale.ENGLISH));
		if (colIndex == null) {
			throw new SQLException("The column named " + columnName + " was not found in this ResultSet");
		}
		return colIndex.intValue();
	}

	@Override
	public boolean next() throws SQLException {
		checkIfClosed();

		rowIndex++;

		return resultConsumer.getResultChunk().getRowList().size()>=rowIndex;
	}

	@Override
	public void close() throws SQLException {
		if ( isClosed() ) return;

		colNameLookup = null;
		isClosed = true;
	}

	@Override
	public boolean wasNull() throws SQLException {
		return wasNull;
	}

	@Override
	public String getString(String columnName) throws SQLException {
		return this.getString(this.findColumn(columnName));
	}


	@Override
	public String getString(int columnIndex) throws SQLException {
		try {
			return resultConsumer.getResultChunk().getSingleValue(rowIndex, columnIndex).getColumnValue().toString();
		} catch (PEException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public long getLong(int index) throws SQLException {
		try {
			wasNull = resultConsumer.getResultChunk().getSingleValue(rowIndex, index).isNull();
			return (this.wasNull ? 0 : getNativeLong(index));
		} catch (PEException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public long getLong(String columnName) throws SQLException {
		return this.getLong(this.findColumn(columnName));
	}

	long getNativeLong(int index) throws PEException {
		return (Long) resultConsumer.getResultChunk().getSingleValue(rowIndex, index).getColumnValue();
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		checkIfClosed();
		
		if ( rsMetaData == null ) 
			throw new SQLException("No metadata present on statement");
		
		return rsMetaData;
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		checkIfClosed();

		return this.getColumnIndexFromName(columnLabel);
	}

	@Override
	public boolean isClosed() throws SQLException {
		return isClosed;
	}

	@Override
	public BigDecimal getBigDecimal(int index) throws SQLException {
		try {
			wasNull = resultConsumer.getResultChunk().getSingleValue(rowIndex, index).isNull();
			return (this.wasNull ? null : getNativeBigDecimal(index));
		} catch (PEException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public BigDecimal getBigDecimal(String columnName) throws SQLException {
		return this.getBigDecimal(this.findColumn(columnName));
	}

	BigDecimal getNativeBigDecimal(int index) throws PEException {
		return (BigDecimal) resultConsumer.getResultChunk().getSingleValue(rowIndex, index).getColumnValue();
	}
	
	@Override
	public boolean getBoolean(int index) throws SQLException {
		try {
			wasNull = resultConsumer.getResultChunk().getSingleValue(rowIndex, index).isNull();
			return (this.wasNull ? false : getNativeBoolean(index));
		} catch (PEException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public boolean getBoolean(String columnName) throws SQLException {
		return this.getBoolean(this.findColumn(columnName));
	}

	boolean getNativeBoolean(int index) throws PEException {
		return (Boolean) resultConsumer.getResultChunk().getSingleValue(rowIndex, index).getColumnValue();
	}

	@Override
	public byte getByte(int index) throws SQLException {
		try {
			wasNull = resultConsumer.getResultChunk().getSingleValue(rowIndex, index).isNull();
			return (this.wasNull ? 0 : getNativeByte(index));
		} catch (PEException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public byte getByte(String columnName) throws SQLException {
		return this.getByte(this.findColumn(columnName));
	}

	byte getNativeByte(int index) throws PEException {
		return ((Byte) resultConsumer.getResultChunk().getSingleValue(rowIndex, index).getColumnValue()).byteValue();
	}

	@Override
	public byte[] getBytes(int index) throws SQLException {
		try {
			wasNull = resultConsumer.getResultChunk().getSingleValue(rowIndex, index).isNull();
			return (wasNull ? null : getNativeBytes(index));
		} catch (PEException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public byte[] getBytes(String columnName) throws SQLException {
		return this.getBytes(this.findColumn(columnName));
	}

	byte[] getNativeBytes(int index) throws PEException {
		return (byte[]) resultConsumer.getResultChunk().getSingleValue(rowIndex, index).getColumnValue();
	}

	@Override
	public Date getDate(int index) throws SQLException {
		try {
			wasNull = resultConsumer.getResultChunk().getSingleValue(rowIndex, index).isNull();
			return (this.wasNull ? null : getNativeDate(index));
		} catch (PEException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public Date getDate(String columnName) throws SQLException {
		return this.getDate(this.findColumn(columnName));
	}

	Date getNativeDate(int index) throws PEException {
		return new Date(((java.util.Date) resultConsumer.getResultChunk().getSingleValue(rowIndex, index).getColumnValue()).getTime());
	}

	@Override
	public double getDouble(int index) throws SQLException {
		try {
			this.wasNull = resultConsumer.getResultChunk().getSingleValue(rowIndex, index).isNull();
			return (this.wasNull ? 0 : getNativeDouble(index));
		} catch (PEException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public double getDouble(String columnName) throws SQLException {
		return this.getDouble(this.findColumn(columnName));
	}

	double getNativeDouble(int index) throws PEException {
		Double d;
		Object o = resultConsumer.getResultChunk().getSingleValue(rowIndex, index).getColumnValue();
		if (o instanceof String) {
			d = Double.parseDouble((String)o);
		} else {
			d = (Double)o;
		}
		return d;
	}

	@Override
	public float getFloat(int index) throws SQLException {
		try {
			this.wasNull = resultConsumer.getResultChunk().getSingleValue(rowIndex, index).isNull();
			return (this.wasNull ? 0 : getNativeFloat(index));
		} catch (PEException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public float getFloat(String columnName) throws SQLException {
		return this.getFloat(this.findColumn(columnName));
	}

	float getNativeFloat(int index) throws PEException {
		Double d = getNativeDouble(index);
		return d.floatValue();
	}

	@Override
	public int getInt(int index) throws SQLException {
		try {
			this.wasNull = resultConsumer.getResultChunk().getSingleValue(rowIndex, index).isNull();
			return (this.wasNull ? 0 : getNativeInt(index));
		} catch (PEException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public int getInt(String columnName) throws SQLException {
		return this.getInt(this.findColumn(columnName));
	}

	int getNativeInt(int index) throws PEException {
		// handle call of getInt for type Long
		Object dataObject = resultConsumer.getResultChunk().getSingleValue(rowIndex, index).getColumnValue();
		if (dataObject instanceof Long)
			return ((Long) dataObject).intValue();
		else
			return (Integer) dataObject;
	}

	@Override
	public Object getObject(int index) throws SQLException {
		try {
			return resultConsumer.getResultChunk().getSingleValue(rowIndex, index).getColumnValue();
		} catch (PEException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public Object getObject(String columnName) throws SQLException {
		return this.getObject(this.findColumn(columnName));
	}

	@Override
	public short getShort(int index) throws SQLException {
		try {
			this.wasNull = resultConsumer.getResultChunk().getSingleValue(rowIndex, index).isNull();
			return (this.wasNull ? 0 : ((Integer) resultConsumer.getResultChunk().getSingleValue(rowIndex, index).getColumnValue()).shortValue());
		} catch (PEException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public short getShort(String columnName) throws SQLException {
		return this.getShort(this.findColumn(columnName));
	}

	@Override
	public Statement getStatement() {
		return null;
	}

	@Override
	public Time getTime(int index) throws SQLException {
		try {
			this.wasNull = resultConsumer.getResultChunk().getSingleValue(rowIndex, index).isNull();
			return (this.wasNull ? null : getNativeTime(index));
		} catch (PEException e) {
			throw new SQLException(e);
		}
	}

	Time getNativeTime(int index) throws PEException {
		return new Time(((java.util.Date) resultConsumer.getResultChunk().getSingleValue(rowIndex, index).getColumnValue()).getTime());
	}

	@Override
	public Time getTime(String columnName) throws SQLException {
		return this.getTime(this.findColumn(columnName));
	}

	@Override
	public Timestamp getTimestamp(int index) throws SQLException {
		try {
			this.wasNull = resultConsumer.getResultChunk().getSingleValue(rowIndex, index).isNull();
			return (this.wasNull ? null : new Timestamp(((java.util.Date) resultConsumer.getResultChunk().getSingleValue(rowIndex, index)
					.getColumnValue()).getTime()));
		} catch (PEException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public Timestamp getTimestamp(String columnName) throws SQLException {
		return this.getTimestamp(this.findColumn(columnName));
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return this.warningChain;
	}

	/**
	 * Currently unsupported methods
	 */
	void unsupportedMethod() throws SQLException {
		throw new SQLFeatureNotSupportedException("Unsupported ResultSet method call");
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		unsupportedMethod();
		return false;
	}

	@SuppressWarnings("deprecation")
	@Deprecated
	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@SuppressWarnings("deprecation")
	@Deprecated
	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@SuppressWarnings("deprecation")
	@Deprecated
	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@SuppressWarnings("deprecation")
	@Deprecated
	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public String getCursorName() throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		unsupportedMethod();
		return false;
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		unsupportedMethod();
		return false;
	}

	@Override
	public boolean isFirst() throws SQLException {
		unsupportedMethod();
		return false;
	}

	@Override
	public boolean isLast() throws SQLException {
		unsupportedMethod();
		return false;
	}

	@Override
	public void beforeFirst() throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void afterLast() throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public boolean first() throws SQLException {
		unsupportedMethod();
		return false;
	}

	@Override
	public boolean last() throws SQLException {
		unsupportedMethod();
		return false;
	}

	@Override
	public int getRow() throws SQLException {
		unsupportedMethod();
		return 0;
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		unsupportedMethod();
		return false;
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		unsupportedMethod();
		return false;
	}

	@Override
	public boolean previous() throws SQLException {
		unsupportedMethod();
		return false;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public int getFetchDirection() throws SQLException {
		unsupportedMethod();
		return 0;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public int getFetchSize() throws SQLException {
		unsupportedMethod();
		return 0;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		unsupportedMethod();
		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException {
		unsupportedMethod();
		return false;
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		unsupportedMethod();
		return false;
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, int length)
			throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void insertRow() throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateRow() throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void deleteRow() throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void refreshRow() throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateNString(int columnIndex, String nString) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateNString(String columnLabel, String nString) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length)
			throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length)
			throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, long length)
			throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream, long length)
			throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		unsupportedMethod();
		
	}

	@Override
	public int getType() throws SQLException {
		unsupportedMethod();
		return 0;
	}

	@Override
	public int getConcurrency() throws SQLException {
		unsupportedMethod();
		return 0;
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		unsupportedMethod();
		return null;
	}

	@Override
	public int getHoldability() throws SQLException {
		unsupportedMethod();
		return 0;
	}

	// TODO this was added for Java 1.7 but removed so we can still build with 1.6
//	@Override
	@Override
	public <T> T getObject(int arg0, Class<T> arg1) throws SQLException {
		unsupportedMethod();
		return null;
	}

	// TODO this was added for Java 1.7 but removed so we can still build with 1.6
//	@Override
	@Override
	public <T> T getObject(String arg0, Class<T> arg1) throws SQLException {
		unsupportedMethod();
		return null;
	}

}
