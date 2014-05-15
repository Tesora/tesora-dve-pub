// OS_STATUS: public
package com.tesora.dve.db;

import io.netty.util.CharsetUtil;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnMetadata;

public abstract class NativeResultHandler implements Serializable {

	private static final long serialVersionUID = 1L;

	public String getObjectAsString(ColumnMetadata uc, Object obj) throws PEException {
		String colStr;
		byte[] b = getObjectAsBytes(uc, obj);
//		try {
			if (uc.getDataType() == Types.BLOB || uc.getDataType() == Types.LONGVARBINARY) {
				colStr = new String(b, CharsetUtil.ISO_8859_1);
			} else {
				colStr = new String(b, CharsetUtil.UTF_8);
			}
//		} catch (UnsupportedEncodingException e) {
//			// Just return the default encoding
//			colStr = new String(b);
//		}
		return colStr;
	}

	public byte[] getObjectAsBytes(ColumnMetadata uc, Object obj) throws PEException {
		byte[] ret = null;
		if (obj == null)
			return null;

		if (obj instanceof String) {
			ret = getStringAsBytes(uc, obj);
		} else if (obj instanceof byte[]) {
			ret = getBytesAsBytes(uc, obj);
		} else if (obj instanceof Byte) {
			ret = getByteAsBytes(uc, obj);
		} else if (obj instanceof Boolean) {
			ret = getBooleanAsBytes(uc, obj);
		} else if (obj instanceof Short) {
			ret = getShortAsBytes(uc, obj);
		} else if (obj instanceof Integer) {
			ret = getIntegerAsBytes(uc, obj);
		} else if (obj instanceof Long) {
			ret = getLongAsBytes(uc, obj);
		} else if (obj instanceof BigInteger) {
			ret = getBigIntegerAsBytes(uc, obj);
		} else if (obj instanceof BigDecimal) {
			ret = getBigDecimalAsBytes(uc, obj);
		} else if (obj instanceof Float) {
			ret = getFloatAsBytes(uc, obj);
		} else if (obj instanceof Double) {
			ret = getDoubleAsBytes(uc, obj);
		} else if (obj instanceof Date) {
			ret = getDateAsBytes(uc, obj);
		} else if (obj instanceof Time) {
			ret = getTimeAsBytes(uc, obj);
		} else if (obj instanceof Timestamp) {
			ret = getTimestampAsBytes(uc, obj);
		} else {
			throw new PEException("Unhandled type " + obj.getClass().getName());
		}

		return ret;
	}

	protected byte[] getBytesAsBytes(ColumnMetadata uc, Object obj) {
		return (byte[]) obj;
	}

	protected byte[] getByteAsBytes(ColumnMetadata uc, Object obj) {
		return obj.toString().getBytes();
	}

	protected byte[] getBooleanAsBytes(ColumnMetadata uc, Object obj) {
		return obj.toString().getBytes();
	}

	protected byte[] getShortAsBytes(ColumnMetadata uc, Object obj) {
		return obj.toString().getBytes();
	}

	protected byte[] getIntegerAsBytes(ColumnMetadata uc, Object obj) {
		return obj.toString().getBytes();
	}

	protected byte[] getLongAsBytes(ColumnMetadata uc, Object obj) {
		return obj.toString().getBytes();
	}

	protected byte[] getBigIntegerAsBytes(ColumnMetadata uc, Object obj) {
		return obj.toString().getBytes();
	}

	protected byte[] getBigDecimalAsBytes(ColumnMetadata uc, Object obj) {
		return obj.toString().getBytes();
	}

	protected byte[] getStringAsBytes(ColumnMetadata uc, Object obj) {
		return ((String) obj).getBytes();
	}

	protected byte[] getFloatAsBytes(ColumnMetadata uc, Object obj) {
		return obj.toString().getBytes();
	}

	protected byte[] getDoubleAsBytes(ColumnMetadata uc, Object obj) {
		return obj.toString().getBytes();
	}

	protected byte[] getDateAsBytes(ColumnMetadata uc, Object obj) {
		return obj.toString().getBytes();
	}

	protected byte[] getTimeAsBytes(ColumnMetadata uc, Object obj) {
		return obj.toString().getBytes();
	}

	protected byte[] getTimestampAsBytes(ColumnMetadata uc, Object obj) {
		return obj.toString().getBytes();
	}

}