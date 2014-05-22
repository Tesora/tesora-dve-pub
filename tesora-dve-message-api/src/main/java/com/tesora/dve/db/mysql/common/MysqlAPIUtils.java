package com.tesora.dve.db.mysql.common;

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

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;

import com.tesora.dve.exceptions.PEException;

public class MysqlAPIUtils {
	public static short LEN_CODED_8_BITS = (short) 250;
	public static short LEN_CODED_NULL = (short) 251;
	public static short LEN_CODED_16_BITS = (short) 252;
	public static short LEN_CODED_24_BITS = (short) 253;
	public static short LEN_CODED_64_BITS = (short) 254;

	private static int UNSIGNED_SHORT_MAX = 65535;
	private static int UNSIGNED_MEDIUM_MAX = 16777215;

	/**
	 * Method to create a Length Coded string that has a Length Code Binary
	 * 
	 * As described in MySQL internals doc: Length Coded String: a
	 * variable-length string. Used instead of Null-Terminated String,
	 * especially for character strings which might contain '\0' or might be
	 * very long. The first part of a Length Coded String is a Length Coded
	 * Binary number (the length); the second part of a Length Coded String is
	 * the actual data. An example of a short Length Coded String is these three
	 * hexadecimal bytes: 02 61 62, which means "length = 2, contents = 'ab'".
	 * 
	 * Length Coded Binary: a variable-length number. To compute the value of a
	 * Length Coded Binary, one must examine the value of its first byte.
	 * 
	 * Value Of # Of Bytes Description First Byte Following 0-250 0 = value of
	 * first byte 251 0 column value = NULL only appropriate in a Row Data
	 * Packet 252 2 = value of following 16-bit word 253 3 = value of following
	 * 24-bit word 254 8 = value of following 64-bit word
	 * 
	 * Thus the length of a Length Coded Binary, including the first byte, will
	 * vary from 1 to 9 bytes. The relevant MySQL source program is
	 * sql/protocol.cc net_store_length().
	 * 
	 * All numbers are stored with the least significant byte first. All numbers
	 * are unsigned.
	 * 
	 * @param cb
	 *            - ByteBuf - length coded string is written here
	 * @param data
	 *            - string to put in buffer
	 */
	public static void putLengthCodedString(ByteBuf cb, String data, boolean codeNullasZero) {
		putLengthCodedString(cb, ((data == null)? null : data.getBytes(CharsetUtil.UTF_8)), codeNullasZero);
	}

	public static void putLengthCodedString(ByteBuf cb, byte[] data, boolean codeNullasZero) {
		if (data != null) {
			// need to handle the case of empty string (NULL and empty string are different)
			// mysql puts (byte)0 in the buffer for empty string
			if (data.length > 0) {
				putLengthCodedLong(cb, data.length);
				cb.writeBytes(data);
			} else
				cb.writeZero(1);
		} else {
			if (!codeNullasZero)
				cb.writeByte(LEN_CODED_NULL); // this indicates the string is NULL
			else
				cb.writeZero(1);
		}
	}

	public static void putLengthCodedString(ByteBuf cb, Object obj) {
		if (obj == null) {
			cb.writeByte(LEN_CODED_NULL); // this indicates the string is NULL
		} else if (obj instanceof byte[]) {
			putLengthCodedBinary(cb, (byte[]) obj);
		} else {
			putLengthCodedBinary(cb, obj.toString().getBytes());
		}
	}

	public static void putLengthCodedBinary(ByteBuf cb, byte[] data) {
		final int length = data.length;
		if (length > 0) {
			putLengthCodedLong(cb, length);
			cb.writeBytes(data);
		} else {
			cb.writeZero(1);
		}
	}

	public static void putLengthCodedLong(ByteBuf cb, long length) {
		if (length <= LEN_CODED_8_BITS) {
			cb.writeByte((int) length); // length is 1 byte
		} else if (length <= UNSIGNED_SHORT_MAX) {
			cb.writeByte(LEN_CODED_16_BITS); // length is 2 bytes
			cb.writeShort((int) length);
		} else if (length <= UNSIGNED_MEDIUM_MAX) {
			cb.writeByte(LEN_CODED_24_BITS); // length is 3 bytes
			cb.writeMedium((int) length);
		} else {
			cb.writeByte(LEN_CODED_64_BITS); // length is 8 bytes
			cb.writeLong(length);
		}
	}

	public static long getLengthCodedLong(ByteBuf cb) {
		short byte1 = cb.readUnsignedByte();
		if ( byte1 == LEN_CODED_NULL )
			return 0;
		if ( byte1 <= LEN_CODED_8_BITS ) {
			return byte1;
		} else if ( byte1 == LEN_CODED_16_BITS ) {
			return cb.readUnsignedShort();
		} else if ( byte1 == LEN_CODED_24_BITS ) 
			return cb.readUnsignedMedium();

		return cb.readLong();
	}
	
	static boolean isLengthCodedLongNull(ByteBuf cb) {
		boolean isNull = (cb.getUnsignedByte(cb.readerIndex()) == LEN_CODED_NULL);
		if ( isNull )
			cb.readUnsignedByte();
		
		return isNull;
	}
	
	public static String getLengthCodedString(ByteBuf cb) {
        return getLengthCodedString(cb, CharsetUtil.UTF_8);
	}

    public static String getLengthCodedString(ByteBuf cb, Charset decoder) {

        if ( isLengthCodedLongNull(cb) )
            return null;

        int length = (int)getLengthCodedLong(cb);
        if ( length == 0 )
            return StringUtils.EMPTY;

        return cb.readSlice((int) length).toString(decoder);
    }

    /**
     * Returns the offsets of a series of length encoded strings in the provided buffer.  This method does not alter the provided buffer's
     * read/write indexes or capacity .
     * @param cb a ByteBuf containing zero or more length encoded strings.
     * @return the number of length encoded strings.
     */
    public static ArrayList<Integer> locateLengthCodedStrings(ByteBuf cb) {
        return locateLengthCodedStrings(cb,0);
    }

    public static ArrayList<Integer> locateLengthCodedStrings(ByteBuf cb, int skipAmount) {
        ArrayList<Integer> offsets = new ArrayList<>();
        ByteBuf slice = cb.slice().order(ByteOrder.LITTLE_ENDIAN);
        slice.skipBytes(skipAmount);
        while (slice.isReadable()){
            offsets.add(slice.readerIndex());
            skipLengthCodedString(slice);
        }
        return offsets;
    }

	
	public static void skipLengthCodedString(ByteBuf cb) {
		cb.skipBytes((int) getLengthCodedLong(cb));
	}

	public static byte[] getLengthCodedBinary(ByteBuf cb) {
		
		if ( isLengthCodedLongNull(cb) ) 
			return null;

		int length = (int)getLengthCodedLong(cb);
		if ( length == 0 )
			return ArrayUtils.EMPTY_BYTE_ARRAY;

		return readBytes(cb, length);
	}
	
	public static byte[] readBytes(ByteBuf cb, int len) {
		byte[] ret = new byte[len];
		cb.readBytes(ret);
		return ret;
	}
	
	public static byte[] readBytes(ByteBuf cb) {
		return readBytes(cb, cb.readableBytes());
	}

    /**
     * Returns a byte[] that matches the readable content of the provided ByteBuf.  If possible, this method will
     * return a direct reference to the backing array, rather than copy into a new array.  If the readable content
     * is smaller than the backing array, or there is no backing array because the buffer is direct, the contents
     * are copied into a new byte[].  The readIndex and writeIndex of the provided ByteBuf are not modified by this call.
     * @param cb source of bytes
     * @return direct reference to backing byte[] data, or a copy if needed,
     */
    public static byte[] unwrapOrCopyReadableBytes(ByteBuf cb){
        if (cb.hasArray() && cb.readableBytes() == cb.array().length){
            //already a heap array, and readable length is entire array
            return cb.array();
        } else {
            byte[] copy = new byte[cb.readableBytes()];
            cb.slice().readBytes(copy);
            return copy;
        }
    }
	
	public static String readBytesAsString(ByteBuf cb, int len, Charset cs) {
		return cb.readBytes(len).toString(cs);
	}
	
	public static String readBytesAsString(ByteBuf cb, Charset cs) {
		return readBytesAsString(cb, cb.readableBytes(), cs);
	}

	/**
	 * Methods to read and write length coded dates in the Binary Protocol
	 * 
	 * if year, month, day, hour, minutes, seconds and micro_seconds are all 0,
	 * length is 0 and no other field is sent if hour, minutes, seconds and
	 * micro_seconds are all 0, length is 4 and no other field is sent if
	 * micro_seconds is 0, length is 7 and micro_seconds is not sent otherwise
	 * length is 11
	 * 
	 * Fields 
	 * 		length (1) -- number of bytes following (valid values: 0, 4, 7, 11) 
	 * 		year (2) -- year 
	 * 		month (1) -- month 
	 * 		day (1) -- day 
	 * 		hour (1) -- hour
	 * 		minute (1) -- minutes 
	 * 		second (1) -- seconds 
	 * 		micro_second (4) -- micro-seconds
	 * 
	 */
	public static Date getLengthCodedDate(ByteBuf cb) throws PEException {
		final Calendar cal = Calendar.getInstance();
		cal.clear();

		short length = cb.readUnsignedByte();
		if ( length >= 4 ) {
			cal.set( Calendar.YEAR, cb.readUnsignedShort());
			cal.set( Calendar.MONTH, cb.readByte()-1);			// MONTH is zero based
			cal.set( Calendar.DAY_OF_MONTH, cb.readByte());

			if ( length >= 7 ) {
				cal.set( Calendar.HOUR_OF_DAY, cb.readByte() );
				cal.set( Calendar.MINUTE, cb.readByte() );
				cal.set( Calendar.SECOND, cb.readByte() );
			}

			if ( length == 11 ) {
				long microSeconds = cb.readUnsignedInt();
				cal.set(Calendar.MILLISECOND, (int) (microSeconds / 1000));
			} 

			if ( length > 11 ) {
				throw new PEException( "Invalid length specified to date type (" + length + ")");
			}
		
			return cal.getTime();
		}

		return null;
	}
	
	public static void putLengthCodedDate(ByteBuf cb, Date inDate) {
		byte length = 0;
		Calendar cal = null;
		if (inDate != null) {
			cal = DateUtils.toCalendar(inDate);
			if (cal.get(Calendar.MILLISECOND) > 0) { // indicates we need full 11 byte date
				length = 11;
			} else if (cal.get(Calendar.HOUR_OF_DAY) > 0 || cal.get(Calendar.MINUTE) > 0
					|| cal.get(Calendar.SECOND) > 0) { // this is the 7 byte format
				length = 7;
			} else if (cal.get(Calendar.YEAR) > 0 || cal.get(Calendar.MONTH) > 0 || cal.get(Calendar.DAY_OF_MONTH) > 0) {
				length = 4;
			}
		}
		cb.writeByte(length);
		if (length >= 4) {
			cb.writeShort(cal.get(Calendar.YEAR));
			cb.writeByte(cal.get(Calendar.MONTH)+1);		// MONTH is zero based
			cb.writeByte(cal.get(Calendar.DAY_OF_MONTH));
		}
		if (length >= 7) {
			cb.writeByte(cal.get(Calendar.HOUR_OF_DAY));
			cb.writeByte(cal.get(Calendar.MINUTE));
			cb.writeByte(cal.get(Calendar.SECOND));
		}
		if (length == 11) {
			// 1 millisecond = 1000 microseconds, right?
			int microSeconds = cal.get(Calendar.MILLISECOND) * 1000;
			cb.writeInt(microSeconds);
		}
	}

	/**
	 * Methods to read and write length coded times in the Binary Protocol
	 * 
	 * if days, hours, minutes, seconds and micro_seconds are all 0, length is 0
	 * and no other field is sent
	 * 
	 * if micro_seconds is 0, length is 8 and micro_seconds is not sent
	 * otherwise length is 12
	 * 
	 * Fields 
	 * 		length (1) -- number of bytes following (valid values: 0, 8, 12)
	 * 		is_negative (1) -- (1 if minus, 0 for plus) 
	 * 		days (4) -- days 
	 * 		hours (1) -- hours
	 * 		minutes (1) -- minutes
	 * 		seconds (1) -- seconds
	 * 		micro_seconds (4) -- micro-seconds
	 */
	public static Time getLengthCodedTime(ByteBuf cb) throws PEException {
		Calendar cal = Calendar.getInstance();
		cal.clear();
		short length = cb.readUnsignedByte();

		if (length == 0) 
			return null;
			
		Time ret = null;

		if (length >= 8) {
			cb.skipBytes(1); // this is the sign - we are ignoring this for now
			cb.skipBytes(4); // this is "days" - we are ignoring this for now
			cal.set(Calendar.HOUR_OF_DAY, cb.readByte());
			cal.set(Calendar.MINUTE, cb.readByte());
			cal.set(Calendar.SECOND, cb.readByte());
			if (length == 12) {
				long microSeconds = cb.readUnsignedInt();
				cal.set(Calendar.MILLISECOND, (int) (microSeconds / 1000));
			}
			if (length > 12) {
				throw new PEException("Invalid length specified to date type (" + length + ")");
			}

			ret = new Time(cal.getTimeInMillis());
		}
		return ret;
	}
	
	public static void putLengthCodedTime(ByteBuf cb, Time inTime) {
		byte length = 0;
		Calendar cal = null;
		if (inTime != null) {
			cal = DateUtils.toCalendar(inTime);
			if (cal.get(Calendar.MILLISECOND) > 0) { // indicates we need full 12 byte date
				length = 12;
			} else if (cal.get(Calendar.HOUR_OF_DAY) > 0 || cal.get(Calendar.MINUTE) > 0
					|| cal.get(Calendar.SECOND) > 0) { // this is the 8 byte format
				length = 8;
			}
		}
		cb.writeByte(length);
		if (length >= 8) {
			cb.writeZero(5);		// this is the sign and days - we are not supporting this
			cb.writeByte(cal.get(Calendar.HOUR_OF_DAY));
			cb.writeByte(cal.get(Calendar.MINUTE));
			cb.writeByte(cal.get(Calendar.SECOND));
		}
		if (length == 12) {
			// 1 millisecond = 1000 microseconds, right?
			int microSeconds = cal.get(Calendar.MILLISECOND) * 1000;
			cb.writeInt(microSeconds);
		}
	}
}