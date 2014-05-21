// OS_STATUS: public
package com.tesora.dve.db.mysql;

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

import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;

import org.apache.commons.lang.time.FastDateFormat;

import com.tesora.dve.common.DateUtils;
import com.tesora.dve.db.NativeResultHandler;
import com.tesora.dve.resultset.ColumnMetadata;

public class MysqlNativeResultHandler extends NativeResultHandler {
	private static final Date ZERO_DATE_INDICATOR = DateUtils.getSpecifiedDate(1,1,1);
	private static final String ZERO_DATE = "0000-00-00";
	private static final String ZERO_DATETIME = "0000-00-00 00:00:00";

	private static final FastDateFormat dateFormatter = FastDateFormat.getInstance(MysqlNativeConstants.MYSQL_DATE_FORMAT);
	private static final FastDateFormat timeFormatter = FastDateFormat.getInstance(MysqlNativeConstants.MYSQL_TIME_FORMAT);
	private static final FastDateFormat timestampFormatter = FastDateFormat.getInstance(MysqlNativeConstants.MYSQL_TIMESTAMP_FORMAT);
	
	private static final long serialVersionUID = 1L;

	@Override
	protected byte[] getBooleanAsBytes(ColumnMetadata uc, Object obj) {
		return (((Boolean) obj == true) ? "1".getBytes() : "0".getBytes());
	}
	
	@Override
	protected byte[] getDateAsBytes(ColumnMetadata uc, Object obj) {
		if ( uc.getDataType() == Types.TIME )
			return getTimeAsBytes(uc, new Time (((Date) obj).getTime()));
		
		if ( uc.getDataType() == Types.TIMESTAMP )
			return getTimestampAsBytes(uc, new Timestamp (((Date) obj).getTime()));

		if ( ((Date) obj).equals(ZERO_DATE_INDICATOR) )
			return ZERO_DATE.getBytes();

		return dateFormatter.format((Date) obj).getBytes();
	}

	@Override
	protected byte[] getTimeAsBytes(ColumnMetadata uc, Object obj) {
		return timeFormatter.format((Time)obj).getBytes();
	}

	@Override
	protected byte[] getTimestampAsBytes(ColumnMetadata uc, Object obj) {
		// for some reason doing .equals on Timestamp doesn't work....
		if ( ((Timestamp) obj).getTime() == ZERO_DATE_INDICATOR.getTime() )
			return ZERO_DATETIME.getBytes();
		
		return timestampFormatter.format((Timestamp)obj).getBytes();
	}
}
