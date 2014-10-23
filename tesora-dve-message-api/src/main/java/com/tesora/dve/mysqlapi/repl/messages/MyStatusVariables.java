package com.tesora.dve.mysqlapi.repl.messages;

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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import com.tesora.dve.exceptions.PEException;

public class MyStatusVariables {

	public enum MyQueryEventCode {
		Q_FLAGS2_CODE((byte) 0x00), 
		Q_SQL_MODE_CODE((byte) 0x01), 
		Q_CATALOG_CODE((byte) 0x02),
		Q_AUTO_INCREMENT((byte) 0x03),
		Q_CHARSET_CODE((byte) 0x04), 
		Q_TIME_ZONE_CODE((byte) 0x05), 
		Q_CATALOG_NZ_CODE((byte) 0x06), 
		Q_LC_TIME_NAMES_CODE((byte) 0x07), 
		Q_CHARSET_DATABASE_CODE((byte) 0x08), 
		Q_TABLE_MAP_FOR_UPDATE_CODE((byte) 0x09), 
		Q_MASTER_DATA_WRITTEN_CODE((byte) 0x0A), 
		Q_INVOKER((byte) 0x0B),
		Q_UPDATED_DB_NAMES((byte) 0x0C),
		Q_MICROSECONDS((byte) 0x0D),
        Q_HRNOW((byte)0x80)
		;

		private final byte code;

		MyQueryEventCode(byte b) {
			code = b;
		}

		public static MyQueryEventCode fromByte(byte b) {
			for (MyQueryEventCode mt : values()) {
				if (mt.code == b) {
					return mt;
				}
			}
			return null;
		}

		public byte getByteValue() {
			return code;
		}
	}

	Set<BaseQueryEvent> suppliedEventCodes = new LinkedHashSet<BaseQueryEvent>();
	
	public MyStatusVariables() {
		// TODO Auto-generated constructor stub
	}

	public Set<BaseQueryEvent> getSuppliedEventCodes() {
		return suppliedEventCodes;
	}

	public void setSuppliedEventCodes(Set<BaseQueryEvent> suppliedEventCodes) {
		this.suppliedEventCodes = suppliedEventCodes;
	}

	public void parseStatusVariables(ByteBuf cb, int svLen) throws PEException {
		if (svLen > 0) {
			ByteBuf statsVarsBuf = cb.readBytes(svLen);
			while (statsVarsBuf.isReadable()) {
				byte code = statsVarsBuf.readByte();
				MyQueryEventCode mqec = MyQueryEventCode.fromByte(code);
				if (mqec == null) {
					throw new PEException("Replication could not decode query event code: '" + code + "' (0x" + Integer.toHexString(code) + ")");
				}

				switch (mqec) {
				case Q_FLAGS2_CODE:
					int flags = statsVarsBuf.readInt();
					suppliedEventCodes.add(new QueryFlags2Event(flags));
					break;

				case Q_SQL_MODE_CODE:
					long sqlMode = statsVarsBuf.readLong();
					suppliedEventCodes.add(new QuerySQLModeEvent(sqlMode));
					break;

				case Q_CATALOG_CODE:
				{
					byte len = statsVarsBuf.readByte();
					String catalog = MysqlAPIUtils.readBytesAsString(statsVarsBuf, len, CharsetUtil.UTF_8);
					statsVarsBuf.readByte(); // null terminated byte

					suppliedEventCodes.add(new QueryCatalogEvent(catalog));
					break;
				}	
				case Q_AUTO_INCREMENT:
					int autoIncrementIncrement = statsVarsBuf.readUnsignedShort();
					int autoIncrementOffset = statsVarsBuf.readUnsignedShort();

					suppliedEventCodes.add(new QueryAutoIncrementEvent(autoIncrementIncrement, autoIncrementOffset));
					break;

				case Q_CHARSET_CODE:
					int charSetClient = statsVarsBuf.readUnsignedShort();
					int collationConnection = statsVarsBuf.readUnsignedShort();
					int collationServer = statsVarsBuf.readUnsignedShort();

					suppliedEventCodes.add(new QueryCharSetCodeEvent(charSetClient, collationConnection, collationServer));
					break;

				case Q_TIME_ZONE_CODE:
				{
					byte len = statsVarsBuf.readByte();
					String timeZone = MysqlAPIUtils.readBytesAsString(statsVarsBuf, len, CharsetUtil.UTF_8);

					suppliedEventCodes.add(new QueryTimeZoneCodeEvent(timeZone));
					break;
				}
				case Q_CATALOG_NZ_CODE:
				{
					byte catalogLen = statsVarsBuf.readByte();
					String catalog = MysqlAPIUtils.readBytesAsString(statsVarsBuf, catalogLen, CharsetUtil.UTF_8);

					suppliedEventCodes.add(new QueryCatalogNZEvent(catalog));
					break;
				}
				case Q_LC_TIME_NAMES_CODE:
					short monthDayNames = statsVarsBuf.readShort();

					suppliedEventCodes.add(new QueryTimeNamesEvent(monthDayNames));
					break;

				case Q_CHARSET_DATABASE_CODE:
					short collationDatabase = statsVarsBuf.readShort();

					suppliedEventCodes.add(new QueryCollationDatabaseEvent(collationDatabase));
					break;

				case Q_TABLE_MAP_FOR_UPDATE_CODE:
					long tableMapForUpdate = statsVarsBuf.readLong();

					suppliedEventCodes.add(new QueryTableMapEvent(tableMapForUpdate));
					break;

				case Q_MASTER_DATA_WRITTEN_CODE:
					int originalLength = statsVarsBuf.readInt();

					suppliedEventCodes.add(new QueryMasterDataWrittenEvent(originalLength));
					break;

				case Q_INVOKER:
					int userLen = statsVarsBuf.readByte();
					String user = MysqlAPIUtils.readBytesAsString(statsVarsBuf, userLen, CharsetUtil.UTF_8);
					int hostLen = statsVarsBuf.readByte();
					String host = MysqlAPIUtils.readBytesAsString(statsVarsBuf, hostLen, CharsetUtil.UTF_8);

					suppliedEventCodes.add(new QueryInvokerEvent(user, host));
					break;

				case Q_UPDATED_DB_NAMES:
					List<String> dbNames = new ArrayList<String>();
					int numDbs = statsVarsBuf.readByte();
					if (numDbs > 0) {
						for(int i=0; i<numDbs; i++) {
							dbNames.add(statsVarsBuf.readSlice(statsVarsBuf.bytesBefore((byte)0)).toString(
									CharsetUtil.UTF_8));
							statsVarsBuf.readByte(); //read null byte
						}
					}
					suppliedEventCodes.add(new QueryUpdatedDBNamesEvent(dbNames));
					break;

				case Q_MICROSECONDS:
					int microseconds = statsVarsBuf.readMedium();

					suppliedEventCodes.add(new QueryMicrosecondsEvent(microseconds));
					break;

                case Q_HRNOW:
                    //TODO: this was apparently added for MariaDB, but I can't find a lot of info on it. skip for now.
                    suppliedEventCodes.add(new QueryMicrosecondsEvent(statsVarsBuf.readUnsignedMedium()));
                    break;

				default :
					throw new PEException("Replication encountered an unknown query event code: '" + code + "' (0x" + Integer.toHexString(code) + ")");
				}
			}
		}
	}

	public void writeStatusVariables(ByteBuf cb) {
		for(BaseQueryEvent qe : getSuppliedEventCodes()) {
			MyQueryEventCode code = qe.getCode();
			switch(code) {
			case Q_FLAGS2_CODE:
				cb.writeByte(code.getByteValue());
				cb.writeInt(((QueryFlags2Event)qe).getFlags());
				break;

			case Q_SQL_MODE_CODE:
				cb.writeByte(code.getByteValue());
				cb.writeLong(((QuerySQLModeEvent)qe).getSqlMode());
				break;

			case Q_CATALOG_CODE:
				cb.writeByte(code.getByteValue());
				cb.writeByte(((QueryCatalogEvent)qe).getCatalog().length());
				cb.writeBytes(((QueryCatalogEvent)qe).getCatalog().getBytes());
				cb.writeByte(0); //for trailing 0
				break;
				
			case Q_AUTO_INCREMENT:
				cb.writeByte(code.getByteValue());
				cb.writeShort(((QueryAutoIncrementEvent)qe).getAutoIncrementIncrement());
				cb.writeShort(((QueryAutoIncrementEvent)qe).getAutoIncrementOffset());
				break;

			case Q_CHARSET_CODE:
				cb.writeByte(code.getByteValue());
				cb.writeShort(((QueryCharSetCodeEvent)qe).getCharSetClient());
				cb.writeShort(((QueryCharSetCodeEvent)qe).getCollationConnection());
				cb.writeShort(((QueryCharSetCodeEvent)qe).getCollationServer());
				break;

			case Q_TIME_ZONE_CODE:
				cb.writeByte(code.getByteValue());
				cb.writeByte(((QueryTimeZoneCodeEvent)qe).getTimeZone().length());
				cb.writeBytes(((QueryTimeZoneCodeEvent)qe).getTimeZone().getBytes());
				break;

			case Q_CATALOG_NZ_CODE:
				cb.writeByte(code.getByteValue());
				cb.writeByte(((QueryCatalogNZEvent)qe).getCatalog().length());
				cb.writeBytes(((QueryCatalogNZEvent)qe).getCatalog().getBytes());
				break;

			case Q_LC_TIME_NAMES_CODE:
				cb.writeByte(code.getByteValue());
				cb.writeShort(((QueryTimeNamesEvent)qe).getMonthDayNames());
				break;

			case Q_CHARSET_DATABASE_CODE:
				cb.writeByte(code.getByteValue());
				cb.writeShort(((QueryCollationDatabaseEvent)qe).getCollationDatabase());
				break;

			case Q_TABLE_MAP_FOR_UPDATE_CODE:
				cb.writeByte(code.getByteValue());
				cb.writeLong(((QueryTableMapEvent)qe).getTableMapForUpdate());
				break;

			case Q_MASTER_DATA_WRITTEN_CODE:
				cb.writeByte(code.getByteValue());
				cb.writeInt(((QueryMasterDataWrittenEvent)qe).getOriginalLength());
				break;
			
			case Q_INVOKER:
				cb.writeByte(code.getByteValue());
				cb.writeByte(((QueryInvokerEvent)qe).getUser().length());
				cb.writeBytes(((QueryInvokerEvent)qe).getUser().getBytes());
				cb.writeByte(((QueryInvokerEvent)qe).getHost().length());
				cb.writeBytes(((QueryInvokerEvent)qe).getHost().getBytes());
				break;

			case Q_UPDATED_DB_NAMES:
				cb.writeByte(code.getByteValue());
				List<String> dbs = ((QueryUpdatedDBNamesEvent)qe).getDbNames();
				cb.writeByte(dbs == null ? 0 : dbs.size());
				if (dbs.size() >  0) {
					for(String db : dbs) {
						cb.writeByte(db.length());
						cb.writeBytes(db.getBytes(CharsetUtil.UTF_8));
						cb.writeByte(0); //for trailing 0
					}
				}
				break;

			case Q_MICROSECONDS:
				cb.writeByte(code.getByteValue());
				cb.writeMedium(((QueryMicrosecondsEvent)qe).getMicroseconds());
				break;

            case Q_HRNOW:
                cb.writeMedium(((QueryHRNowEvent)qe).threeBytes);
                break;
			
			default :
				break;
			}
		}
	}

	public abstract class BaseQueryEvent {
		MyQueryEventCode code;

		public BaseQueryEvent() {
		}
		
		public BaseQueryEvent(MyQueryEventCode code) {
			this.code = code;
		}
		
		public abstract void outputConsole();

		public MyQueryEventCode getCode() {
			return code;
		}

		public void setCode(MyQueryEventCode code) {
			this.code = code;
		}
	}
	
	public class QueryFlags2Event extends BaseQueryEvent {
		final int OPTION_AUTO_IS_NULL = (1 << 14);
		final int OPTION_NO_FOREIGN_KEY_CHECKS = (1 << 26);
		final int OPTION_RELAXED_UNIQUE_CHECKS = (1 << 27);
		final int OPTION_NOT_AUTOCOMMIT = (1 << 19);

		int flags = 0;
		Boolean optionAutoIsNull = null;
		Boolean optionNoForeignKeyChecks = null;
		Boolean optionRelaxedUniqueChecks = null;
		Boolean optionNotAutoCommit = null;
		
		public QueryFlags2Event(int flags) {
			super(MyQueryEventCode.Q_FLAGS2_CODE);
			
			this.flags = flags;
			optionAutoIsNull = (flags | OPTION_AUTO_IS_NULL) == 1;
			optionNoForeignKeyChecks = (flags | OPTION_NO_FOREIGN_KEY_CHECKS) == 1;
			optionRelaxedUniqueChecks = (flags | OPTION_RELAXED_UNIQUE_CHECKS) == 1;
			optionNotAutoCommit = (flags | OPTION_NOT_AUTOCOMMIT) == 1;
		}

		@Override
		public void outputConsole() {
			System.out.println("\t\tQueryFlag2");
			System.out.println("\t\t\tAutoIsNull: " + optionAutoIsNull);
			System.out.println("\t\t\tNoForeignKeyChecks: " + optionNoForeignKeyChecks);
			System.out.println("\t\t\tRelaxedUniqueChecks: " + optionRelaxedUniqueChecks);
			System.out.println("\t\t\tNotAutoCommit: " + optionNotAutoCommit);
		}

		public int getFlags() {
			return flags;
		}

		public void setFlags(int flags) {
			this.flags = flags;
		}

		public boolean isOptionAutoIsNull() {
			return optionAutoIsNull;
		}

		public void setOptionAutoIsNull(boolean optionAutoIsNull) {
			this.optionAutoIsNull = optionAutoIsNull;
		}

		public boolean isOptionNoForeignKeyChecks() {
			return optionNoForeignKeyChecks;
		}

		public void setOptionNoForeignKeyChecks(boolean optionNoForeignKeyChecks) {
			this.optionNoForeignKeyChecks = optionNoForeignKeyChecks;
		}

		public boolean isOptionRelaxedUniqueChecks() {
			return optionRelaxedUniqueChecks;
		}

		public void setOptionRelaxedUniqueChecks(boolean optionRelaxedUniqueChecks) {
			this.optionRelaxedUniqueChecks = optionRelaxedUniqueChecks;
		}

		public boolean isOptionNotAutoCommit() {
			return optionNotAutoCommit;
		}

		public void setOptionNotAutoCommit(boolean optionNotAutoCommit) {
			this.optionNotAutoCommit = optionNotAutoCommit;
		}
	}
	
	public class QuerySQLModeEvent extends BaseQueryEvent {
		long sqlMode;
		
		public QuerySQLModeEvent(long sqlMode) {
			super(MyQueryEventCode.Q_SQL_MODE_CODE);
			this.sqlMode = sqlMode;
		}

		@Override
		public void outputConsole() {
			System.out.println("\t\tSQLMode");
			System.out.println("\t\t\tvalue: " + sqlMode);
		}

		public long getSqlMode() {
			return sqlMode;
		}

		public void setSqlMode(long sqlMode) {
			this.sqlMode = sqlMode;
		}
	}
	
	public class QueryCatalogEvent extends BaseQueryEvent {
		String cat;

		public QueryCatalogEvent(String cat) {
			super(MyQueryEventCode.Q_CATALOG_CODE);
			this.cat = cat;
		}

		@Override
		public void outputConsole() {
			System.out.println("\t\tCatalog");
			System.out.println("\t\t\tCatalog: " + cat);
		}

		public String getCatalog() {
			return cat;
		}

		public void setCatalog(String cat) {
			this.cat = cat;
		}
	}

	public class QueryAutoIncrementEvent extends BaseQueryEvent {
		int autoIncrementIncrement;
		int autoIncrementOffset;
		
		public QueryAutoIncrementEvent(int autoIncrementIncrement, int autoIncrementOffset) {
			super(MyQueryEventCode.Q_AUTO_INCREMENT);
			this.autoIncrementIncrement = autoIncrementIncrement;
			this.autoIncrementOffset = autoIncrementOffset;
		}

		@Override
		public void outputConsole() {
			System.out.println("\t\tAutoIncrement");
			System.out.println("\t\t\tIncrement: " + autoIncrementIncrement);
			System.out.println("\t\t\tOffset: " + autoIncrementOffset);
		}

		public int getAutoIncrementIncrement() {
			return autoIncrementIncrement;
		}

		public void setAutoIncrementIncrement(int autoIncrementIncrement) {
			this.autoIncrementIncrement = autoIncrementIncrement;
		}

		public int getAutoIncrementOffset() {
			return autoIncrementOffset;
		}

		public void setAutoIncrementOffset(int autoIncrementOffset) {
			this.autoIncrementOffset = autoIncrementOffset;
		}
	}
	
	public class QueryCharSetCodeEvent extends BaseQueryEvent {
		int charSetClient;
		int collationConnection;
		int collationServer;

		public QueryCharSetCodeEvent(int charSetClient, int collationConnection, int collationServer) {
			super(MyQueryEventCode.Q_CHARSET_CODE);
			this.charSetClient = charSetClient;
			this.collationConnection = collationConnection;
			this.collationServer = collationServer;
		}

		@Override
		public void outputConsole() {
			System.out.println("\t\tCharSet Code");
			System.out.println("\t\t\tClient Charset: " + charSetClient);
			System.out.println("\t\t\tConnection Collation: " + collationConnection);
			System.out.println("\t\t\tServer Collation: " + collationServer);
		}

		public int getCharSetClient() {
			return charSetClient;
		}

		public void setCharSetClient(int charSetClient) {
			this.charSetClient = charSetClient;
		}

		public int getCollationConnection() {
			return collationConnection;
		}

		public void setCollationConnection(int collationConnection) {
			this.collationConnection = collationConnection;
		}

		public int getCollationServer() {
			return collationServer;
		}

		public void setCollationServer(int collationServer) {
			this.collationServer = collationServer;
		}
	}
	
	public class QueryTimeZoneCodeEvent extends BaseQueryEvent {
		String timeZone;

		public QueryTimeZoneCodeEvent(String timeZone) {
			super(MyQueryEventCode.Q_TIME_ZONE_CODE);
			this.timeZone = timeZone;
		}

		@Override
		public void outputConsole() {
			System.out.println("\t\tTime Zone");
			System.out.println("\t\t\tTime Zone: " + timeZone);
		}

		public String getTimeZone() {
			return timeZone;
		}

		public void setTimeZone(String timeZone) {
			this.timeZone = timeZone;
		}
	}
	
	public class QueryCatalogNZEvent extends BaseQueryEvent {
		String catalog;
		
		public QueryCatalogNZEvent(String catalog) {
			super(MyQueryEventCode.Q_CATALOG_NZ_CODE);
			this.catalog = catalog;
		}

		@Override
		public void outputConsole() {
			System.out.println("\t\tCatalog");
			System.out.println("\t\t\tName: " + catalog);
		}

		public String getCatalog() {
			return catalog;
		}

		public void setCatalog(String catalog) {
			this.catalog = catalog;
		}
	}
	
	public class QueryTimeNamesEvent extends BaseQueryEvent {
		short monthDayNames;

		public QueryTimeNamesEvent(short monthDayNames) {
			super(MyQueryEventCode.Q_LC_TIME_NAMES_CODE);
			this.monthDayNames = monthDayNames;
		}

		@Override
		public void outputConsole() {
			System.out.println("\t\tTime Name Map");
			System.out.println("\t\t\tMonth/Day: " + monthDayNames);
		}

		public short getMonthDayNames() {
			return monthDayNames;
		}

		public void setMonthDayNames(short monthDayNames) {
			this.monthDayNames = monthDayNames;
		}
	}
	
	public class QueryCollationDatabaseEvent extends BaseQueryEvent {
		short collationDatabase;

		public QueryCollationDatabaseEvent(short collationDatabase) {
			super(MyQueryEventCode.Q_CHARSET_DATABASE_CODE);
			this.collationDatabase = collationDatabase;
		}

		@Override
		public void outputConsole() {
			System.out.println("\t\tDB Collation");
			System.out.println("\t\t\tCollation Code: " + collationDatabase);
		}

		public short getCollationDatabase() {
			return collationDatabase;
		}

		public void setCollationDatabase(short collationDatabase) {
			this.collationDatabase = collationDatabase;
		}
	}
	
	class QueryTableMapEvent extends BaseQueryEvent {
		long tableMapForUpdate;

		public QueryTableMapEvent(long tableMapForUpdate) {
			super(MyQueryEventCode.Q_TABLE_MAP_FOR_UPDATE_CODE);
			this.tableMapForUpdate = tableMapForUpdate;
		}

		@Override
		public void outputConsole() {
			System.out.println("\t\tTable Map for Update");
			System.out.println("\t\t\tCode: " + tableMapForUpdate);
		}

		public long getTableMapForUpdate() {
			return tableMapForUpdate;
		}

		public void setTableMapForUpdate(long tableMapForUpdate) {
			this.tableMapForUpdate = tableMapForUpdate;
		}
	}

	class QueryMasterDataWrittenEvent extends BaseQueryEvent {
		int originalLength;

		public QueryMasterDataWrittenEvent(int originalLength) {
			super(MyQueryEventCode.Q_MASTER_DATA_WRITTEN_CODE);
			this.originalLength = originalLength;
		}

		@Override
		public void outputConsole() {
			System.out.println("\t\tMaster Data Written");
			System.out.println("\t\t\tOriginal Length: " + originalLength);
		}

		public int getOriginalLength() {
			return originalLength;
		}

		public void setOriginalLength(int originalLength) {
			this.originalLength = originalLength;
		}
	}

	class QueryInvokerEvent extends BaseQueryEvent {
		String user;
		String host;

		public QueryInvokerEvent(String user, String host) {
			super(MyQueryEventCode.Q_INVOKER);
			this.user = user;
			this.host = host;
		}

		@Override
		public void outputConsole() {
			System.out.println("\t\tInvoker");
			System.out.println("\t\t\tUser: " + user);
			System.out.println("\t\t\tHost: " + host);
		}

		public String getUser() {
			return user;
		}

		public void setUser(String user) {
			this.user = user;
		}

		public String getHost() {
			return host;
		}

		public void setHost(String host) {
			this.host = host;
		}
	}

	class QueryUpdatedDBNamesEvent extends BaseQueryEvent {
		List<String> dbNames;
		
		public List<String> getDbNames() {
			return dbNames;
		}

		@Override
		public void outputConsole() {
			System.out.println("\t\tUpdated Databases");
			for(String db : dbNames) {
				System.out.println("\t\t\tDatabase: " + db);
			}
		}

		public void setDbNames(List<String> dbNames) {
			this.dbNames = dbNames;
		}

		public QueryUpdatedDBNamesEvent(List<String> dbNames) {
			this.dbNames = dbNames;
		}
	}
	
	class QueryMicrosecondsEvent extends BaseQueryEvent {
		int microseconds;
		
		public QueryMicrosecondsEvent(int microseconds) {
			this.microseconds = microseconds;
		}

		@Override
		public void outputConsole() {
			System.out.println("\t\tMicroseconds");
			System.out.println("\t\t\tMicroseconds: " + microseconds);
		}

		public int getMicroseconds() {
			return microseconds;
		}

		public void setMicroseconds(int microseconds) {
			this.microseconds = microseconds;
		}
	}

    class QueryHRNowEvent extends BaseQueryEvent {
        int threeBytes;

        QueryHRNowEvent(int threeBytes) {
            this.threeBytes = threeBytes;
        }

        @Override
        public void outputConsole() {
            System.out.println("\t\tHRNOW");
            System.out.println("\t\t\tHRNOW: " + threeBytes);
        }
    }
}
