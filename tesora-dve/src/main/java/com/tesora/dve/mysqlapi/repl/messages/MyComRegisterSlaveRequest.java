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

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import com.tesora.dve.db.mysql.libmy.MyMessageType;
import com.tesora.dve.db.mysql.libmy.MyRequestMessage;
import com.tesora.dve.exceptions.PEException;

public class MyComRegisterSlaveRequest extends MyRequestMessage {
	private int slaveServerId;
	private String reportHost;
	private String reportUser;
	private String reportPassword;
	private short reportPort;
	
	public MyComRegisterSlaveRequest() {};
	
	// it appears as though you only need to populate server_id and report_port
	// for this to work
	public MyComRegisterSlaveRequest(int slaveServerId, int reportPort) {
		this.slaveServerId = slaveServerId;
		this.reportPort = (short) reportPort;
	}
	
	@Override
	public void marshallMessage(ByteBuf cb) throws PEException {
		cb.writeInt(slaveServerId);
		MysqlAPIUtils.putLengthCodedString(cb, reportHost, true /* codeNullasZero */);
		MysqlAPIUtils.putLengthCodedString(cb, reportUser, true /* codeNullasZero */);
		MysqlAPIUtils.putLengthCodedString(cb, reportPassword, true /* codeNullasZero */);
		cb.writeShort(reportPort);
		cb.writeZero(8);
	}

	@Override
	public void unmarshallMessage(ByteBuf cb) {
		slaveServerId = cb.readInt();
		reportHost = MysqlAPIUtils.getLengthCodedString(cb);
		reportUser = MysqlAPIUtils.getLengthCodedString(cb);
		reportPassword = MysqlAPIUtils.getLengthCodedString(cb);
		reportPort = cb.readShort();
	}

	@Override
	public MyMessageType getMessageType() {
		return MyMessageType.COM_REGISTER_SLAVE_REQUEST;
	}

	public int getSlaveServerId() {
		return slaveServerId;
	}

	public void setSlaveServerId(int slaveServerId) {
		this.slaveServerId = slaveServerId;
	}

	public String getReportHost() {
		return reportHost;
	}

	public void setReportHost(String reportHost) {
		this.reportHost = reportHost;
	}

	public String getReportUser() {
		return reportUser;
	}

	public void setReportUser(String reportUser) {
		this.reportUser = reportUser;
	}

	public String getReportPassword() {
		return reportPassword;
	}

	public void setReportPassword(String reportPassword) {
		this.reportPassword = reportPassword;
	}

	public short getReportPort() {
		return reportPort;
	}

	public void setPort(short reportPort) {
		this.reportPort = reportPort;
	}
}
