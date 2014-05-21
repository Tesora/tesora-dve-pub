// OS_STATUS: public
package com.tesora.dve.comms.client.messages;

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


/**
 * Message version enum.
 */
public enum MessageVersion {

	// Client message protocol versions
    VERSION1((byte) 0x01),
    UNKNOWN((byte) 0x00);

    private final byte versionAsByte;

    private MessageVersion(byte b) {
        this.versionAsByte = b;
    }

    public static MessageVersion fromByte(byte b) {
    	switch (b) {
		case 0x01:
			return VERSION1;
		default:
			return UNKNOWN;
		}
    }

    public byte getByteValue() {
        return this.versionAsByte;
    }
}
