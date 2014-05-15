// OS_STATUS: public
package com.tesora.dve.comms.client.messages;


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
