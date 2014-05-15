// OS_STATUS: public
package com.tesora.dve.db.mysql.portal.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;

/**
 *
 */
public interface MSPMessage extends ReferenceCounted {
    byte getMysqlMessageType();
    byte getSequenceID();
    void setSequenceID(byte sequence);
    MSPMessage newPrototype(byte sequenceID, ByteBuf source);



    ByteBuf unwrap();
    void writeTo(ByteBuf destination);
}
