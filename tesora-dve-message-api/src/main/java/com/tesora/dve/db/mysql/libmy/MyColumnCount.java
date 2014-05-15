// OS_STATUS: public
package com.tesora.dve.db.mysql.libmy;

import com.tesora.dve.db.mysql.common.MysqlAPIUtils;
import io.netty.buffer.ByteBuf;

public class MyColumnCount extends MyResponseMessage {
    int columnCount;


    @Override
    public MyMessageType getMessageType() {
        return MyMessageType.RESULTSET_RESPONSE;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public void setColumnCount(int columnCount) {
        this.columnCount = columnCount;
    }

    @Override
    public void marshallMessage(ByteBuf cb) {
        MysqlAPIUtils.putLengthCodedLong(cb,columnCount);
    }

    @Override
    public void unmarshallMessage(ByteBuf cb) {
        columnCount = (int)MysqlAPIUtils.getLengthCodedLong(cb);
    }
}
