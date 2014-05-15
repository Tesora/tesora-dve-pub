// OS_STATUS: public
package com.tesora.dve.db;

import com.tesora.dve.db.mysql.libmy.*;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnInfo;

public interface MysqlQueryResultConsumer {

	boolean emptyResultSet(MyOKResponse ok)
			throws PEException;

    void error(MyErrorResponse err)
            throws PEException;

    void fieldCount(MyColumnCount colCount)
            throws PEException;

    void field(int fieldIndex, MyFieldPktResponse columnDef, ColumnInfo columnProjection)
            throws PEException;

	void fieldEOF(MyMessage unknown)
			throws PEException;

    //TODO: rowText() and rowBinary() should probably share an abstract resultrow type, but previous code expected text or bin, and we don't have a great way to convert on a mismatch.
    void rowText(MyTextResultRow textRow)
            throws PEException;

    void rowBinary(MyBinaryResultRow textRow)
            throws PEException;

    /**
     * Called when the upstream processor has no more complete rows buffered, and cannot proceed without more packets from the sender.
     * @throws PEException
     */
    void rowFlush()
            throws PEException;

    void rowEOF(MyEOFPktResponse wholePacket)
            throws PEException;

}