// OS_STATUS: public
package com.tesora.dve.db.mysql;

import com.tesora.dve.db.mysql.common.DBTypeBasedUtils;
import com.tesora.dve.db.mysql.portal.protocol.MSPComStmtExecuteRequestMessage;
import com.tesora.dve.db.mysql.portal.protocol.MysqlGroupedPreparedStatementId;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.Charset;
import java.util.List;

import org.apache.log4j.Logger;

import com.tesora.dve.concurrent.PEPromise;
import com.tesora.dve.db.MysqlQueryResultConsumer;
import com.tesora.dve.db.DBConnection.Monitor;
import com.tesora.dve.db.mysql.libmy.MyParameter;
import com.tesora.dve.db.mysql.libmy.MyPreparedStatement;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.SQLCommand;

public class MysqlStmtExecuteCommand extends MysqlExecuteCommand {

	static Logger logger = Logger.getLogger( MysqlStmtExecuteCommand.class );

	private MyPreparedStatement<MysqlGroupedPreparedStatementId> pstmt;
	private List<Object> params;

	public MysqlStmtExecuteCommand(SQLCommand sql, Monitor connectionMonitor, MyPreparedStatement<MysqlGroupedPreparedStatementId> pstmt, List<Object> params,
			MysqlQueryResultConsumer resultConsumer, PEPromise<Boolean> promise) {
		super(sql, connectionMonitor, resultConsumer, promise);
		this.pstmt = pstmt;
		this.params = params;
	}
	
	@Override
	public void execute(ChannelHandlerContext ctx, Charset charset) throws PEException {
		// Make sure the parameter types in the param metadata match the types of the objects in
		// the parameter values
		if (pstmt.getNumParams() > 0) {
			MyFieldType mft;
			for (int i = 0; i < params.size(); ++i) {
				if (params.get(i) != null) {
					mft = DBTypeBasedUtils.getJavaTypeFunc(params.get(i).getClass()).getMyFieldType();
					if (pstmt.getParameter(i + 1).getType() != mft)
						pstmt.setParameter(i + 1, new MyParameter(mft));
				} else {
					pstmt.setParameter(i + 1, new MyParameter(MyFieldType.FIELD_TYPE_NULL));
				}
			}
		}
		if (logger.isDebugEnabled())
			logger.debug("Written: " + this);

        MSPComStmtExecuteRequestMessage exec = MSPComStmtExecuteRequestMessage.newMessage(pstmt.getStmtId().getStmtId(ctx.channel()), pstmt, params);
        ctx.write(exec);
    }
}
