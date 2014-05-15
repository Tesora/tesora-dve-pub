// OS_STATUS: public
package com.tesora.dve.worker;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.db.NativeResultHandler;
import com.tesora.dve.db.NativeTypeCatalog;
import com.tesora.dve.db.mysql.FieldMetadataAdapter;
import com.tesora.dve.db.mysql.libmy.MyMessage;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

import com.tesora.dve.charset.mysql.MysqlNativeCharSet;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.concurrent.PEFuture;
import com.tesora.dve.concurrent.PEPromise;
import com.tesora.dve.db.DBConnection;
import com.tesora.dve.db.mysql.MysqlExecuteCommand;
import com.tesora.dve.db.mysql.libmy.MyEOFPktResponse;
import com.tesora.dve.db.mysql.libmy.MyTextDataResponse;
import com.tesora.dve.db.mysql.MSPResultSetResponse;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.server.messaging.SQLCommand;

public class MysqlTextResultForwarder extends MysqlDemultiplexingResultForwarder {

	public MysqlTextResultForwarder(ChannelHandlerContext outboundCtx, byte sequenceId) {
		super(outboundCtx, sequenceId);
	}

	@Override
	public void inject(ColumnSet metadata, List<ResultRow> rows) throws PEException {

		outboundCtx.write(new MSPResultSetResponse(metadata.size()).withPacketNumber(++sequenceId));

		DBNative dbNative = Singletons.require(HostService.class).getDBNative();
		NativeTypeCatalog nativeTypeCatalog = dbNative.getTypeCatalog();

		for (ColumnMetadata cm : metadata.getColumnList()) {
			//                MSPFieldResponse mspFieldResponse = new MSPFieldResponse(cm, nativeTypeCatalog, MysqlNativeCharSet.UTF8);
			MyMessage mspFieldResponse = FieldMetadataAdapter.buildPacket(cm, nativeTypeCatalog, MysqlNativeCharSet.UTF8);
			outboundCtx.write(mspFieldResponse.withPacketNumber(++sequenceId));
		}
		outboundCtx.write(new MyEOFPktResponse().withPacketNumber(++sequenceId));

		for (ResultRow row : rows) {
			NativeResultHandler resultHandler = Singletons.require(HostService.class).getDBNative().getResultHandler();
			outboundCtx.write(new MyTextDataResponse(resultHandler,metadata, row).withPacketNumber(++sequenceId));
		}
		outboundCtx.flush();

		super.setHasResults();
	}

	@Override
	public PEFuture<Boolean> writeCommandExecutor(Channel channel, StorageSite site, DBConnection.Monitor connectionMonitor, SQLCommand sql, PEPromise<Boolean> promise) {
		channel.write(new MysqlExecuteCommand(sql, connectionMonitor, this, promise));
		return promise;
	}

}
