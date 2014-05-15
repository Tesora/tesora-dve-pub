// OS_STATUS: public
package com.tesora.dve.server.messaging;

import java.sql.SQLException;

import javax.transaction.xa.XAException;

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.comms.client.messages.ExecuteResponse;
import com.tesora.dve.comms.client.messages.MessageType;
import com.tesora.dve.comms.client.messages.MessageVersion;
import com.tesora.dve.comms.client.messages.ResponseMessage;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSContext;
import com.tesora.dve.server.statistics.SiteStatKey.OperationClass;
import com.tesora.dve.server.statistics.manager.LogSiteStatisticRequest;
import com.tesora.dve.worker.Worker;
import com.tesora.dve.worker.WorkerStatement;

public class WorkerAlterDatabaseRequest extends WorkerRequest {

	private static final long serialVersionUID = -1638470828106328178L;
	private static final Logger logger = Logger.getLogger(WorkerAlterDatabaseRequest.class);

	private final UserDatabase alteredDatabase;

	public WorkerAlterDatabaseRequest(final SSContext ctx, final UserDatabase alteredDatabase) {
		super(ctx);
		this.alteredDatabase = alteredDatabase;
	}

	@Override
	public ResponseMessage executeRequest(final Worker w, final DBResultConsumer resultConsumer) throws SQLException, PEException, XAException {
		final String onSiteName = this.alteredDatabase.getNameOnSite(w.getWorkerSite());

		final String defaultCharSet = alteredDatabase.getDefaultCharacterSetName();
		final String defaultCollation = alteredDatabase.getDefaultCollationName();

        final SQLCommand ddl = Singletons.require(HostService.class).getDBNative().getAlterDatabaseStmt(onSiteName, defaultCharSet, defaultCollation);
		if (logger.isDebugEnabled()) {
			logger.debug(w.getName() + ": current database is " + w.getCurrentDatabaseName());
			logger.debug(w.getName() + ": executing statement " + ddl);
		}

		final WorkerStatement stmt = w.getStatement();
		stmt.execute(getConnectionId(), ddl, resultConsumer);

		return new ExecuteResponse(false, resultConsumer.getUpdateCount(), null).from(w.getAddress()).success();
	}

	@Override
	public LogSiteStatisticRequest getStatisticsNotice() {
		return new LogSiteStatisticRequest(OperationClass.DDL);
	}

	@Override
	public MessageType getMessageType() {
		return MessageType.W_ALTER_DB_REQUEST;
	}

	@Override
	public MessageVersion getVersion() {
		return MessageVersion.VERSION1;
	}

	@Override
	public String toString() {
		final String thisClassName = this.getClass().getSimpleName();
		return thisClassName.concat("(").concat(this.alteredDatabase.getName()).concat(")");
	}

}
