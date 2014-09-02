package com.tesora.dve.mysqlapi.repl;

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

import com.google.common.primitives.UnsignedLong;
import com.tesora.dve.dbc.ServerDBConnection;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.mysqlapi.repl.messages.*;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.variable.VariableConstants;
import io.netty.util.CharsetUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;

/**
 *
 */
public class MyReplicationVisitorDispatch implements ReplicationVisitorTarget {
    static final Logger logger = Logger.getLogger(MyReplicationVisitorDispatch.class);
    MyReplicationSlaveService plugin;

    public MyReplicationVisitorDispatch(MyReplicationSlaveService plugin) {
        this.plugin = plugin;
    }

    @Override
    public void visit(MyAppendBlockLogEvent packet) throws PEException {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("** START AppendBlock Log Event **");
                logger.debug("File id = " + packet.getFileID() + ", size of block = " + packet.getDataBlock().readableBytes());
                logger.debug("** END AppendBlock Log Event **");
            }

            plugin.getInfileHandler().addBlock(packet.getFileID(), packet.getDataBlock().array());

            updateBinLogPosition(packet,plugin);

        } catch (IOException e) {
            throw new PEException("Received APPEND_BLOCK_EVENT but cannot add to infile.", e);
        }
    }

    @Override
    public void visit(MyBeginLoadLogEvent packet) throws PEException {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("** START BeginLoadLog Event **");
                logger.debug("File id = " + packet.getFileId());
                logger.debug("** END BeginLoadLog Event **");
            }

            plugin.getInfileHandler().createInfile(packet.getFileId());
            plugin.getInfileHandler().addInitialBlock(packet.getFileId(), packet.getDataBlock().array());

            updateBinLogPosition(packet,plugin);

        } catch (Exception e) {
            throw new PEException("Receive BEGIN_LOAD_QUERY_EVENT but cannot create new infile.", e);
        }
    }

    @Override
    public void visit(MyCreateFileLogEvent packet) throws PEException {
        logger.warn("Message is parsed but no handler is implemented for log event type: CREATE_FILE_EVENT");

    }

    @Override
    public void visit(MyDeleteFileLogEvent packet) throws PEException {
        logger.warn("Message is parsed but no handler is implemented for log event type: DELETE_FILE_EVENT");
    }

    @Override
    public void visit(MyExecLoadLogEvent packet) throws PEException {
        logger.warn("Message is parsed but no handler is implemented for log event type: EXEC_LOAD_EVENT");

    }

    @Override
    public void visit(MyExecuteLoadLogEvent packet) throws PEException {
        boolean switchToDb = true;
        ServerDBConnection conn = null;
        String dbName = packet.getDbName();
        String origQuery = packet.getOrigQuery();

        try {
            if (!this.includeDatabase(plugin, dbName)) {
                // still want to update log position if we filter out message
                updateBinLogPosition(packet,plugin);
                return;
            }

            conn = plugin.getServerDBConnection();

            // If any session variables are to be set do it first
            plugin.getSessionVariableCache().setAllSessionVariableValues(conn);

            if (logger.isDebugEnabled()) {
                logger.debug("** START ExecuteLoadLog Event **");
                if ( switchToDb ) logger.debug("USE " + dbName);
                logger.debug(origQuery);
                logger.debug("** END ExecuteLoadLog Event **");
            }

            if ( switchToDb ) conn.setCatalog(dbName);

            // since we don't want to parse here to determine if a time function is specified
            // set the TIMESTAMP variable to the master statement execution time
            conn.executeUpdate("set " + VariableConstants.REPL_SLAVE_TIMESTAMP_NAME + "=" + packet.getCommonHeader().getTimestamp());

            conn.executeLoadDataRequest(plugin.getClientConnectionContext().getCtx(), origQuery.getBytes(CharsetUtil.UTF_8));

            // start throwing down the bytes from the load data infile
            File infile = plugin.getInfileHandler().getInfile(packet.getFileId());

            FileInputStream in = null;
            byte[] readData = new byte[MyExecuteLoadLogEvent.MAX_BUFFER_LEN];
            try {
                in = new FileInputStream(infile);
                int len = in.read(readData);
                do {
                    conn.executeLoadDataBlock(plugin.getClientConnectionContext().getCtx(),
                            (len == MyExecuteLoadLogEvent.MAX_BUFFER_LEN) ? readData : ArrayUtils.subarray(readData, 0, len));
                    len = in.read(readData);
                } while (len > -1);
                conn.executeLoadDataBlock(plugin.getClientConnectionContext().getCtx(), ArrayUtils.EMPTY_BYTE_ARRAY);
            } finally {
                IOUtils.closeQuietly(in);
                plugin.getInfileHandler().cleanUp();
            }

            updateBinLogPosition(packet,plugin);

        } catch (Exception e) {
            if (plugin.validateErrorAndStop(packet.getErrorCode(), e)) {
                logger.error("Error occurred during replication processing: ",e);
                try {
                    conn.execute("ROLLBACK");
                } catch (SQLException e1) {
                    throw new PEException("Error attempting to rollback after exception",e); // NOPMD by doug on 18/12/12 8:07 AM
                }
            } else {
                packet.setSkipErrors(true, "Replication Slave failed processing: '" + origQuery
                        + "' but slave_skip_errors is active. Replication processing will continue");
            }
            throw new PEException("Error executing: " + origQuery,e);
        } finally { // NOPMD by doug on 18/12/12 8:08 AM
            // Clear all the session variables since they are only good for one
            // event
            plugin.getSessionVariableCache().clearAllSessionVariables();
        }
    }

    @Override
    public void visit(MyFormatDescriptionLogEvent packet) throws PEException {
        String binLogVerTypeString = StringUtils.EMPTY;
        switch (MyFormatDescriptionLogEvent.MyBinLogVerType.fromByte((byte) packet.getBinaryLogVersion())) {
            case MySQL_3_23:
                binLogVerTypeString = "MySQL_3_23(" + MyFormatDescriptionLogEvent.MyBinLogVerType.MySQL_3_23 + ")";
                break;

            case MySQL_4_0_2_to_4_1:
                binLogVerTypeString = "MySQL_4_0_2_to_4_1(" + MyFormatDescriptionLogEvent.MyBinLogVerType.MySQL_4_0_2_to_4_1 + ")";
                break;

            case MySQL_5_0:
                binLogVerTypeString = "MySQL_5_0(" + MyFormatDescriptionLogEvent.MyBinLogVerType.MySQL_5_0 + ")";
                break;

            default:
                break;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("** START Format Description Event **");
            logger.debug("Bin Log Ver: " + binLogVerTypeString);
            for (MyLogEventType levt : packet.getEventTypeValues().keySet()) {
                logger.debug("Variable(value): " + levt.name() + "(" + packet.getEventTypeValues().get(levt) + ")");
            }
            logger.debug("** END Format Description Event **");
        }

        try {
            updateBinLogPosition(packet,plugin);
        } catch (PEException e) {
            logger.error("Error updating binlog from Format Description Event.", e);
            // TODO I think we really need to stop the service in this case
            throw new PEException("Error updating bin log position", e);
        }

    }

    @Override
    public void visit(MyIntvarLogEvent packet) throws PEException {
        boolean lastInsertIdEvent = (MyIntvarLogEvent.MyIntvarEventVariableType.fromByte(packet.getVariableType()) == MyIntvarLogEvent.MyIntvarEventVariableType.LAST_INSERT_ID_EVENT);
        if (logger.isDebugEnabled()) {
            logger.debug("** START Intvar Event **");
            logger.debug("Var Type: "
                    + ( lastInsertIdEvent ? "LAST_INSERT_ID_EVENT("
                    + MyIntvarLogEvent.MyIntvarEventVariableType.LAST_INSERT_ID_EVENT
                    + ")"
                    : "INSERT_ID_EVENT("
                    + MyIntvarLogEvent.MyIntvarEventVariableType.INSERT_ID_EVENT
                    + ")"));
            logger.debug("Var Value: " + packet.getVariableValue());
            logger.debug("** END Intvar Event **");
        }
        plugin.getSessionVariableCache().setIntVarValue(packet.getVariableType(), packet.getVariableValue());
    }

    @Override
    public void visit(MyLoadLogEvent packet) throws PEException {
        logger.warn("Message is parsed but no handler is implemented for log event type: LOAD_EVENT");
    }

    @Override
    public void visit(MyNewLoadLogEvent packet) throws PEException {
        logger.warn("Message is parsed but no handler is implemented for log event type: NEW_LOAD_EVENT");
    }

    @Override
    public void visit(MyQueryLogEvent packet) throws PEException {
        boolean switchToDb = true;
        ServerDBConnection conn = null;
        String dbName = packet.getDbName();
        String origQuery = packet.getOrigQuery();
        try {
            if (!this.includeDatabase(plugin, dbName)) {
                // still want to update log position if we filter out message
                updateBinLogPosition(packet,plugin);
                return;
            }

            if (StringUtils.startsWithIgnoreCase(origQuery, "CREATE DATABASE")
                    || StringUtils.startsWithIgnoreCase(origQuery, "DROP DATABASE")) {
                switchToDb = false;
            }

            conn = plugin.getServerDBConnection();

            // If any session variables are to be set do it first
            plugin.getSessionVariableCache().setAllSessionVariableValues(conn);

            if (logger.isDebugEnabled()) {
                logger.debug("** START QueryLog Event **");
                if ( switchToDb ) logger.debug("USE " + dbName);
                logger.debug(origQuery);
                logger.debug("** END QueryLog Event **");
            }

            if ( switchToDb ) conn.setCatalog(dbName);

            // since we don't want to parse here to determine if a time function is specified
            // set the TIMESTAMP variable to the master statement execution time
            conn.executeUpdate("set " + VariableConstants.REPL_SLAVE_TIMESTAMP_NAME + "=" + packet.getCommonHeader().getTimestamp());

            boolean unset = handleAutoIncrement(conn,plugin.getSessionVariableCache().getIntVarValue());
            conn.executeUpdate(packet.getQuery().array());
            if (unset)
                conn.executeUpdate("set " + VariableConstants.REPL_SLAVE_INSERT_ID_NAME + "=null");

            updateBinLogPosition(packet,plugin);

        } catch (Exception e) {
            if (plugin.validateErrorAndStop(packet.getErrorCode(), e)) {
                logger.error("Error occurred during replication processing: ",e);
                try {
                    conn.execute("ROLLBACK");
                } catch (SQLException e1) {
                    throw new PEException("Error attempting to rollback after exception",e); // NOPMD by doug on 18/12/12 8:07 AM
                }
            } else {

                packet.setSkipErrors(true,"Replication Slave failed processing: '" + origQuery
                        + "' but slave_skip_errors is active. Replication processing will continue");
            }
            throw new PEException("Error executing: " + origQuery,e);
        } finally { // NOPMD by doug on 18/12/12 8:08 AM
            // Clear all the session variables since they are only good for one
            // event
            plugin.getSessionVariableCache().clearAllSessionVariables();
        }
    }

    boolean handleAutoIncrement(ServerDBConnection conn, Pair<Byte, UnsignedLong> intVarValue) throws SQLException {
        if (intVarValue.getFirst() != null && intVarValue.getSecond() != null &&
                intVarValue.getFirst() == MyIntvarLogEvent.MyIntvarEventVariableType.INSERT_ID_EVENT.getByteValue()) {

            conn.executeUpdate("set " + VariableConstants.REPL_SLAVE_INSERT_ID_NAME + "=" + intVarValue.getSecond().toString());
            return true;
        }
        return false;
    }

    @Override
    public void visit(MyRandLogEvent packet) throws PEException {
        if (logger.isDebugEnabled()) {
            logger.debug("** START Rand Event **");
            logger.debug("seed1="+packet.getSeed1().toString());
            logger.debug("seed2=" + packet.getSeed2().toString());
            logger.debug("** END Rand Event **");
        }
        plugin.getSessionVariableCache().setRandValue(packet.getSeed1(),packet.getSeed2());
    }

    @Override
    public void visit(MyRotateLogEvent packet) throws PEException {
        if (logger.isDebugEnabled()) {
            logger.debug("** START Rotate Event **");
            logger.debug("Position: " + packet.getPosition());
            logger.debug("New Log File: " + packet.getNewLogFileName());
            logger.debug("** END Rotate Event **");
        }

        plugin.getSessionVariableCache().setRotateLogValue(packet.getNewLogFileName());
        plugin.getSessionVariableCache().setRotateLogPositionValue(packet.getPosition());

        try {
            updateBinLogPosition(packet,plugin);
        } catch (PEException e) {
            logger.error("Error updating binlog from Rotate Log Event.", e);
            // TODO I think we really need to stop the service in this case
            throw new PEException("Error updating bin log position",e);
        }
    }

    @Override
    public void visit(MyStopLogEvent packet) throws PEException {
        if ( logger.isDebugEnabled() )
            logger.debug("** Stop Event: NO BODY **");

        try {
            updateBinLogPosition(packet,plugin);
        } catch (PEException e) {
            logger.error("Error updating binlog from Stop Log Event.", e);
            throw new PEException("Error updating bin log position",e);
        }

    }

    @Override
    public void visit(MyTableMapLogEvent packet) throws PEException {
        logger.warn("Message is parsed but no handler is implemented for log event type: TABLE_MAP_EVENT");
    }

    @Override
    public void visit(MyUserVarLogEvent packet) throws PEException {
        if (logger.isDebugEnabled()) {
            logger.debug("** START UserVarLog Event **");
            logger.debug("Var Name: " + packet.getVariableName());
            logger.debug("Var Value: " + packet.getVariableValue());
            logger.debug("** END UserVarLog Event **");
        }
        plugin.getSessionVariableCache().setUserVariable(new Pair<String, String>(packet.getVariableName(),packet.getVariableValue()));

    }

    @Override
    public void visit(MyXIdLogEvent packet) throws PEException {
        if ( logger.isDebugEnabled() )
            logger.debug("COMMIT (from XId event)");

        try {
            updateBinLogPosition(packet,plugin);

            plugin.getServerDBConnection().executeUpdate("COMMIT");
        } catch (Exception e) {
            logger.error("Error occurred processing XID log event: ",e);
            try {
                plugin.getServerDBConnection().execute("ROLLBACK");
            } catch (SQLException e1) {
                throw new PEException("Error attempting to rollback after exception",e); // NOPMD by doug on 18/12/12 8:08 AM
            }
            throw new PEException("Error executing executing commit.",e);
        } finally { // NOPMD by doug on 18/12/12 8:08 AM
            // Clear all the session variables since they are only good for one
            // event
            plugin.getSessionVariableCache().clearAllSessionVariables();
        }
    }

    @Override
    public void visit(MyReplEvent packet) throws PEException {
        plugin.setLastEventTimestamp(packet.getCommonHeader().getTimestamp());
        MyLogEventPacket levp = packet.getLogEventPacket();
        try {
            levp.accept(this);
        } catch (PEException e) {
            if ( levp.skipErrors() ) {
                logger.error(levp.getSkipErrorMessage(), e);
            } else {
                logger.error("Exception encountered processing Replication Events - stopping service "
                        + plugin.getSlaveInfo(), e);

                // if we get an exception while processing Replication
                // Events we need to stop the service
                // unless stop has already been called...
                if (!plugin.stopCalled()) {
                    plugin.stop();
                }
                // make sure we re-throw the exception
                throw e;
            }
        }
    }


    public static void updateBinLogPosition(MyLogEventPacket logEvent, MyReplicationSlaveService plugin) throws PEException {
        if (!logEvent.isSaveBinaryLogPosition()) {
            return;
        }

        try {
            Long position = logEvent.getCommonHeader().getMasterLogPosition();
            MyReplSessionVariableCache svc = plugin.getSessionVariableCache();

            if (svc.getRotateLogPositionValue() != null) {
                position = svc.getRotateLogPositionValue();
                // clear out the value
                svc.setRotateLogPositionValue(null);
            }

            if (position >= 4) {
                // update only if valid position
                MyBinLogPosition myBLPos = new MyBinLogPosition(plugin.getMasterHost(),
                        svc.getRotateLogValue(), position);
                plugin.updateBinLogPosition(myBLPos);
                logger.debug("Updating binary log position: " + myBLPos.getMasterHost() + ", "
                        + myBLPos.getFileName() + ", " + myBLPos.getPosition());
            }
        } catch (Exception e) {
            throw new PEException ("Error updating bin log position.",e);
        }
    }

    public boolean includeDatabase(MyReplicationSlaveService plugin, String dbName) {
        boolean ret = plugin.includeDatabase(dbName);

        if (!ret) {
            logger.debug("Filtered event for '" + dbName + "'");
        }

        return ret;
    }


}