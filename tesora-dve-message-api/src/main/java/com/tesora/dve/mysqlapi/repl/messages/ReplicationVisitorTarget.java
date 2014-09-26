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

import com.tesora.dve.exceptions.PEException;

/**
 *
 */
public interface ReplicationVisitorTarget {
    void visit(MyAppendBlockLogEvent packet) throws PEException;
    void visit(MyBeginLoadLogEvent packet) throws PEException;
    void visit(MyCreateFileLogEvent packet) throws PEException;
    void visit(MyDeleteFileLogEvent packet) throws PEException;
    void visit(MyExecLoadLogEvent packet) throws PEException;
    void visit(MyExecuteLoadLogEvent packet) throws PEException;
    void visit(MyFormatDescriptionLogEvent packet) throws PEException;
    void visit(MyIntvarLogEvent packet) throws PEException;
    void visit(MyLoadLogEvent packet) throws PEException;
    void visit(MyNewLoadLogEvent packet) throws PEException;
    void visit(MyQueryLogEvent packet) throws PEException;
    void visit(MyRandLogEvent packet) throws PEException;
    void visit(MyRotateLogEvent packet) throws PEException;
    void visit(MyStopLogEvent packet) throws PEException;
    void visit(MyTableMapLogEvent packet) throws PEException;
    void visit(MyUserVarLogEvent packet) throws PEException;
    void visit(MyXIdLogEvent packet) throws PEException;
    void visit(MyReplEvent packet) throws PEException;

    void visit(MyUnknownLogPayload packet) throws PEException;

    void visit(MyLogDeleteRowsPayload myLogDeleteRowsPayload) throws PEException;
    void visit(MyLogUpdateRowsPayload myLogUpdateRowsPayload) throws PEException;
    void visit(MyLogWriteRowsPayload myLogWriteRowsPayload) throws PEException;
}
