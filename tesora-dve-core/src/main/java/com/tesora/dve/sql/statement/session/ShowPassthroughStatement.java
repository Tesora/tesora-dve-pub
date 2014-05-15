// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import org.apache.commons.lang.BooleanUtils;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.SessionExecutionStep;

public class ShowPassthroughStatement extends SessionStatement {
	public enum PassThroughCommandType {
		UNKNOWN("UNKNOWN", null),
		PLUGINS("PLUGINS", false), 
		MASTERLOGS("MASTER LOGS", null),
		MASTERSTATUS("MASTER STATUS", null),
		SLAVESTATUS("SLAVE STATUS", null),
		GRANTS("GRANTS", false) // requires no privilege for any user
		;

		private final String sqlCommand;
		private final Boolean overridePrivilege;
		
		private PassThroughCommandType(String command, Boolean overridePrivilege) {
			this.sqlCommand = command;
			this.overridePrivilege = overridePrivilege;
		}
		
		public String getSqlCommand() {
			return sqlCommand;
		}

		public Boolean getOverridePrivilege() {
			return overridePrivilege;
		}
	}

	private boolean allSites = true;
	private Boolean full = null;
	private PassThroughCommandType command = PassThroughCommandType.UNKNOWN;
	
	public ShowPassthroughStatement(PassThroughCommandType command, boolean allSites, Boolean full) {
		super();
		this.command = command;
		this.allSites = allSites;
		this.full = full;
	}

	public boolean hasFullSpec() {
		return full != null;
	}
	
	public boolean isFull() {
		return BooleanUtils.isTrue(full);
	}
	
	@Override
	public void plan(SchemaContext pc, ExecutionSequence es) throws PEException {
		// definitely not a passthrough - instead we want to send this down to all persistent sites
		// or maybe all sites
		PEStorageGroup sg = (allSites ? buildAllSitesGroup(pc,command.getOverridePrivilege()) : buildOneSiteGroup(pc,command.getOverridePrivilege()));
		// schedule as a qso:selectall - this won't work right, but in the future we're going to fix this
		// to also include the host
		es.append(new SessionExecutionStep(null, sg, getSQL(pc)));
	}

	public PassThroughCommandType getCommand() {
		return command;
	}
	
	public String emitShowSql() {
		StringBuffer buf = new StringBuffer();
		switch(command) {
		case GRANTS:
		case MASTERLOGS:
		case MASTERSTATUS:
		case SLAVESTATUS:
		case PLUGINS:
			buf.append("SHOW ").append(command.getSqlCommand());
			break;
		default:
			throw new SchemaException(Pass.PLANNER, "No support for SHOW command: " + command.getSqlCommand());
		}
		return buf.toString();
	}
	
	@Override
	public StatementType getStatementType() {
		StatementType st = super.getStatementType();
		
		switch(command) {
		case GRANTS:
			st = StatementType.SHOW_GRANTS;
			break;
		case MASTERLOGS:
		case MASTERSTATUS:
			st = StatementType.SHOW_MASTER_STATUS;
			break;
		case SLAVESTATUS:
			st = StatementType.SHOW_SLAVE_STATUS;
			break;
		case PLUGINS:
			st = StatementType.SHOW_PLUGINS;
			break;
		default:
			break;
		}
		return st;
	}
		
}
