package com.tesora.dve.common.logutil;

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

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.common.PEConstants;


// responsible for collecting a set of individual executions
public class StructuredExecutionLogger implements ExecutionLogger {

	protected long start = -1;
	protected long end = -1;
	
	protected LogSubject subject;
	
	protected List<ExecutionLogger> subloggers = new ArrayList<ExecutionLogger>();
	
	public StructuredExecutionLogger(LogSubject subj) {
		subject = subj;
		begin();
	}
	
	public List<ExecutionLogger> getSubLoggers() {
		return subloggers;
	}
	
	public ExecutionLogger getCurrentSubLogger() {
		if (subloggers.isEmpty())
			return null;
		return subloggers.get(subloggers.size() - 1);
	}

	public ExecutionLogger buildNewLogger(LogSubject subj) {
		return new StructuredExecutionLogger(subj);
	}
	
	@Override
	public ExecutionLogger getNewLogger(LogSubject subj) {
		return addNewLogger(buildNewLogger(subj));
	}
	
	public ExecutionLogger addNewLogger(ExecutionLogger el) {
		ExecutionLogger current = getCurrentSubLogger();
		if (current == null || current.completed() || !(current instanceof StructuredExecutionLogger)) {
			subloggers.add(el);
		} else {
			StructuredExecutionLogger sel = (StructuredExecutionLogger) current;
			sel.addNewLogger(el);
		}
		return el;
	}
	
	@Override
	public ExecutionLogger getNewLogger(String message) {
		return addNewLogger(new StructuredExecutionLogger(new AdHocSubject(message)));
	}
	
	@Override
	public void begin() {
		start = System.nanoTime();
	}

	@Override
	public void end() {
		end = System.nanoTime();
	}

	@Override
	public boolean completed() {
		return start > -1 && end > -1;
	}

	@Override
	public long getDelta() {
		return end - start;
	}

	@Override
	public boolean isEnabled() {
		return true;
	}

	@Override
	public LogSubject getSubject() {
		return subject;
	}
	
	protected void accumulateLog(StringBuilder buf, String in) {
		buf.append(in);
		buf.append(PEConstants.LINE_SEPARATOR);
	}	

	protected String getSelfLog(int level, String[] tags, StringBuilder buf, int offset, String indent) {
		String t = (level < tags.length ? tags[level] : "unknown");
		StringBuilder line = new StringBuilder();
		line.append(indent);
		if (level > 0) 
			// have to emit the offset
			line.append("[").append(offset).append("] ");
		line.append(t).append("=").append(getDelta()).append("; ").append(getSubject().describeForLog());
		return line.toString();
	}
	
	protected void accumulateDescendantLog(int level, String[] tags, StringBuilder buf, String indent) {
		int suboff = 0;
		String subindent = indent + "  ";
		for(ExecutionLogger el : subloggers) {
			if (el instanceof StructuredExecutionLogger) {
				StructuredExecutionLogger sel = (StructuredExecutionLogger) el;
				sel.accumulateLogMessage(level + 1, tags, buf, ++suboff, subindent);
			}
		}		
	}
	
	protected void accumulateLogMessage(int level, String[] tags, StringBuilder buf, int offset, String indent) {
		accumulateLog(buf,getSelfLog(level,tags,buf,offset,indent));
		accumulateDescendantLog(level, tags, buf, indent);
	}
}
