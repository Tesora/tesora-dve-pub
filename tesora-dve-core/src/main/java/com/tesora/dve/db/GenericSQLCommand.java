package com.tesora.dve.db;

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

import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.node.expression.DelegatingLiteralExpression;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.schema.cache.IDelegatingLiteralExpression;
import com.tesora.dve.sql.schema.cache.ILiteralExpression;
import com.tesora.dve.sql.schema.cache.IParameter;
import com.tesora.dve.sql.statement.StatementType;

// a generic sql command is what would be emitted by the emitter upto any literals
// instead it uses a 'format' string (non literal parts of the final result)
// and a list of offsets for where literals exist.
public class GenericSQLCommand {
	
	public interface DBNameResolver {
		String getNameOnSite(String dbName);

		int getSiteIndex();

		StorageSite getWorkerSite();
	}

	private byte[] format;
	private OffsetEntry[] entries;
	
	private StatementType type;
	
	private Boolean isUpdate = null;
	private Boolean hasLimit = null;
	
	public GenericSQLCommand(String format, OffsetEntry[] offsets, StatementType stmtType, Boolean isUpdate, Boolean hasLimit) {
		this(format.getBytes(CharsetUtil.ISO_8859_1), offsets, stmtType, isUpdate, hasLimit);
	}
	
	public GenericSQLCommand(byte[] format, OffsetEntry[] offsets, StatementType stmtType, Boolean isUpdate, Boolean hasLimit) {
		this.format = format;
		this.entries = offsets;
		this.type = stmtType;
		this.isUpdate = isUpdate;
		this.hasLimit = hasLimit;
	}
	
	public GenericSQLCommand(String format) {
		this(format, new OffsetEntry[0], null, null, null);
	}

	public GenericSQLCommand(byte[] format) {
		this(format, new OffsetEntry[0], null, null, null);
	}

	public String getUnresolved() {
		return new String(getUnresolvedAsBytes());
	}
	
	public byte[] getUnresolvedAsBytes() {
		return format;
	}
	
	public boolean hasLateResolution() {
		return entries.length > 0;
	}

	public SQLCommand getSQLCommand() {
		return new SQLCommand(this);
	}
	
	// for fetch support
	public GenericSQLCommand modify(String toAppend) {
		return new GenericSQLCommand(format + toAppend,entries, type, isUpdate, hasLimit);
	}
	
	private static final String forUpdate = "FOR UPDATE";
	
	// also for fetch support
	public GenericSQLCommand stripForUpdate() {
		if (!isUpdate) return this;
		String formatStr = new String(format);
		int offset = formatStr.indexOf(forUpdate);
		StringBuilder out = new StringBuilder();
		out.append(formatStr.substring(0, offset - 1));
		out.append(" ");
		out.append(formatStr.substring(offset + forUpdate.length()));
		return new GenericSQLCommand(out.toString(), entries, type, false, hasLimit);
	}
	
	public GenericSQLCommand resolve(SchemaContext sc, String prettyIndent) {
		return resolve(sc, false, prettyIndent);
	}

	private static final byte[] spaceAsBytes = " ".getBytes();
	private static final byte[] singleQuoteAsBytes = "'".getBytes();
	private static final byte[] nullAsBytes = "null".getBytes();
	private static final byte[] questionMarkAsBytes = "?".getBytes();
	
	// TODO we really need a function specific class to handle resolve/display
	
	public GenericSQLCommand resolve(SchemaContext sc, boolean preserveParamMarkers, String indent) {
		if (entries.length == 0) return this;
		List<OffsetEntry> downstream = new ArrayList<OffsetEntry>();
		List<byte[]> sqlFragments = new ArrayList<byte[]>();
        Emitter emitter = Singletons.require(HostService.class).getDBNative().getEmitter();
		int offset = 0;
		for(OffsetEntry oe : entries) {
			int index = oe.getOffset();
			String tok = oe.getToken();
			sqlFragments.add(ArrayUtils.subarray(format,offset,index));
			if (oe.getKind().isLate()) {
				// still need to get the next part of the format
				downstream.add(oe.makeAdjusted(getTotalBytes(sqlFragments)));				
				sqlFragments.add(oe.getToken().getBytes());
			} else if (oe.getKind() == EntryKind.LITERAL) {
				LiteralOffsetEntry loe = (LiteralOffsetEntry) oe;
				StringBuilder buf = new StringBuilder();
				emitter.emitLiteral(sc, loe.getLiteral(), buf);
				sqlFragments.add(buf.toString().getBytes());
			} else if (oe.getKind() == EntryKind.PRETTY) {
				int totalBytes = getTotalBytes(sqlFragments);
				if (indent != null) {
					PrettyOffsetEntry poe = (PrettyOffsetEntry)oe;
					StringBuilder buf = new StringBuilder();
					if (totalBytes > 0)
						buf.append(PEConstants.LINE_SEPARATOR);
					poe.addIndent(buf, indent);
					sqlFragments.add(buf.toString().getBytes());
				} else {
					if (totalBytes > 0)
						sqlFragments.add(spaceAsBytes);
				}
			} else if (oe.getKind() == EntryKind.TEMPTABLE) {
				TempTableOffsetEntry ttoe = (TempTableOffsetEntry) oe;
				sqlFragments.add(ttoe.getTempTable().getName(sc).get().getBytes());
			} else if (oe.getKind() == EntryKind.PARAMETER) {
				ParameterOffsetEntry poe = (ParameterOffsetEntry) oe;
				if (!preserveParamMarkers) {
					// get the expr from the expr manager and swap it in
					Object o = sc.getValueManager().getValue(sc, poe.getParameter());
					if (o!=null) {
						if (o.getClass().isArray()) {
							sqlFragments.add(singleQuoteAsBytes);
							sqlFragments.add((byte[])o);
							sqlFragments.add(singleQuoteAsBytes);
						} else {
							sqlFragments.add(String.valueOf(o).getBytes());
						}
					} else { 
						sqlFragments.add(nullAsBytes);
					}
				} else {
					sqlFragments.add(questionMarkAsBytes);
				}
			} else if (oe.getKind() == EntryKind.LATEVAR) {
				LateResolvingVariableOffsetEntry lrvoe = (LateResolvingVariableOffsetEntry) oe;
				Object value = lrvoe.expr.getValue(sc);
				String s = (value == null ? "null" : "'" + value.toString() + "'");
				sqlFragments.add(s.getBytes());
			}
			offset = index + tok.length();
		}
		sqlFragments.add(ArrayUtils.subarray(format, offset, format.length));
		OffsetEntry[] does = downstream.toArray(new OffsetEntry[0]);
		return new GenericSQLCommand(concatSQLFragments(sqlFragments), does, type, isUpdate, hasLimit);
	}
	
	public void display(SchemaContext sc, boolean preserveParamMarkers, String indent, List<String> lines) {
		if (entries.length == 0) {
			lines.add(new String(format));
			return;
		}
		List<byte[]> sqlFragments = new ArrayList<byte[]>();
        Emitter emitter = Singletons.require(HostService.class).getDBNative().getEmitter();
		int offset = 0;
		for(OffsetEntry oe : entries) {
			int index = oe.getOffset();
			String tok = oe.getToken();
			byte[] originalPart = ArrayUtils.subarray(format,offset,index);
			if (originalPart.length > 0) {
				sqlFragments.add(originalPart);
			}
			if (oe.getKind().isLate()) {
				// still need to get the next part of the format
				sqlFragments.add(oe.getToken().getBytes());
			} else if (oe.getKind() == EntryKind.LITERAL) {
				LiteralOffsetEntry loe = (LiteralOffsetEntry) oe;
				StringBuilder buf = new StringBuilder();
				emitter.emitLiteral(sc, loe.getLiteral(), buf);
				sqlFragments.add(buf.toString().getBytes());
			} else if (oe.getKind() == EntryKind.PRETTY) {
				// tie off the old one
				if (getTotalBytes(sqlFragments) > 0) {
					lines.add(new String(concatSQLFragments(sqlFragments)));
				}
				sqlFragments.clear();
				PrettyOffsetEntry poe = (PrettyOffsetEntry)oe;
				StringBuilder buf = new StringBuilder();
				poe.addIndent(buf, indent);
				sqlFragments.add(buf.toString().getBytes());
			} else if (oe.getKind() == EntryKind.TEMPTABLE) {
				TempTableOffsetEntry ttoe = (TempTableOffsetEntry) oe;
				sqlFragments.add(ttoe.getTempTable().getName(sc).get().getBytes());
			} else if (oe.getKind() == EntryKind.PARAMETER) {
				ParameterOffsetEntry poe = (ParameterOffsetEntry) oe;
				if (!preserveParamMarkers) {
					// get the expr from the expr manager and swap it in
					Object o = sc.getValueManager().getValue(sc, poe.getParameter());
					if (o!=null) {
						if (o.getClass().isArray()) {
							sqlFragments.add(singleQuoteAsBytes);
							sqlFragments.add((byte[])o);
							sqlFragments.add(singleQuoteAsBytes);
						} else {
							sqlFragments.add(String.valueOf(o).getBytes());
						}
					} else { 
						sqlFragments.add(nullAsBytes);
					}
				} else {
					sqlFragments.add(questionMarkAsBytes);
				}
			} else if (oe.getKind() == EntryKind.LATEVAR) {
				LateResolvingVariableOffsetEntry lrvoe = (LateResolvingVariableOffsetEntry) oe;
				Object value = lrvoe.expr.getValue(sc);
				String s = (value == null ? "null" : "'" + value.toString() + "'");
				sqlFragments.add(s.getBytes());
			}
			offset = index + tok.length();
		}
		sqlFragments.add(ArrayUtils.subarray(format, offset, format.length));
		lines.add(new String(concatSQLFragments(sqlFragments)));
	}
	
	public GenericSQLCommand resolve(Map<Integer,String> rawRepls, SchemaContext sc) {
		if (entries.length == 0) return this;
//		boolean swapParams = !sc.getValueManager().hasPassDownParams();
		List<OffsetEntry> downstream = new ArrayList<OffsetEntry>();
		String formatStr = new String(format);
		StringBuilder buf = new StringBuilder(formatStr.length() * 2);
        Emitter emitter = Singletons.require(HostService.class).getDBNative().getEmitter();
		int offset = 0;
		for(OffsetEntry oe : entries) {
			int index = oe.getOffset();
			String tok = oe.getToken();
			buf.append(formatStr.substring(offset,index));
			if (oe.getKind().isLate()) {
				// still need to get the next part of the format
				int newoff = buf.length();
				buf.append(oe.getToken());
				downstream.add(oe.makeAdjusted(newoff));				
			} else if (oe.getKind() == EntryKind.LITERAL) {
				LiteralOffsetEntry loe = (LiteralOffsetEntry) oe;
				ILiteralExpression ile = loe.getLiteral();
				if (ile instanceof IDelegatingLiteralExpression) {
					IDelegatingLiteralExpression idle = (IDelegatingLiteralExpression) ile;
					String repl = rawRepls.get(idle.getPosition());
					buf.append(repl);					
				} else {
					emitter.emitLiteral(sc, loe.getLiteral(), buf);
				}
			} else if (oe.getKind() == EntryKind.TEMPTABLE) {
				TempTableOffsetEntry ttoe = (TempTableOffsetEntry) oe;
				buf.append(ttoe.getTempTable().getName(sc));
			} else if (oe.getKind() == EntryKind.PRETTY) {
				if (buf.length() > 0)
					buf.append(" ");
			} else if (oe.getKind() == EntryKind.PARAMETER) {
//				ParameterOffsetEntry poe = (ParameterOffsetEntry) oe;
//				if (false && swapParams) {
				// get the expr from the expr manager and swap it in
//					buf.append(sc.getValueManager().getValue(sc, poe.getParameter()));
//				} else {
					buf.append("?");
//				}
			}
			offset = index + tok.length();
		}
		buf.append(formatStr.substring(offset));
		OffsetEntry[] does = downstream.toArray(new OffsetEntry[0]);
		return new GenericSQLCommand(buf.toString(),does, type, isUpdate, hasLimit);	
	}
		
	public List<Object> getFinalParams(SchemaContext sc) {
		// does not apply if params are not pushdown
		if (sc.getValueManager().hasPassDownParams()) {
			List<Object> out = new ArrayList<Object>();
			for(OffsetEntry oe : entries) {
				if (oe.getKind() == EntryKind.PARAMETER) {
					ParameterOffsetEntry poe = (ParameterOffsetEntry)oe;
					out.add(sc.getValueManager().getValue(sc, poe.getParameter()));
				}
			}
			return out;
		}
		return null;
	}
	
	public String resolve(DBNameResolver w) {
		return new String(resolveAsBytes(w));
	}
	
	public byte[] resolveAsBytes(DBNameResolver w) {
		if (entries.length == 0) return format;
		// should be no downstream
		List<byte[]> sqlFragments = new ArrayList<byte[]>();
		int offset = 0;
		for(OffsetEntry oe : entries) {
			if (oe.getKind() == EntryKind.LITERAL) {
				continue;
			}

			int index = oe.getOffset();
			String tok = oe.getToken();
			sqlFragments.add(ArrayUtils.subarray(format,offset,index));
			offset = index + tok.length();
			String actualValue;
			if (oe.getKind() == EntryKind.RANDOM_SEED) {
				final String seed = oe.getToken();
				if (w.getWorkerSite() instanceof PersistentSite) {
					/* The IF clause handles the case when seed = 0. */
					actualValue = String.valueOf(w.getSiteIndex()).concat(" * (").concat(seed).concat(" + IF(").concat(seed).concat(", 0, 1))");
				} else {
					actualValue = seed;
				}
			} else {
				actualValue = w.getNameOnSite(tok);
			}
			sqlFragments.add(actualValue.getBytes());
		}
		sqlFragments.add(ArrayUtils.subarray(format, offset, format.length));
		return concatSQLFragments(sqlFragments);

	}
	
	public Boolean isSelect() {
		if (type == null) return null;
		return (type == StatementType.SELECT || type == StatementType.UNION);
	}
	
	public Boolean isForUpdate() {
		return isUpdate;
	}
	
	public Boolean isLimit() {
		return hasLimit;
	}
	
	public StatementType getStatementType() {
		return type;
	}

	private byte[] concatSQLFragments(List<byte[]> sqlFragments) {
		int totalBytes = getTotalBytes(sqlFragments);
		ByteBuf formattedSQL = Unpooled.buffer(totalBytes);
		for(byte[] fragment : sqlFragments) {
			formattedSQL.writeBytes(fragment);
		}
		return formattedSQL.array();
	}
	
	private int getTotalBytes(List<byte[]> sqlFragments) {
		int totalBytes = 0;
		for (byte[] fragment : sqlFragments) {
			totalBytes += fragment.length;
		}
		return totalBytes;
	}
	
	public enum EntryKind {
		LITERAL(false),
		DBNAME(true),
		TEMPTABLE(false),
		PARAMETER(false),
		LATEVAR(false),
		PRETTY(false),
		RANDOM_SEED(true);
		
		private final boolean late;
		private EntryKind(boolean phase) {
			late = phase;
		}
		
		public boolean isLate() {
			return late;
		}
	}
	
	public static abstract class OffsetEntry {

		protected int offset;
		protected String token;
		
		public OffsetEntry(int off, String tok) {
			offset = off;
			token = tok;
		}
		
		public int getOffset() {
			return offset;
		}
		
		public String getToken() {
			return token;
		}
		
		public abstract EntryKind getKind();
		
		public abstract OffsetEntry makeAdjusted(int newoff);
	}
	
	public static class LiteralOffsetEntry extends OffsetEntry {

		protected final ILiteralExpression literal;
		
		public LiteralOffsetEntry(int off, String tok, ILiteralExpression dle) {
			super(off, tok);
			literal = dle;
		}

		@Override
		public EntryKind getKind() {
			return EntryKind.LITERAL;
		}

		@Override
		public OffsetEntry makeAdjusted(int newoff) {
			return new LiteralOffsetEntry(newoff, getToken(),literal);
		}

		public ILiteralExpression getLiteral() {
			return literal;
		}
		
	}

	public static class PrettyOffsetEntry extends OffsetEntry {

		private final short indent;
		
		public PrettyOffsetEntry(int off, short indent) {
			super(off, "");
			this.indent = indent;
		}

		public void addIndent(StringBuilder buf, String multiple) {
			for(int i = 0; i < indent; i++)
				buf.append(multiple);
		}
		
		@Override
		public EntryKind getKind() {
			return EntryKind.PRETTY;
		}

		@Override
		public OffsetEntry makeAdjusted(int newoff) {
			return new PrettyOffsetEntry(newoff, indent);
		}
		
	}
	
	// two different kinds of parameters - those that we can just sub in
	// and those that we have to pass down - but this is entirely controlled by
	// the expr manager
	public static class ParameterOffsetEntry extends OffsetEntry {

		// this is the original position
		private final IParameter parameter;
		
		public ParameterOffsetEntry(int off, String tok, IParameter param) {
			super(off,tok);
			this.parameter = param;
		}
		
		@Override
		public EntryKind getKind() {
			return EntryKind.PARAMETER;
		}

		@Override
		public OffsetEntry makeAdjusted(int newoff) {
			return new ParameterOffsetEntry(newoff, getToken(), parameter);
		}
		
		public IParameter getParameter() {
			return parameter;
		}
		
	}
	
	public static class LateResolveEntry extends OffsetEntry {

		public LateResolveEntry(int off, String tok) {
			super(off, tok);
		}

		@Override
		public EntryKind getKind() {
			return EntryKind.DBNAME;
		}

		@Override
		public OffsetEntry makeAdjusted(int newoff) {
			return new LateResolveEntry(newoff, getToken());
		}
		
	}

	public static class TempTableOffsetEntry extends OffsetEntry {
		
		protected TempTable temp;
		
		public TempTableOffsetEntry(int off, String tok, TempTable tt) {
			super(off, tok);
			temp = tt;
		}
		
		@Override
		public EntryKind getKind() {
			return EntryKind.TEMPTABLE;
		}

		@Override
		public OffsetEntry makeAdjusted(int newoff) {
			return new TempTableOffsetEntry(newoff, getToken(), temp);
		}

		public TempTable getTempTable() {
			return temp;
		}
				
	}
	
	public static class LateResolvingVariableOffsetEntry extends OffsetEntry {
		
		private final IConstantExpression expr;
		
		public LateResolvingVariableOffsetEntry(int off, String tok, IConstantExpression expr) {
			super(off,tok);
			this.expr = expr;
		}

		@Override
		public EntryKind getKind() {
			return EntryKind.LATEVAR;
		}

		@Override
		public OffsetEntry makeAdjusted(int newoff) {
			return new LateResolvingVariableOffsetEntry(newoff, getToken(), expr);
		}
		
	}
	
	public static class RandomSeedOffsetEntry extends LateResolveEntry {

		private final ExpressionNode expr;

		public RandomSeedOffsetEntry(final int off, final String tok, final ExpressionNode expr) {
			super(off, tok);
			this.expr = expr;
		}

		public ExpressionNode getSeedExpression() {
			return this.expr;
		}

		@Override
		public EntryKind getKind() {
			return EntryKind.RANDOM_SEED;
		}

		@Override
		public OffsetEntry makeAdjusted(int newoff) {
			return new RandomSeedOffsetEntry(newoff, getToken(), this.expr);
		}

	}

	// helper class
	public static class Builder {
		
		private List<OffsetEntry> entries;
		private boolean isLimit = false;
		private boolean isForUpdate = false;
		private StatementType type;
		
		public Builder() {
			entries = new ArrayList<OffsetEntry>();
			type = null;
		}
		
		public Builder withLiteral(int offset, String tok, DelegatingLiteralExpression dle) {
			entries.add(new LiteralOffsetEntry(offset, tok, dle.getCacheExpression()));
			return this;
		}
		
		public Builder withParameter(int offset, String tok, IParameter p) {
			entries.add(new ParameterOffsetEntry(offset, tok, (IParameter) p.getCacheExpression()));
			return this;
		}
		
		public Builder withDBName(int offset, String tok) {
			entries.add(new LateResolveEntry(offset, tok));
			return this;
		}
		
		public Builder withTempTable(int offset, String tok, TempTable tt) {
			entries.add(new TempTableOffsetEntry(offset, tok, tt));
			return this;
		}

		public Builder withLateVariable(int offset, String tok, IConstantExpression ice) {
			entries.add(new LateResolvingVariableOffsetEntry(offset,tok,ice));
			return this;
		}

		public Builder withPretty(int offset, int indent) {
			entries.add(new PrettyOffsetEntry(offset,(short)indent));
			return this;
		}
		
		public Builder withLimit() {
			isLimit = true;
			return this;
		}
		
		public Builder withForUpdate() {
			isForUpdate = true;
			return this;
		}
		
		public Builder withType(StatementType st) {
			type = st;
			return this;
		}

		public Builder withRandomSeed(int offset, String tok, ExpressionNode expr) {
			entries.add(new RandomSeedOffsetEntry(offset, tok, expr));
			return this;
		}

		public GenericSQLCommand build(String format) {
			OffsetEntry[] out = entries.toArray(new OffsetEntry[0]);
			return new GenericSQLCommand(format,out, type, isForUpdate, isLimit);
		}
	}
}
