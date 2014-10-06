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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;

import com.tesora.dve.charset.NativeCharSet;
import com.tesora.dve.charset.NativeCharSetCatalog;
import com.tesora.dve.charset.mysql.MysqlNativeCharSet;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.db.mysql.MysqlEmitter;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.node.expression.DelegatingLiteralExpression;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.schema.cache.IDelegatingLiteralExpression;
import com.tesora.dve.sql.schema.cache.ILiteralExpression;
import com.tesora.dve.sql.schema.cache.IParameter;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.variables.KnownVariables;
import com.tesora.dve.variables.VariableStoreSource;
import com.tesora.dve.worker.Worker;

// a generic sql command is what would be emitted by the emitter upto any literals
// instead it uses a 'format' string (non literal parts of the final result)
// and a list of offsets for where literals exist.
public class GenericSQLCommand {

	public interface DBNameResolver {
		String getNameOnSite(String dbName);

		int getSiteIndex();

		StorageSite getWorkerSite();
	}

	private int numCharacters;
	private byte[] format;
	private final Charset encoding;
	private final SortedMap<OffsetEntry, Integer> entryMap;

	private final StatementType type;

	private Boolean isUpdate = null;
	private Boolean hasLimit = null;

	private static Charset getCurrentSessionConnectionCharSet(final SchemaContext sc) {
		if (sc.getOptions() != null && sc.getOptions().isInfoSchemaView()) {
			// won't have the host service set up, so hardcode in a charset.
			return MysqlNativeCharSet.UTF8.getJavaCharset();
		}
		return getCurrentSessionConnectionCharSet(sc.getConnection().getVariableSource());
	}

	private static Charset getCurrentSessionConnectionCharSet(final VariableStoreSource vs) {
		final NativeCharSetCatalog charSetcatalog = Singletons.require(HostService.class).getDBNative().getSupportedCharSets();
		final String connectionCharSetName = KnownVariables.CHARACTER_SET_CONNECTION.getSessionValue(vs);
		NativeCharSet connectionCharSet;
		try {
			connectionCharSet = charSetcatalog.findCharSetByName(connectionCharSetName, true);
			return connectionCharSet.getJavaCharset();
		} catch (final PEException e) {
			// This should never happen as we validate the variable values when set.
			throw new PECodingException("Session variable '" + KnownVariables.CHARACTER_SET_CONNECTION.getName() + "' is set to an unsupported value.", e);
		}
	}

	private static byte[] resolveToByteArrayWithOffsetMap(final Charset encoding, final String value, final OffsetEntry[] entries,
			final Map<OffsetEntry, Integer> offsetMap) throws PEException {
		final CharsetEncoder encoder = encoding.newEncoder();
		final int inputLength = value.length();
		final int maxBytesPerChar = (int) Math.ceil(encoder.maxBytesPerChar());
		final CharBuffer input = CharBuffer.wrap(value).asReadOnlyBuffer();
		final ByteBuffer resolved = ByteBuffer.allocate(maxBytesPerChar * inputLength);

		try {
			if (entries.length > 0) {
				for (final OffsetEntry entry : entries) {
					final int stringOffset = entry.getCharacterOffset();
					encodeInputSlice(encoder, input, stringOffset, false, resolved);
					offsetMap.put(entry, resolved.position());
				}
			}

			encodeInputSlice(encoder, input, inputLength, true, resolved);

			return slice((ByteBuffer) resolved.flip());
		} catch (final CharacterCodingException e) {
			throw new PEException("Error encoding command: " + value, e);
		}
	}

	/**
	 * Copy bytes from a given buffer to a new byte array.
	 * The copy starts at the current position()
	 * and ends at the limit().
	 */
	private static byte[] slice(final ByteBuffer buffer) {
		final byte[] format = new byte[buffer.limit()];
		buffer.get(format);
		return format;
	}

	private static void encodeInputSlice(final CharsetEncoder encoder, final CharBuffer in, final int limit, final boolean isEnd, final ByteBuffer out)
			throws CharacterCodingException {
		in.limit(limit);
		final CoderResult status = encoder.encode(in, out, isEnd);
		if (status.isError()) {
			status.throwException();
		}
	}

	public GenericSQLCommand(final SchemaContext sc, final String format) {
		this(getCurrentSessionConnectionCharSet(sc), format);
	}

	public GenericSQLCommand(final VariableStoreSource vs, final String format) {
		this(getCurrentSessionConnectionCharSet(vs), format);
	}

	public GenericSQLCommand(final Charset connectionCharset, final String format) {
		this(connectionCharset, format.length(), format.getBytes(connectionCharset));
	}

	/**
	 * This involves slow decoding of the format bytes. Intended for tests only!
	 */
	@Deprecated
	public GenericSQLCommand(final Charset encoding, final byte[] format) {
		this(encoding, new String(format, encoding).length(), format, MapUtils.EMPTY_SORTED_MAP, null, null, null);
	}

	public GenericSQLCommand(final Charset encoding, final int formatStringLength, final byte[] format) {
		this(encoding, formatStringLength, format, MapUtils.EMPTY_SORTED_MAP, null, null, null);
	}

	/**
	 * @param entryMap
	 *            Offsets into the given byte array.
	 */
	private GenericSQLCommand(final Charset encoding, final int formatStringLength, final byte[] format, final SortedMap<OffsetEntry, Integer> entryMap,
			final StatementType stmtType, final Boolean isUpdate,
			final Boolean hasLimit) {
		this.numCharacters = formatStringLength;
		this.entryMap = entryMap;
		this.format = format;
		this.encoding = encoding;
		this.type = stmtType;
		this.isUpdate = isUpdate;
		this.hasLimit = hasLimit;
	}

	private GenericSQLCommand(final SchemaContext sc, final String format, final OffsetEntry[] entries, StatementType stmtType, Boolean isUpdate,
			Boolean hasLimit) throws PEException {
		this(getCurrentSessionConnectionCharSet(sc), format, entries, stmtType, isUpdate, hasLimit);
	}

	/**
	 * This constructor takes entries. We need to map entries to positions in
	 * the resolved string.
	 */
	private GenericSQLCommand(final Charset connectionCharset, final String format, final OffsetEntry[] entries, StatementType stmtType, Boolean isUpdate,
			Boolean hasLimit) throws PEException {
		this.numCharacters = format.length();
		this.entryMap = new TreeMap<OffsetEntry, Integer>();
		this.format = resolveToByteArrayWithOffsetMap(connectionCharset, format, entries, this.entryMap);
		this.encoding = connectionCharset;
		this.type = stmtType;
		this.isUpdate = isUpdate;
		this.hasLimit = hasLimit;
	}

	public String getUnresolved() {
		return new String(getUnresolvedAsBytes(), this.encoding);
	}

	public byte[] getUnresolvedAsBytes() {
		return format;
	}

	public boolean hasLateResolution() {
		return !entryMap.isEmpty();
	}

	public SQLCommand getSQLCommand() {
		return new SQLCommand(this);
	}

	public GenericSQLCommand append(final GenericSQLCommand other) {
		this.append(other.entryMap).append(other.format);
		this.numCharacters += other.numCharacters;
		return this;
	}

	private GenericSQLCommand append(final byte[] formatToAppend) {
		this.format = ArrayUtils.addAll(this.format, formatToAppend);
		return this;
	}

	/**
	 * Append other command's entries and update their offsets.
	 */
	private GenericSQLCommand append(final Map<OffsetEntry, Integer> entriesToAppend) {
		final int initialStringOffset = this.numCharacters;
		final int initialArrayOffset = this.format.length;
		for (final OffsetEntry entry : entriesToAppend.keySet()) {
			final int originalStringOffset = entry.getCharacterOffset();
			final int originalArrayOffset = entriesToAppend.get(entry);
			final OffsetEntry adjustedEntry = entry.makeAdjusted(initialStringOffset + originalStringOffset);
			this.entryMap.put(adjustedEntry, initialArrayOffset + originalArrayOffset);
		}

		return this;
	}

	public GenericSQLCommand resolve(SchemaContext sc, String prettyIndent) {
		return resolve(sc, false, prettyIndent);
	}

	private static enum Tokens {

		SPACE(" "),
		SINGLE_QUOTE("'"),
		QUESTION_MARK("?"),
		NULL("null");

		private final String token;

		private Tokens(final String token) {
			this.token = token;
		}

		public final byte[] getBytes(final Charset encoding) {
			return this.token.getBytes(encoding);
		}

		public int length() {
			return this.token.length();
		}

		@Override
		public String toString() {
			return this.token;
		}
	}

	/**
	 * This involves slow byte decoding.
	 */
	private int emitTokenBytes(final byte[] token, final List<byte[]> buffer) {
		buffer.add(token);
		final String decoded = new String(token, this.encoding);
		return decoded.length();
	}

	private int emitTokenBytes(final Tokens token, final List<byte[]> buffer) {
		final byte[] tokenBytes = token.getBytes(this.encoding);
		buffer.add(tokenBytes);
		return token.length();
	}

	private int emitTokenBytes(final Name token, final List<byte[]> buffer) {
		return emitTokenBytes(token.get(), buffer);
	}

	private int emitTokenBytes(final StringBuilder token, final List<byte[]> buffer) {
		return emitTokenBytes(token.toString(), buffer);
	}

	private int emitTokenBytes(final String token, final List<byte[]> buffer) {
		final byte[] tokenBytes = token.getBytes(this.encoding);
		buffer.add(tokenBytes);
		return token.length();
	}

	// TODO we really need a function specific class to handle resolve/display

	public GenericSQLCommand resolve(SchemaContext sc, boolean preserveParamMarkers, String indent) {
		if (!this.hasLateResolution()) {
			return this;
		}

		final SortedMap<OffsetEntry, Integer> downstreamOffsetMapping = new TreeMap<OffsetEntry, Integer>();
		final List<byte[]> sqlFragments = new ArrayList<byte[]>();
		final Emitter emitter = 
				((sc.getOptions() != null && sc.getOptions().isInfoSchemaView()) ? new MysqlEmitter() :        			
					Singletons.require(HostService.class).getDBNative().getEmitter());
		int currentStringOffset = 0;
		int currentByteArrayOffset = 0;
		for (final OffsetEntry oe : entryMap.keySet()) {
			final int tokenBytesPosition = this.entryMap.get(oe);
			final String token = oe.getToken();
			final byte[] tokenBytes = token.getBytes(this.encoding);
			currentStringOffset += oe.getCharacterOffset() - currentStringOffset;
			sqlFragments.add(ArrayUtils.subarray(format, currentByteArrayOffset, tokenBytesPosition));
			if (oe.getKind().isLate()) {
				// still need to get the next part of the format
				final int fragmentsLength = getTotalBytes(sqlFragments);
				downstreamOffsetMapping.put(oe, fragmentsLength);
				sqlFragments.add(tokenBytes);
				currentStringOffset += token.length();
			} else if (oe.getKind() == EntryKind.LITERAL) {
				final LiteralOffsetEntry loe = (LiteralOffsetEntry) oe;
				final StringBuilder buf = new StringBuilder();
				emitter.emitLiteral(sc, loe.getLiteral(), buf);
				currentStringOffset += emitTokenBytes(buf, sqlFragments);
			} else if (oe.getKind() == EntryKind.PRETTY) {
				final int totalBytes = getTotalBytes(sqlFragments);
				if (indent != null) {
					final PrettyOffsetEntry poe = (PrettyOffsetEntry) oe;
					final StringBuilder buf = new StringBuilder();
					if (totalBytes > 0) {
						buf.append(PEConstants.LINE_SEPARATOR);
					}
					poe.addIndent(buf, indent);
					currentStringOffset += emitTokenBytes(buf, sqlFragments);
				} else {
					if (totalBytes > 0) {
						currentStringOffset += emitTokenBytes(Tokens.SPACE, sqlFragments);
					}
				}
			} else if (oe.getKind() == EntryKind.TEMPTABLE) {
				final TempTableOffsetEntry ttoe = (TempTableOffsetEntry) oe;
				currentStringOffset += emitTokenBytes(ttoe.getTempTable().getName(sc), sqlFragments);
			} else if (oe.getKind() == EntryKind.PARAMETER) {
				final ParameterOffsetEntry poe = (ParameterOffsetEntry) oe;
				if (!preserveParamMarkers) {
					// get the expr from the expr manager and swap it in
					final Object o = sc.getValueManager().getValue(sc, poe.getParameter());
					if (o != null) {
						if (o.getClass().isArray()) {
							currentStringOffset += emitTokenBytes(Tokens.SINGLE_QUOTE, sqlFragments);
							currentStringOffset += emitTokenBytes((byte[]) o, sqlFragments);
							currentStringOffset += emitTokenBytes(Tokens.SINGLE_QUOTE, sqlFragments);
						} else {
							currentStringOffset += emitTokenBytes(String.valueOf(o), sqlFragments);
						}
					} else {
						currentStringOffset += emitTokenBytes(Tokens.NULL, sqlFragments);
					}
				} else {
					currentStringOffset += emitTokenBytes(Tokens.QUESTION_MARK, sqlFragments);
				}
			} else if (oe.getKind() == EntryKind.LATEVAR) {
				final LateResolvingVariableOffsetEntry lrvoe = (LateResolvingVariableOffsetEntry) oe;
				final Object value = lrvoe.expr.getValue(sc);
				if (value != null) {
					currentStringOffset += emitTokenBytes(PEStringUtils.singleQuote(value.toString()), sqlFragments);
				} else {
					currentStringOffset += emitTokenBytes(Tokens.NULL, sqlFragments);
				}
			}
			currentByteArrayOffset = tokenBytesPosition + tokenBytes.length;
		}
		sqlFragments.add(ArrayUtils.subarray(format, currentByteArrayOffset, format.length));

		final byte[] formatBytes = concatSQLFragments(sqlFragments);
		return new GenericSQLCommand(this.encoding, currentStringOffset, formatBytes, downstreamOffsetMapping, this.type, this.isUpdate,
				this.hasLimit);
	}

	public void display(final SchemaContext sc, final boolean preserveParamMarkers, final String indent, final List<String> lines) {
		final GenericSQLCommand resolved = resolve(sc, preserveParamMarkers, indent);
		final String resolvedAsString = resolved.getUnresolved();
		lines.addAll(Arrays.asList(resolvedAsString.split(PEConstants.LINE_SEPARATOR)));
	}

	//	public void display(SchemaContext sc, boolean preserveParamMarkers, String indent, List<String> lines) {
	//		if (!this.hasLateResolution()) {
	//			lines.add(new String(format));
	//			return;
	//		}
	//
	//		final List<byte[]> sqlFragments = new ArrayList<byte[]>();
	//		final Emitter emitter = Singletons.require(HostService.class).getDBNative().getEmitter();
	//		int currentByteArrayOffset = 0;
	//		for (final OffsetEntry oe : entryMap.keySet()) {
	//			final int tokenBytesPosition = this.entryMap.get(oe);
	//			final String token = oe.getToken();
	//			final byte[] tokenBytes = token.getBytes(this.encoding);
	//			final byte[] originalPart = ArrayUtils.subarray(format, currentByteArrayOffset, tokenBytesPosition);
	//			if (originalPart.length > 0) {
	//				sqlFragments.add(originalPart);
	//			}
	//			if (oe.getKind().isLate()) {
	//				// still need to get the next part of the format
	//				sqlFragments.add(tokenBytes);
	//			} else if (oe.getKind() == EntryKind.LITERAL) {
	//				final LiteralOffsetEntry loe = (LiteralOffsetEntry) oe;
	//				final StringBuilder buf = new StringBuilder();
	//				emitter.emitLiteral(sc, loe.getLiteral(), buf);
	//				sqlFragments.add(buf.toString().getBytes(this.encoding));
	//			} else if (oe.getKind() == EntryKind.PRETTY) {
	//				// tie off the old one
	//				if (getTotalBytes(sqlFragments) > 0) {
	//					lines.add(new String(concatSQLFragments(sqlFragments)));
	//				}
	//				sqlFragments.clear();
	//				final PrettyOffsetEntry poe = (PrettyOffsetEntry) oe;
	//				final StringBuilder buf = new StringBuilder();
	//				poe.addIndent(buf, indent);
	//				sqlFragments.add(buf.toString().getBytes(this.encoding));
	//			} else if (oe.getKind() == EntryKind.TEMPTABLE) {
	//				final TempTableOffsetEntry ttoe = (TempTableOffsetEntry) oe;
	//				sqlFragments.add(ttoe.getTempTable().getName(sc).get().getBytes(this.encoding));
	//			} else if (oe.getKind() == EntryKind.PARAMETER) {
	//				final ParameterOffsetEntry poe = (ParameterOffsetEntry) oe;
	//				if (!preserveParamMarkers) {
	//					// get the expr from the expr manager and swap it in
	//					final Object o = sc.getValueManager().getValue(sc, poe.getParameter());
	//					if (o != null) {
	//						if (o.getClass().isArray()) {
	//							sqlFragments.add(Tokens.SINGLE_QUOTE.getBytes(this.encoding));
	//							sqlFragments.add((byte[]) o);
	//							sqlFragments.add(Tokens.SINGLE_QUOTE.getBytes(this.encoding));
	//						} else {
	//							sqlFragments.add(String.valueOf(o).getBytes(this.encoding));
	//						}
	//					} else {
	//						sqlFragments.add(Tokens.NULL.getBytes(this.encoding));
	//					}
	//				} else {
	//					sqlFragments.add(Tokens.QUESTION_MARK.getBytes(this.encoding));
	//				}
	//			} else if (oe.getKind() == EntryKind.LATEVAR) {
	//				final LateResolvingVariableOffsetEntry lrvoe = (LateResolvingVariableOffsetEntry) oe;
	//				final Object value = lrvoe.expr.getValue(sc);
	//				if (value != null) {
	//					sqlFragments.add(PEStringUtils.singleQuote(value.toString()).getBytes(this.encoding));
	//				} else {
	//					sqlFragments.add(Tokens.NULL.getBytes(encoding));
	//				}
	//			}
	//			currentByteArrayOffset = tokenBytesPosition + tokenBytes.length;
	//		}
	//		sqlFragments.add(ArrayUtils.subarray(format, currentByteArrayOffset, format.length));
	//		lines.add(new String(concatSQLFragments(sqlFragments)));
	//	}

	public GenericSQLCommand resolve(Map<Integer, String> rawRepls, SchemaContext sc) throws PEException {
		if (!this.hasLateResolution()) {
			return this;
		}

		//		boolean swapParams = !sc.getValueManager().hasPassDownParams();
		final List<OffsetEntry> downstream = new ArrayList<OffsetEntry>();
		final String formatStr = new String(format);
		final StringBuilder buf = new StringBuilder(formatStr.length() * 2);
		final Emitter emitter = Singletons.require(HostService.class).getDBNative().getEmitter();
		int currentByteArrayOffset = 0;
		for (final OffsetEntry oe : entryMap.keySet()) {
			final int tokenBytesPosition = this.entryMap.get(oe);
			final String token = oe.getToken();
			final byte[] tokenBytes = token.getBytes(this.encoding);
			buf.append(formatStr.substring(currentByteArrayOffset, tokenBytesPosition));
			if (oe.getKind().isLate()) {
				// still need to get the next part of the format
				final int newoff = buf.length();
				buf.append(oe.getToken());
				downstream.add(oe.makeAdjusted(newoff));
			} else if (oe.getKind() == EntryKind.LITERAL) {
				final LiteralOffsetEntry loe = (LiteralOffsetEntry) oe;
				final ILiteralExpression ile = loe.getLiteral();
				if (ile instanceof IDelegatingLiteralExpression) {
					final IDelegatingLiteralExpression idle = (IDelegatingLiteralExpression) ile;
					final String repl = rawRepls.get(idle.getPosition());
					buf.append(repl);
				} else {
					emitter.emitLiteral(sc, loe.getLiteral(), buf);
				}
			} else if (oe.getKind() == EntryKind.TEMPTABLE) {
				final TempTableOffsetEntry ttoe = (TempTableOffsetEntry) oe;
				buf.append(ttoe.getTempTable().getName(sc));
			} else if (oe.getKind() == EntryKind.PRETTY) {
				if (buf.length() > 0) {
					buf.append(Tokens.SPACE);
				}
			} else if (oe.getKind() == EntryKind.PARAMETER) {
				//				ParameterOffsetEntry poe = (ParameterOffsetEntry) oe;
				//				if (false && swapParams) {
				// get the expr from the expr manager and swap it in
				//					buf.append(sc.getValueManager().getValue(sc, poe.getParameter()));
				//				} else {
				buf.append(Tokens.QUESTION_MARK);
				//				}
			}
			currentByteArrayOffset = tokenBytesPosition + tokenBytes.length;
		}
		buf.append(formatStr.substring(currentByteArrayOffset));
		final OffsetEntry[] does = downstream.toArray(new OffsetEntry[0]);
		return new GenericSQLCommand(this.encoding, buf.toString(), does, this.type, this.isUpdate, this.hasLimit);
	}

	public String resolve(final Worker w) {
		return new String(resolveAsBytes(w));
	}

	public GenericSQLCommand getResolvedCopy(final Worker w) {
		return new GenericSQLCommand(this.encoding, this.numCharacters, this.resolveAsBytes(w));
	}

	private byte[] resolveAsBytes(final Worker w) {
		if (!this.hasLateResolution()) {
			return format;
		}

		// should be no downstream
		final List<byte[]> sqlFragments = new ArrayList<byte[]>();
		int currentByteArrayOffset = 0;
		for (final OffsetEntry oe : entryMap.keySet()) {
			if (oe.getKind() == EntryKind.LITERAL) {
				continue;
			}

			final int tokenBytesPosition = this.entryMap.get(oe);
			final String token = oe.getToken();
			final byte[] tokenBytes = token.getBytes(this.encoding);
			sqlFragments.add(ArrayUtils.subarray(format, currentByteArrayOffset, tokenBytesPosition));
			currentByteArrayOffset = tokenBytesPosition + tokenBytes.length;
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
				actualValue = w.getNameOnSite(token);
			}

			sqlFragments.add(actualValue.getBytes(this.encoding));
		}
		sqlFragments.add(ArrayUtils.subarray(format, currentByteArrayOffset, format.length));
		return concatSQLFragments(sqlFragments);

	}

	public List<Object> getFinalParams(SchemaContext sc) {
		// does not apply if params are not pushdown
		if (sc.getValueManager().hasPassDownParams()) {
			final List<Object> out = new ArrayList<Object>();
			for (final OffsetEntry oe : entryMap.keySet()) {
				if (oe.getKind() == EntryKind.PARAMETER) {
					final ParameterOffsetEntry poe = (ParameterOffsetEntry) oe;
					out.add(sc.getValueManager().getValue(sc, poe.getParameter()));
				}
			}
			return out;
		}
		return null;
	}

	public Boolean isSelect() {
		if (type == null) {
			return null;
		}
		return ((type == StatementType.SELECT) || (type == StatementType.UNION));
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
		final int totalBytes = getTotalBytes(sqlFragments);
		final ByteBuf formattedSQL = Unpooled.buffer(totalBytes);
		for (final byte[] fragment : sqlFragments) {
			formattedSQL.writeBytes(fragment);
		}
		return formattedSQL.array();
	}

	private int getTotalBytes(List<byte[]> sqlFragments) {
		int totalBytes = 0;
		for (final byte[] fragment : sqlFragments) {
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

	/**
	 * Entries are compared and sorted by their offsets.
	 */
	public static abstract class OffsetEntry implements Comparable<OffsetEntry> {

		protected int offset;
		protected String token;

		public OffsetEntry(int off, String tok) {
			offset = off;
			token = tok;
		}

		public int getCharacterOffset() {
			return offset;
		}

		public String getToken() {
			return token;
		}

		@Override
		public int hashCode() {
			return Integer.valueOf(this.offset).hashCode();
		}

		@Override
		public boolean equals(final Object other) {
			if (this == other) {
				return true;
			}

			if (other instanceof OffsetEntry) {
				final OffsetEntry otherEntry = (OffsetEntry) other;
				return this.offset == otherEntry.offset;
			}

			return false;
		}

		@Override
		public int compareTo(final OffsetEntry other) {
			return this.offset - other.offset;
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
			return new LiteralOffsetEntry(newoff, getToken(), literal);
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
			for (int i = 0; i < indent; i++) {
				buf.append(multiple);
			}
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
			super(off, tok);
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
			super(off, tok);
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

		private final List<OffsetEntry> entries;
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
			entries.add(new LateResolvingVariableOffsetEntry(offset, tok, ice));
			return this;
		}

		public Builder withPretty(int offset, int indent) {
			entries.add(new PrettyOffsetEntry(offset, (short) indent));
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

		public GenericSQLCommand build(final SchemaContext sc, String format) throws PEException {
			final OffsetEntry[] out = entries.toArray(new OffsetEntry[0]);
			return new GenericSQLCommand(sc, format, out, this.type, this.isForUpdate, this.isLimit);
		}
	}
}
