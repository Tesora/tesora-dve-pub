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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.db.mysql.MysqlEmitter;
import com.tesora.dve.exceptions.PECodingException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.node.expression.DelegatingLiteralExpression;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LateBindingConstantExpression;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.cache.IAutoIncrementLiteralExpression;
import com.tesora.dve.sql.schema.cache.IConstantExpression;
import com.tesora.dve.sql.schema.cache.IDelegatingLiteralExpression;
import com.tesora.dve.sql.schema.cache.ILiteralExpression;
import com.tesora.dve.sql.schema.cache.IParameter;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.variables.KnownVariables;
import com.tesora.dve.variables.VariableStoreSource;

// a generic sql command is what would be emitted by the emitter upto any literals
// instead it uses a 'format' string (non literal parts of the final result)
// and a list of offsets for where literals exist.
public class GenericSQLCommand {

    public interface DBNameResolver {
        String getNameOnSite(String dbName);

        int getSiteIndex();

        StorageSite getWorkerSite();
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

		public byte[] getBytes(final Charset encoding) {
			return this.token.getBytes(encoding); // TODO: encode using CharsetEncoder.
		}

		@Override
		public String toString() {
			return this.token;
		}
	}

	private static final class FragmentTable {

		private final List<CommandFragment> fragments;
		private final Map<OffsetEntry, Integer> fragmentIndex;

		public FragmentTable(final int initialCapacity) {
			this.fragments = new ArrayList<CommandFragment>(initialCapacity);
			this.fragmentIndex = new HashMap<OffsetEntry, Integer>(initialCapacity);
		}

		public FragmentTable(final FragmentTable other) {
			this(other.fragments, other.fragmentIndex);
		}

		private FragmentTable(final List<CommandFragment> fragments, final Map<OffsetEntry, Integer> fragmentIndex) {
			this.fragments = new ArrayList<CommandFragment>(fragments);
			this.fragmentIndex = new HashMap<OffsetEntry, Integer>(fragmentIndex);
		}

		/**
		 * May trigger lazy decoding of all fragments.
		 * 
		 * @return The length of a String formed by concatenating decoded values
		 *         of all fragments.
		 */
		public int getDecodedLength() {
			int totalLength = 0;
			for (final CommandFragment fragment : this.fragments) {
				totalLength += fragment.getDecoded().length();
			}

			return totalLength;
		}

		/**
		 * May trigger lazy encoding of all fragments.
		 * 
		 * @return The size of a buffer needed to accommodate encoded values of
		 *         all fragments.
		 */
		public int getEncodedLength() {
			int totalLength = 0;
			for (final CommandFragment fragment : this.fragments) {
				totalLength += fragment.getEncoded().remaining();
			}

			return totalLength;
		}

		public Set<OffsetEntry> viewIndexEntries() {
			return Collections.unmodifiableSet(this.fragmentIndex.keySet());
		}

		public List<CommandFragment> viewFragments() {
			return Collections.unmodifiableList(this.fragments);
		}

		public boolean hasIndexEntries() {
			return !this.fragmentIndex.isEmpty();
		}

		/**
		 * @return Position of an indexable fragment in the list of fragments.
		 */
		public int getFragmentPosition(final OffsetEntry key) {
			return this.fragmentIndex.get(key);
		}

		/**
		 * Add a non-indexable fragment that cannot be resolved/replaced.
		 * It is safe to share these fragments between different command
		 * instances as they never change.
		 */
		public void add(final CommandFragment fragment) {
			this.add(null, fragment);
		}

		/**
		 * Add an indexable fragment. These fragments can be accessed and
		 * replaced during command resolution.
		 */
		public void add(final OffsetEntry key, final CommandFragment fragment) {
			if (fragment != null) {
				if (key != null) {
					if (this.fragmentIndex.containsKey(key)) {
						throw new PECodingException("Fragment for '" + key.toString() + "' is already occupied.");
					}

					this.fragmentIndex.put(key, this.fragments.size());
				}

				this.fragments.add(fragment);
			}
		}

		public void replace(final OffsetEntry key, final CommandFragment newValue) {
			final Integer position = this.fragmentIndex.get(key);
			if (position == null) {
				throw new PECodingException("No fragment to replace for '" + key.toString() + "' found.");
			}

			this.fragments.set(position, newValue);
		}

		public void addAll(final FragmentTable other) {
			final int indexOffset = this.fragments.size();
			this.fragments.addAll(other.fragments);
			for (final Map.Entry<OffsetEntry, Integer> otherEntry : other.fragmentIndex.entrySet()) {
				this.fragmentIndex.put(otherEntry.getKey(), otherEntry.getValue() + indexOffset);
			}
		}

		/**
		 * Produce a display-friendly version of this table.
		 * 
		 * The column headers are:
		 * "fragment position" (index into the fragment list)"
		 * "index key entry" (offset entry that binds to the fragment if any)"
		 * "decoded fragment value"
		 * 
		 * May trigger lazy decoding of all fragments.
		 * 
		 * @return Text formatted version of this table.
		 */
		@Override
		public String toString() {
			final int numFragments = this.fragments.size();
			final StringBuilder table = new StringBuilder();
			for (int i = 0; i < numFragments; ++i) {
				table
						.append(i).append(" ")
						.append(String.valueOf(findIndexKeyForValue(i))).append(" \"")
						.append(this.fragments.get(i)).append("\"")
						.append(PEConstants.LINE_SEPARATOR);
			}

			return table.toString();
		}

		private OffsetEntry findIndexKeyForValue(final int value) {
			for (final Map.Entry<OffsetEntry, Integer> indexEntry : this.fragmentIndex.entrySet()) {
				if (indexEntry.getValue() == value) {
					return indexEntry.getKey();
				}
			}

			return null;
		}

	}

	public static final class CommandFragment {

		private final Charset encoding;
		private String decoded;
		private ByteBuffer encoded;

		protected CommandFragment(final Charset encoding, final Tokens sqlToken) {
			this(encoding, sqlToken.toString());
		}

		protected CommandFragment(final Charset encoding, final Name schemaObjectName) {
			this(encoding, schemaObjectName.get());
		}

		protected CommandFragment(final Charset encoding, final StringBuilder decoded) {
			this(encoding, decoded.toString());
		}

		protected CommandFragment(final Charset encoding, final String decoded) {
			this.encoding = encoding;
			this.decoded = decoded;
		}

		protected CommandFragment(final Charset encoding, final ByteBuffer encoded) {
			this.encoding = encoding;
			this.encoded = (ByteBuffer) encoded.duplicate().rewind(); // TODO: is not read-only @see getDecoded()
		}

		/**
		 * Decode the encoded value if not already.
		 * 
		 * @return Decoded value of this fragment.
		 */
		public String getDecoded() {
			if (this.decoded == null) {
				this.decoded = new String(this.encoded.array(), this.encoding); // TODO: decode using CharsetDecoder.
			}
			return this.decoded;
		}

		/**
		 * Encode the decoded value if not already.
		 * 
		 * @return Encoded value of this fragment.
		 */
		public ByteBuffer getEncoded() {
			if (this.encoded == null) {
				this.encoded = ByteBuffer.wrap(this.decoded.getBytes(this.encoding)).asReadOnlyBuffer(); // TODO: encode using CharsetEncoder.
			}
			return this.encoded.duplicate();
		}

		/**
		 * May trigger lazy decoding of this fragment.
		 * 
		 * @return Decoded value of this fragment.
		 */
		@Override
		public String toString() {
			return this.getDecoded();
		}

	}

	//	private static final class FormatBuffer {
	//
	//		private static final float BUFFER_INITIAL_CAPACITY_FACTOR = 2.0f;
	//
	//		private ByteBuffer buffer;
	//
	//		private static ByteBuffer ensureCapacity(final ByteBuffer container, final int requiredRemainingCapacity) {
	//			final int needeExtraCapacity = requiredRemainingCapacity - container.remaining();
	//			if (needeExtraCapacity > 0) {
	//				return expandCapacity(container, needeExtraCapacity);
	//			}
	//
	//			return container;
	//		}
	//
	//		private static ByteBuffer expandCapacity(final ByteBuffer container, final int extraSpace) {
	//			final ByteBuffer extended = allocateByteBuffer(container.capacity() + extraSpace);
	//			extended.put(container.array(), 0, container.position());
	//			return extended;
	//		}
	//
	//		private static ByteBuffer allocateByteBuffer(final int minInitialCapacity) {
	//			return allocateByteBuffer(minInitialCapacity, BUFFER_INITIAL_CAPACITY_FACTOR);
	//		}
	//
	//		private static ByteBuffer allocateByteBuffer(final int minInitialCapacity, final float extraCapacityFraction) {
	//			return ByteBuffer.allocate((int) (minInitialCapacity * extraCapacityFraction));
	//		}
	//
	//		public static FormatBuffer wrap(final byte[] bytes) {
	//			return wrap(ByteBuffer.wrap(bytes));
	//		}
	//
	//		public static FormatBuffer wrap(final ByteBuffer bytes) {
	//			return new FormatBuffer(bytes);
	//		}
	//
	//		public static FormatBuffer allocate(final int minInitialCapacity) {
	//			return new FormatBuffer(minInitialCapacity);
	//		}
	//
	//		private FormatBuffer(final int minInitialCapacity) {
	//			this.buffer = allocateByteBuffer(minInitialCapacity);
	//		}
	//
	//		private FormatBuffer(final ByteBuffer storage) {
	//			this.buffer = storage;
	//		}
	//
	//		public FormatBuffer append(final byte[] source) {
	//			return append(ByteBuffer.wrap(source));
	//		}
	//
	//		public FormatBuffer append(final FormatBuffer source) {
	//			return append(source.viewBytes());
	//		}
	//
	//		public FormatBuffer append(final ByteBuffer source) {
	//			return append(source, 0, source.limit());
	//		}
	//
	//		public FormatBuffer append(final FormatBuffer source, final int startIndexInclusive, final int endIndexExclusive) {
	//			return append(source.viewBytes(), startIndexInclusive, endIndexExclusive);
	//		}
	//
	//		public void ensureCapacity(final int requiredRemainingCapacity) {
	//			this.buffer = ensureCapacity(this.buffer, requiredRemainingCapacity);
	//		}
	//
	//		public void expandCapacity(final int extraSpace) {
	//			this.buffer = expandCapacity(this.buffer, extraSpace);
	//		}
	//
	//		private FormatBuffer append(final ByteBuffer source, final int startIndexInclusive, final int endIndexExclusive) {
	//			final int numBytesToCopy = endIndexExclusive - startIndexInclusive;
	//			if (numBytesToCopy > 0) {
	//				this.ensureCapacity(numBytesToCopy);
	//				this.buffer.put((ByteBuffer) source.asReadOnlyBuffer().position(startIndexInclusive).limit(endIndexExclusive));
	//			}
	//
	//			return this;
	//		}
	//
	//		public ByteBuffer viewBytes() {
	//			final ByteBuffer readOnlyView = this.buffer.asReadOnlyBuffer();
	//			if (readOnlyView.position() > 0) {
	//				return (ByteBuffer) readOnlyView.flip();
	//			}
	//
	//			return readOnlyView;
	//		}
	//
	//		public int bytesWritten() {
	//			return this.buffer.position();
	//		}
	//	}

	private final Charset encoding;
	private final FragmentTable commandFragments;

	private final StatementType type;
	private Boolean isUpdate = null;
	private Boolean hasLimit = null;

	private static Charset getCurrentSessionConnectionCharSet(final SchemaContext sc) {
		if ((sc.getOptions() != null) && sc.getOptions().isInfoSchemaView()) {
			// won't have the host service set up, so hardcode in a charset.
			return KnownVariables.CHARACTER_SET_CLIENT.getDefaultOnMissing().getJavaCharset();
		}
		return getCurrentSessionConnectionCharSet(sc.getConnection().getVariableSource());
	}

	private static Charset getCurrentSessionConnectionCharSet(final VariableStoreSource vs) {
		return KnownVariables.CHARACTER_SET_CLIENT.getSessionValue(vs).getJavaCharset();
	}

	public GenericSQLCommand(final SchemaContext sc, final String format) {
		this(getCurrentSessionConnectionCharSet(sc), format);
	}

	public GenericSQLCommand(final VariableStoreSource vs, final String format) {
		this(getCurrentSessionConnectionCharSet(vs), format);
	}

	public GenericSQLCommand(final Charset connectionCharset, final String format) {
		this(connectionCharset, format, Collections.EMPTY_LIST, null, null, null);
	}

	public GenericSQLCommand(final Charset encoding, final byte[] format) {
		this(encoding, new FragmentTable(1), null, null, null);
		this.commandFragments.add(new CommandFragment(encoding, ByteBuffer.wrap(format)));

	}

	private GenericSQLCommand(final SchemaContext sc, final String format, final List<OffsetEntry> entries, StatementType stmtType, Boolean isUpdate,
			Boolean hasLimit) {
		this(getCurrentSessionConnectionCharSet(sc), format, entries, stmtType, isUpdate, hasLimit);
	}

	private GenericSQLCommand(final Charset connectionCharset, final String format, final List<OffsetEntry> entries, StatementType stmtType, Boolean isUpdate,
			Boolean hasLimit) {
		this(connectionCharset, chopToFragments(connectionCharset, format, entries), stmtType, isUpdate, hasLimit);
	}

	private GenericSQLCommand(final Charset connectionCharset, final FragmentTable fragments, StatementType stmtType, Boolean isUpdate, Boolean hasLimit) {
		this.encoding = connectionCharset;
		this.commandFragments = fragments;
		this.type = stmtType;
		this.isUpdate = isUpdate;
		this.hasLimit = hasLimit;
	}

    public Charset getEncoding(){
        return encoding;
    }     
        

	private static FragmentTable chopToFragments(final Charset encoding, final String value, final List<OffsetEntry> entries) {
		final FragmentTable fragments = new FragmentTable((2 * entries.size()) + 1);
		int lastEntryIndex = 0;
		for (final OffsetEntry entry : entries) {
			final int nextEntryIndex = entry.getCharacterOffset();
			fragments.add(getFragment(encoding, value, lastEntryIndex, nextEntryIndex));
			final String entryToken = entry.getToken();
			fragments.add(entry, new CommandFragment(encoding, entryToken));
			lastEntryIndex = nextEntryIndex + entryToken.length();
		}
		fragments.add(getFragment(encoding, value, lastEntryIndex, value.length()));

		return fragments;
	}

	private static CommandFragment getFragment(final Charset encoding, final String value, final int startIndexInclusive, final int endIndexExclusive) {
		final String fragmentText = value.substring(startIndexInclusive, endIndexExclusive);
		if (!fragmentText.isEmpty()) {
			return new CommandFragment(encoding, fragmentText);
		}

		return null;
	}

	/**
	 * May trigger lazy decoding of all command fragments.
	 * 
	 * @return Decoded command String.
	 */
	@Override
	public String toString() {
		return this.getDecoded();
	}

	/**
	 * May trigger lazy decoding of all command fragments.
	 * 
	 * @return String formed by concatenating decoded values of all command
	 *         fragments.
	 * @deprecated This method copies all decoded bytes into a new String. Try
	 *             to work with the command fragments directly ( @see public
	 *             List<CommandFragment> viewCommandFragments() ) instead.
	 */
	@Deprecated
	public String getDecoded() {
		final StringBuilder decoded = new StringBuilder(this.commandFragments.getDecodedLength());
		for (final CommandFragment cf : this.commandFragments.viewFragments()) {
			decoded.append(cf.getDecoded());
		}

		return decoded.toString();
	}

	/**
	 * May trigger lazy encoding of all command fragments.
	 * 
	 * @return ByteBuffer formed by concatenating encoded values of all command
	 *         fragments.
	 * @deprecated This method copies all encoded bytes into a new buffer. Try
	 *             to work with the command fragments directly ( @see public
	 *             List<CommandFragment> viewCommandFragments() ) instead.
	 */
	@Deprecated
	public ByteBuffer getEncoded() {
		final ByteBuffer encoded = ByteBuffer.allocate(this.commandFragments.getEncodedLength());
		for (final CommandFragment cf : this.commandFragments.viewFragments()) {
			encoded.put(cf.getEncoded());
		}

		return (ByteBuffer) encoded.rewind();
	}

	/**
	 * @return A read-only view of the fragments forming this command.
	 */
	public List<CommandFragment> viewCommandFragments() {
		return this.commandFragments.viewFragments();
	}

	/**
	 * May trigger lazy decoding of all fragments.
	 * 
	 * @return The length of a String formed by concatenating decoded values
	 *         of all command fragments.
	 */
	public int getDecodedLength() {
		return this.commandFragments.getDecodedLength();
	}

	/**
	 * May trigger lazy encoding of all fragments.
	 * 
	 * @return The size of a buffer needed to accommodate encoded values of
	 *         all command fragments.
	 */
	public int getEncodedLength() {
		return this.commandFragments.getEncodedLength();
	}

	public SQLCommand getSQLCommand() {
		return new SQLCommand(this);
	}

	public GenericSQLCommand append(final GenericSQLCommand other) {
		// TODO: this may be relaxed
		if (this.encoding != other.encoding) {
			throw new PECodingException("Appended commands must use same encodings.");
		}

		this.commandFragments.addAll(other.commandFragments);

		return this;
	}

	public GenericSQLCommand resolve(ConnectionValues cv, String prettyIndent) {
		return resolve(cv, false, prettyIndent);
	}

	/**
	 * Here we resolve variable entries except late-resolving ones.
	 * 
	 * @see public GenericSQLCommand getLateResolvedOnWorker(Worker)
	 */
	public GenericSQLCommand resolve(ConnectionValues cv, boolean preserveParamMarkers, String indent) {
		if (!this.commandFragments.hasIndexEntries()) {
			return this;
		}

		final Emitter emitter = new MysqlEmitter();

		final FragmentTable resolvedFragments = new FragmentTable(this.commandFragments);
		for (final OffsetEntry oe : resolvedFragments.viewIndexEntries()) {
			if (oe.getKind() == EntryKind.LITERAL) {
				final LiteralOffsetEntry loe = (LiteralOffsetEntry) oe;
				final StringBuilder buf = new StringBuilder();
				emitter.emitLiteral(cv, loe.getLiteral(), buf);
				resolvedFragments.replace(oe, new CommandFragment(this.encoding, buf));
			} else if (oe.getKind() == EntryKind.LATE_CONSTANT) {
				final LateBindingConstantOffsetEntry lbcoe = (LateBindingConstantOffsetEntry) oe;
				final StringBuilder buf = new StringBuilder();
				emitter.emitLateBindingConstantExpression(cv, lbcoe.getExpression(), buf);
				resolvedFragments.replace(oe, new CommandFragment(this.encoding, buf));
			} else if (oe.getKind() == EntryKind.PRETTY) {
				if (indent != null) {
					final PrettyOffsetEntry poe = (PrettyOffsetEntry) oe;
					final StringBuilder buf = new StringBuilder();
					if (resolvedFragments.getFragmentPosition(oe) > 0) {
						buf.append(PEConstants.LINE_SEPARATOR);
					}
					poe.addIndent(buf, indent);
					resolvedFragments.replace(oe, new CommandFragment(this.encoding, buf));
				} else {
					if (resolvedFragments.getFragmentPosition(oe) > 0) {
						resolvedFragments.replace(oe, new CommandFragment(this.encoding, Tokens.SPACE));
					}
				}
			} else if (oe.getKind() == EntryKind.TEMPTABLE) {
				final TempTableOffsetEntry ttoe = (TempTableOffsetEntry) oe;
				resolvedFragments.replace(oe, new CommandFragment(this.encoding,
						cv.getTempTableName(ttoe.getTempTable().getValuesIndex())));
			} else if (oe.getKind() == EntryKind.PARAMETER) {
				final ParameterOffsetEntry poe = (ParameterOffsetEntry) oe;
				if (!preserveParamMarkers) {
					// get the expr from the expr manager and swap it in
					final Object o =  cv.getParameterValue(poe.getParameter().getPosition());
					if (o != null) {
						if (o.getClass().isArray()) {
							final byte[] quoteBytes = Tokens.SINGLE_QUOTE.getBytes(this.encoding); // TODO: encode using CharsetEncoder.
							final byte[] parameterValueBytes = (byte[]) o;

							final ByteBuffer encoded = ByteBuffer.allocate((2 * quoteBytes.length) + parameterValueBytes.length);
							encoded.put(quoteBytes);
							encoded.put(parameterValueBytes);
							encoded.put(quoteBytes);
							resolvedFragments.replace(oe, new CommandFragment(this.encoding, encoded));
						} else {
							resolvedFragments.replace(oe, new CommandFragment(this.encoding, String.valueOf(o)));
						}
					} else {
						resolvedFragments.replace(oe, new CommandFragment(this.encoding, Tokens.NULL));
					}
				} else {
					resolvedFragments.replace(oe, new CommandFragment(this.encoding, Tokens.QUESTION_MARK));
				}
			} else if (oe.getKind() == EntryKind.LATEVAR) {
				final LateResolvingVariableOffsetEntry lrvoe = (LateResolvingVariableOffsetEntry) oe;
				final Object value = lrvoe.expr.getValue(cv);
				if (value != null) {
					resolvedFragments.replace(oe, new CommandFragment(this.encoding, PEStringUtils.singleQuote(value.toString())));
				} else {
					resolvedFragments.replace(oe, new CommandFragment(this.encoding, Tokens.NULL));
				}
			} 
		}

		return new GenericSQLCommand(this.encoding, resolvedFragments, this.type, this.isUpdate, this.hasLimit);
	}

	public GenericSQLCommand resolveLateConstants(ConnectionValues cv) {
		if (!this.commandFragments.hasIndexEntries()) {
			return this;
		}

		final Emitter emitter = Singletons.require(HostService.class).getDBNative().getEmitter();
		
		final FragmentTable resolvedFragments = new FragmentTable(this.commandFragments);
		for (final OffsetEntry oe : resolvedFragments.viewIndexEntries()) {
			if (oe.getKind() == EntryKind.LATE_CONSTANT) {
				final LateBindingConstantOffsetEntry lbcoe = (LateBindingConstantOffsetEntry) oe;
				String value = emitter.emitConstantExprValue(lbcoe.getExpression(), lbcoe.getExpression().getValue(cv)); 
				resolvedFragments.replace(oe, new CommandFragment(this.encoding,value));
			} else if (oe.getKind() == EntryKind.LATE_AUTOINC) {
				final LateAutoincOffsetEntry laoe = (LateAutoincOffsetEntry) oe;
				String value = emitter.emitConstantExprValue(laoe.getLiteral(), laoe.getLiteral().getValue(cv));
				resolvedFragments.replace(oe, new CommandFragment(this.encoding,value));
			}
		}

		return new GenericSQLCommand(this.encoding, resolvedFragments, this.type, this.isUpdate, this.hasLimit);		
	}
	
	/**
	 * Resolve the command as String for display/logging purposes.
	 * 
	 * @param lines
	 *            Resolved command's lines.
	 */
	public void resolveAsTextLines(final ConnectionValues cv, final boolean preserveParamMarkers, final String indent, final List<String> lines) {
		final GenericSQLCommand resolved = resolve(cv, preserveParamMarkers, indent);
		final String resolvedAsString = resolved.getDecoded();
		lines.addAll(Arrays.asList(resolvedAsString.split(PEConstants.LINE_SEPARATOR)));
	}

	/**
	 * Replace delegating literals with raw plan entries.
	 * 
	 * @param mapping
	 *            Mapping of offset entries to raw plan literal replacements.
	 */
	public GenericSQLCommand resolveRawEntries(final Map<Integer, String> mapping, final ConnectionValues cv) {
		return resolveRawEntries(mapping).resolve(cv, true, null);
	}

	private GenericSQLCommand resolveRawEntries(final Map<Integer, String> mapping) {
		if (!this.commandFragments.hasIndexEntries()) {
			return this;
		}

		final FragmentTable resolvedFragments = new FragmentTable(this.commandFragments);
		for (final OffsetEntry oe : resolvedFragments.viewIndexEntries()) {
			if (oe.getKind() == EntryKind.LITERAL) {
				final LiteralOffsetEntry loe = (LiteralOffsetEntry) oe;
				final ILiteralExpression ile = loe.getLiteral();
				if (ile instanceof IDelegatingLiteralExpression) {
					final IDelegatingLiteralExpression idle = (IDelegatingLiteralExpression) ile;
					final String repl = mapping.get(idle.getPosition());
					resolvedFragments.replace(oe, new CommandFragment(this.encoding, repl));
				}
			}
		}

		return new GenericSQLCommand(this.encoding, resolvedFragments, this.type, this.isUpdate, this.hasLimit);
	}

	/**
	 * Here we resolve late entries whose values depend on the worker/site they
	 * execute on.
	 */
	public GenericSQLCommand resolveLateEntries(final GenericSQLCommand.DBNameResolver w) {
		if (!this.commandFragments.hasIndexEntries()) {
			return this;
		}

		final FragmentTable resolvedFragments = new FragmentTable(this.commandFragments);
		for (final OffsetEntry oe : resolvedFragments.viewIndexEntries()) {
			if (oe.getKind().isLate()) {
				final String token = oe.getToken();
				if (oe.getKind() == EntryKind.RANDOM_SEED) {
					if (w.getWorkerSite() instanceof PersistentSite) {
						/* The IF clause handles the case when seed = 0. */
						final String seedSql = String.valueOf(w.getSiteIndex()).concat(" * (").concat(token).concat(" + IF(").concat(token).concat(", 0, 1))");
						resolvedFragments.replace(oe, new CommandFragment(this.encoding, seedSql));
					} else {
						resolvedFragments.replace(oe, new CommandFragment(this.encoding, token));
					}
				} else {
					resolvedFragments.replace(oe, new CommandFragment(this.encoding, w.getNameOnSite(token)));
				}
			}
		}

		return new GenericSQLCommand(this.encoding, resolvedFragments, this.type, this.isUpdate, this.hasLimit);
	}

	public List<Object> getFinalParams(SchemaContext sc) {
		// does not apply if params are not pushdown
		if (sc.getValueManager().hasPassDownParams()) {
			final List<Object> out = new ArrayList<Object>();
			for (final OffsetEntry oe : this.commandFragments.viewIndexEntries()) {
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
		if (this.type == null) {
			return null;
		}
		return ((this.type == StatementType.SELECT) || (this.type == StatementType.UNION));
	}

	public Boolean isForUpdate() {
		return this.isUpdate;
	}

	public Boolean isLimit() {
		return this.hasLimit;
	}

	public StatementType getStatementType() {
		return this.type;
	}

	public enum EntryKind {
		LITERAL(false),
		DBNAME(true),
		TEMPTABLE(false),
		PARAMETER(false),
		LATEVAR(false),
		PRETTY(false),
		RANDOM_SEED(true),
		LATE_CONSTANT(false),
		LATE_AUTOINC(false);

		private final boolean late;

		private EntryKind(final boolean isLate) {
			this.late = isLate;
		}

		public boolean isLate() {
			return this.late;
		}
	}

	public static abstract class OffsetEntry {

		protected int offset;
		protected String token;

		public OffsetEntry(int off, String tok) {
			this.offset = off;
			this.token = tok;
		}

		public int getCharacterOffset() {
			return this.offset;
		}

		public String getToken() {
			return this.token;
		}

		public abstract EntryKind getKind();

		@Override
		public String toString() {
			return this.getKind().toString().concat(" (").concat(this.getToken()).concat(")");
		}
	}

	public static class LiteralOffsetEntry extends OffsetEntry {

		protected final ILiteralExpression literal;

		public LiteralOffsetEntry(int off, String tok, ILiteralExpression dle) {
			super(off, tok);
			this.literal = dle;
		}

		@Override
		public EntryKind getKind() {
			return EntryKind.LITERAL;
		}

		public ILiteralExpression getLiteral() {
			return this.literal;
		}

	}

	public static class LateAutoincOffsetEntry extends OffsetEntry {
		
		protected final IAutoIncrementLiteralExpression literal;
		
		public LateAutoincOffsetEntry(int off, String tok, IAutoIncrementLiteralExpression expr) {
			super(off,tok);
			this.literal = expr;
		}
		
		@Override
		public EntryKind getKind() {
			return EntryKind.LATE_AUTOINC;
		}
		
		public IAutoIncrementLiteralExpression getLiteral() {
			return this.literal;
		}
		
	}
	
	public static class PrettyOffsetEntry extends OffsetEntry {

		private final short indent;

		public PrettyOffsetEntry(int off, short indent) {
			super(off, "");
			this.indent = indent;
		}

		public void addIndent(StringBuilder buf, String multiple) {
			for (int i = 0; i < this.indent; i++) {
				buf.append(multiple);
			}
		}

		@Override
		public EntryKind getKind() {
			return EntryKind.PRETTY;
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

		public IParameter getParameter() {
			return this.parameter;
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

	}

	public static class TempTableOffsetEntry extends OffsetEntry {

		protected TempTable temp;

		public TempTableOffsetEntry(int off, String tok, TempTable tt) {
			super(off, tok);
			this.temp = tt;
		}

		@Override
		public EntryKind getKind() {
			return EntryKind.TEMPTABLE;
		}

		public TempTable getTempTable() {
			return this.temp;
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

	}

	public static class LateBindingConstantOffsetEntry extends LateResolveEntry {
		
		private final LateBindingConstantExpression expr;
		
		public LateBindingConstantOffsetEntry(int offset, String token, LateBindingConstantExpression expr) {
			super(offset,token);
			this.expr = expr;
		}
		
		public LateBindingConstantExpression getExpression() {
			return this.expr;
		}
		
		@Override
		public EntryKind getKind() {
			return EntryKind.LATE_CONSTANT;
		}

	}
	
	// helper class
	public static class Builder {

		private final List<OffsetEntry> entries;
		private boolean isLimit = false;
		private boolean isForUpdate = false;
		private StatementType type;

		public Builder() {
			this.entries = new ArrayList<OffsetEntry>();
			this.type = null;
		}

		public Builder withLiteral(int offset, String tok, DelegatingLiteralExpression dle) {
			this.entries.add(new LiteralOffsetEntry(offset, tok, dle.getCacheExpression()));
			return this;
		}
		
		public Builder withLateAutoinc(int offset, String tok, IAutoIncrementLiteralExpression ile) {
			this.entries.add(new LateAutoincOffsetEntry(offset, tok, (IAutoIncrementLiteralExpression) ile.getCacheExpression()));
			return this;
		}

		public Builder withParameter(int offset, String tok, IParameter p) {
			this.entries.add(new ParameterOffsetEntry(offset, tok, (IParameter) p.getCacheExpression()));
			return this;
		}

		public Builder withDBName(int offset, String tok) {
			this.entries.add(new LateResolveEntry(offset, tok));
			return this;
		}

		public Builder withTempTable(int offset, String tok, TempTable tt) {
			this.entries.add(new TempTableOffsetEntry(offset, tok, tt));
			return this;
		}

		public Builder withLateVariable(int offset, String tok, IConstantExpression ice) {
			this.entries.add(new LateResolvingVariableOffsetEntry(offset, tok, ice));
			return this;
		}

		public Builder withPretty(int offset, int indent) {
			this.entries.add(new PrettyOffsetEntry(offset, (short) indent));
			return this;
		}

		public Builder withLimit() {
			this.isLimit = true;
			return this;
		}

		public Builder withForUpdate() {
			this.isForUpdate = true;
			return this;
		}

		public Builder withType(StatementType st) {
			this.type = st;
			return this;
		}

		public Builder withRandomSeed(int offset, String tok, ExpressionNode expr) {
			this.entries.add(new RandomSeedOffsetEntry(offset, tok, expr));
			return this;
		}

		public Builder withLateConstant(int offset, String tok, LateBindingConstantExpression expr) {
			entries.add(new LateBindingConstantOffsetEntry(offset,tok,expr));
			return this;
		}

		public GenericSQLCommand build(final SchemaContext sc, String format) {
			return new GenericSQLCommand(sc, format, this.entries, this.type, this.isForUpdate, this.isLimit);
		}
	}
}
