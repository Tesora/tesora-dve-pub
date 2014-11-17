package com.tesora.dve.sql.schema.modifiers;

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

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.tesora.dve.charset.NativeCharSet;
import com.tesora.dve.charset.NativeCharSetCatalog;
import com.tesora.dve.charset.NativeCollation;
import com.tesora.dve.charset.NativeCollationCatalog;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.errmap.AvailableErrors;
import com.tesora.dve.errmap.ErrorInfo;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.util.Pair;

public final class CharsetCollationModifierBuilder {

	public static Pair<String, String> buildCharsetCollationNamePair(final Name charset, final Name collation,
			final NativeCharSetCatalog availableCharsets, final NativeCollationCatalog availableCollations) {
		final Pair<String, String> values = convert(charset, collation);
		return buildCharsetCollationNamePair(
				values.getFirst(), values.getSecond(),
				availableCharsets, availableCollations);
	}

	/**
	 * Find a valid charset-collation combination.
	 * 
	 * CASES:
	 * a) Both values left unspecified => use server-wide defaults.
	 * b) Charset, but no collation => use default collation for the charset.
	 * c) Collation, but no charset => find charset the collation belongs to.
	 * d) Both values specified => just validate they are compatible.
	 */
	private static Pair<String, String> buildCharsetCollationNamePair(final String charsetName, final String collationName,
			final NativeCharSetCatalog availableCharsets, final NativeCollationCatalog availableCollations) {
	    final DBNative db = Singletons.require(HostService.class).getDBNative();
		if ((charsetName == null) && (collationName == null)) { // Use server defaults.
			final String defaultCharSet = db.getDefaultServerCharacterSet();
			final String defaultCollation = db.getDefaultServerCollation();
			return new Pair<String, String>(defaultCharSet, defaultCollation);
		}
		
		if ((charsetName != null) && (collationName == null)) { // Use default for the charset.
			final NativeCollation nc = availableCollations.findDefaultCollationForCharSet(charsetName);
			if (nc == null) {
				throw new SchemaException(new ErrorInfo(AvailableErrors.UNKNOWN_CHARACTER_SET, charsetName));
			}
			return new Pair<String, String>(charsetName, nc.getName());
		} else if ((charsetName == null) && (collationName != null)) { // Use an appropriate charset.
			final NativeCharSet charset = availableCharsets.findCharSetByCollation(collationName);
			if (charset == null) {
				throw new SchemaException(new ErrorInfo(AvailableErrors.UNKNOWN_COLLATION, collationName));
			}
			return new Pair<String, String>(charset.getName(), collationName);
		} else { // Just check the values for mutual compatibility.
			final NativeCharSet charset = availableCharsets.findCharSetByName(charsetName);
			if (charset == null) {
				throw new SchemaException(new ErrorInfo(AvailableErrors.UNKNOWN_CHARACTER_SET, charsetName));
			} else if (charset.isCompatibleWith(collationName)) {
				return new Pair<String, String>(charsetName, collationName);
			}
			throw new SchemaException(new ErrorInfo(AvailableErrors.COLLATION_CHARSET_MISMATCH, collationName, charsetName));
		}
	}

	private static Set<TableModifier> asSet(final CharsetTableModifier charset, final CollationTableModifier collation) {
		return ImmutableSet.<TableModifier> of(charset, collation);
	}

	private static Pair<String, String> convert(final Name charset, final Name collation) {
		final String charSetValue = (charset != null) ? charset.get() : null;
		final String collationValue = (collation != null) ? collation.get() : null;
		return new Pair<String, String>(charSetValue, collationValue);
	}

	private Name charset;
	private Name collation;

	public void setCharset(final Name charset) {
		this.charset = charset;
	}

	public void setCollation(final Name collation) {
		this.collation = collation;
	}

	public Set<TableModifier> buildModifiers(final NativeCharSetCatalog availableCharsets, final NativeCollationCatalog availableCollations) {
		final Pair<String, String> charsetCollationPair = buildCharsetCollationNamePair(this.charset, this.collation, availableCharsets,
				availableCollations);
		return asSet(new CharsetTableModifier(new UnqualifiedName(charsetCollationPair.getFirst())),
				new CollationTableModifier(new UnqualifiedName(charsetCollationPair.getSecond())));
	}

	public boolean hasValues() {
		return ((this.charset != null) || (this.collation != null));
	}
}
