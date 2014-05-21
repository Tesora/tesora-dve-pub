// OS_STATUS: public
package com.tesora.dve.sql.parser;

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

import com.tesora.dve.sql.util.UnaryFunction;

public final class ExtractedLiteral {

	// we use a simplified type scheme, since we're not doing full parsing
	public enum Type {
		STRING, INTEGRAL, DECIMAL, HEX
	};
	
	private final String text;
	private final Type type;
	private final int finalOffset;
	
	private ExtractedLiteral(String txt, Type t, int shrunkOffset) {
		text = txt;
		type = t;
		finalOffset = shrunkOffset;
	}
	
	public String getText() { return text; }
	public Type getType() { return type; }
	public int getFinalOffset() { return finalOffset; }
	
	@Override
	public String toString() {
		return getType() + ":{" + getText() + "}/" + finalOffset;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ExtractedLiteral other = (ExtractedLiteral) obj;
		if (text == null) {
			if (other.text != null)
				return false;
		} else if (!text.equals(other.text))
			return false;
		if (type != other.type)
			return false;
		if (finalOffset != other.finalOffset)
			return false;
		return true;
	}

	// constructors
	public static ExtractedLiteral makeStringLiteral(String in, int shrunkOff) {
		return new ExtractedLiteral(in,Type.STRING, shrunkOff);
	}
	
	public static ExtractedLiteral makeIntegralLiteral(String in, int shrunkOff) {
		return new ExtractedLiteral(in,Type.INTEGRAL, shrunkOff);
	}
	
	public static ExtractedLiteral makeDecimalLiteral(String in, int shrunkOff) {
		return new ExtractedLiteral(in,Type.DECIMAL, shrunkOff);
	}
	
	public static ExtractedLiteral makeHexLiteral(String in, int shrunkOff) {
		return new ExtractedLiteral(in,Type.HEX, shrunkOff);
	}
	
	public static final UnaryFunction<Type,ExtractedLiteral> typeAccessor = new UnaryFunction<Type,ExtractedLiteral>() {

		@Override
		public Type evaluate(ExtractedLiteral object) {
			return object.getType();
		}
		
	};
}
