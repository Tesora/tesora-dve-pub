// OS_STATUS: public
package com.tesora.dve.sql.schema;

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

public class SizeTypeAttribute {

	public enum SizeTypeAttributeEnum {
		DEFAULT_SIZE_TYPE((byte) 0x01),
		FLOAT_TYPE((byte) 0x02),

		UNKNOWN_TYPE((byte) 0x00);

		private final byte sizeTypeAsByte;
		
		private SizeTypeAttributeEnum(byte b) {
			sizeTypeAsByte = b;
		}
		
		public static SizeTypeAttributeEnum fromByte(byte b) {
			for (SizeTypeAttributeEnum mt : values()) {
				if (mt.sizeTypeAsByte == b) {
					return mt;
				}
			}
			return UNKNOWN_TYPE;
		}
		
		public byte getByteValue() {
			return sizeTypeAsByte;
		}
	}
	
	private int size;
	private SizeTypeAttributeEnum sizeType;
	
	public SizeTypeAttribute(int s) {
		this(s, SizeTypeAttributeEnum.DEFAULT_SIZE_TYPE);
	}
	
	public SizeTypeAttribute(int s, SizeTypeAttributeEnum sizeType) {
		this.size = s;
		this.setSizeType(sizeType);
	}
	
	public int getSize() { return size; }

	@Override
	public String toString() {
		return "SizeTypeAttribute [size=" + size + ", sizeType=" + sizeType + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + size;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SizeTypeAttribute other = (SizeTypeAttribute) obj;
		if (size != other.size)
			return false;
		return true;
	}

	public SizeTypeAttributeEnum getSizeType() {
		return sizeType;
	}

	public void setSizeType(SizeTypeAttributeEnum sizeType) {
		this.sizeType = sizeType;
	}
	
}
