package com.tesora.dve.common;

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

public class DVEPublicCryptoSettings {

	static final byte[] salt = { (byte) 0x74, (byte) 0x69, (byte) 0x6E, (byte) 0x61, (byte) 0x68, (byte) 0x65,
		(byte) 0x6C, (byte) 0x79 };

	static final String password = "Shard starts with S and ends with Hard";

	static final int iterations = 13;

	
}
