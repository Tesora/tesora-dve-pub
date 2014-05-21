// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl;

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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class MyInfileHandler {

	int fileId = -1;
	File infile = null; 
	
	public MyInfileHandler() {
	}
	
	public File getInfile(int fileId) {
		if (infile == null) {
			infile = FileUtils.getFile(Integer.toString(fileId) + ".txt");
			if (!infile.exists()) {
				infile = null;
			}
		}
		return infile;
	}

	public File createInfile(int fileId) {
		if (infile != null) {
			cleanUp();
		}
		infile = FileUtils.getFile(Integer.toString(fileId) + ".txt");
		
		return infile;
	}
	
	public void addInitialBlock(int fileId, byte[] block) throws IOException {
		FileUtils.writeByteArrayToFile(getInfile(fileId), block);
	}

	public void addBlock(int fileId, byte[] block) throws IOException {
		FileUtils.writeByteArrayToFile(getInfile(fileId), block, true);
	}
	
	public void cleanUp() {
		FileUtils.deleteQuietly(infile);
		infile = null;
		fileId = -1;
	}
}
