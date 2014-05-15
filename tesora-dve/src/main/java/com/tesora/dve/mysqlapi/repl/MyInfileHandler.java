// OS_STATUS: public
package com.tesora.dve.mysqlapi.repl;

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
