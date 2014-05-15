// OS_STATUS: public
package com.tesora.dve.db;

import java.io.Serializable;

import com.tesora.dve.common.DBType;
import com.tesora.dve.exceptions.PEException;

public class DBMetadata implements Serializable {
	private static final long serialVersionUID = 1L;

	private String literalQuoteChar;
	private String identifierQuoteChar;
	private NativeTypeCatalog typeCatalog;
	private DBType dbType;
	private NativeResultHandler resultHandler;
	private String charsetEncoding;

	protected DBMetadata() {
	}
	
	public DBMetadata( DBType dbType ) {
		this.dbType = dbType;
	}
	
	// TODO these need to be investigated and probably removed/changed
	public String getCharsetEncoding() {
		return charsetEncoding;
	}

	public void setCharsetEncoding(String charsetEncoding) {
		this.charsetEncoding = charsetEncoding;
	}

	public String getLiteralQuoteChar() {
		return literalQuoteChar;
	}

	public void setLiteralQuoteChar(String quoteChar) {
		this.literalQuoteChar = quoteChar;
	}
	
	public String getIdentifierQuoteChar() {
		return identifierQuoteChar;
	}

	public void setIdentifierQuoteChar(String quoteChar) {
		this.identifierQuoteChar = quoteChar;
	}

	public DBType getDbType() {
		return dbType;
	}

	public void setDbType(DBType dbType) {
		this.dbType = dbType;
	}

	public NativeTypeCatalog getTypeCatalog() {
		return typeCatalog;
	};

	public void setTypeCatalog(NativeTypeCatalog typeCatalog) {
		this.typeCatalog = typeCatalog;
	}

	public NativeType findType(String typeName) throws PEException {
		return typeCatalog.findType(typeName, true);
	}

	public NativeResultHandler getResultHandler() {
		return resultHandler;
	}

	public void setResultHandler(NativeResultHandler resultHandler) {
		this.resultHandler = resultHandler;
	}

	public String quoteIdentifier(String name) {
		return new StringBuffer(getIdentifierQuoteChar()).append(name)
				.append(getIdentifierQuoteChar()).toString();
	}
	
	public String quoteLiteral(String name) {
		return new StringBuffer(getLiteralQuoteChar()).append(name)
				.append(getLiteralQuoteChar()).toString();
	}
}
