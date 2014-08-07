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

import java.io.Serializable;
import java.sql.ParameterMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.tesora.dve.charset.NativeCharSetCatalog;
import com.tesora.dve.charset.NativeCollationCatalog;
import com.tesora.dve.common.DBType;
import com.tesora.dve.common.catalog.PersistentTable;
import com.tesora.dve.common.catalog.User;
import com.tesora.dve.common.catalog.UserColumn;
import com.tesora.dve.db.mysql.MariaDBNative;
import com.tesora.dve.db.mysql.MysqlNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnMetadata;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.ForeignKeyAction;
import com.tesora.dve.sql.schema.types.Type;

public abstract class DBNative implements Serializable {
	private static final long serialVersionUID = 1L;

	public static final String DVE_SITENAME_VAR = "dve_sitename";

	private String literalQuoteChar;
	private String identifierQuoteChar;
	private NativeTypeCatalog typeCatalog;
	private DBType dbType;
	private NativeResultHandler resultHandler;

	// stuff relating to the parser
	private transient ValueConverter valueConverter = ValueConverter.INSTANCE;
	private transient Emitter emitter;
	private String charsetEncoding = "UTF-8";

	/**
	 * Populate the DBMetadata object and return - typically used to send
	 * a subset of DBNative to a client application (i.e. *DBC)
	 * 
	 * @return DBMetadata
	 */
	public DBMetadata getDBMetadata() {
		DBMetadata dbMeta = new DBMetadata(dbType);
		dbMeta.setCharsetEncoding(charsetEncoding);
		dbMeta.setIdentifierQuoteChar(identifierQuoteChar);
		dbMeta.setLiteralQuoteChar(literalQuoteChar);
		dbMeta.setResultHandler(resultHandler);
		dbMeta.setTypeCatalog(typeCatalog);
		
		return dbMeta;
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
	}

	public void setTypeCatalog(NativeTypeCatalog tc) throws PEException {
		this.typeCatalog = tc;
		typeCatalog.load();
	}

	public NativeType findType(String typeName) throws PEException {
		return typeCatalog.findType(typeName, true);
	}

	public int typeCatalogSize() {
		return typeCatalog.size();
	}

	public ValueConverter getValueConverter() {
		return valueConverter;
	}

	public Emitter getEmitter() {
		return emitter;
	}

	public void setEmitter(Emitter e) {
		emitter = e;
	}

	public NativeResultHandler getResultHandler() {
		return resultHandler;
	}

	public void setResultHandler(NativeResultHandler resultHandler) {
		this.resultHandler = resultHandler;
	}

	public abstract NativeCharSetCatalog getSupportedCharSets();

	public abstract NativeCollationCatalog getSupportedCollations();

	/**
	 * Abstract methods for DB specific SQL statements
	 */
	public abstract SQLCommand getDropDatabaseStmt(String databaseName);

	public abstract SQLCommand getCreateDatabaseStmt(String databaseName, boolean ine, String defaultCharSet, String defaultCollation);

	public abstract SQLCommand getAlterDatabaseStmt(String databaseName, String defaultCharSet, String defaultCollation);

	public abstract SQLCommand getCreateUserCommand(User user);

	public abstract SQLCommand getGrantPriviledgesCommand(String userDeclaration, String databaseName);

	public abstract String getUserDeclaration(User user, boolean emitPassword);

	/**
	 * Method to populate a ColumnMetadata given a JDBC ResultSetMetaData and a column index
	 * This allows for any database native logic in the population of the ColumnMetadata such
	 * as the way Mysql returns "UNSIGNED" as part of the ColumnTypeName instead of as a
	 * modifier to the type.
	 * 
	 * @param rsmd - ResultSetMetaData
	 * @param projection - ProjectionInfo from parser containing extra info not in RSMD
	 * @param colIdx - index of column to use
	 * @return ColumnMetadata - a new instance of a ColumnMetadata populated from rsmd
	 *         for colIdx
	 * @throws SQLException
	 */
	public abstract ColumnMetadata getResultSetColumnInfo(ResultSetMetaData rsmd, ProjectionInfo projection,
			int colIdx) throws SQLException;

	public abstract ColumnMetadata getParameterColumnInfo(ParameterMetaData pmd, int colIdx) throws SQLException;
	
	public abstract UserColumn updateUserColumn(UserColumn uc, Type schemaType);

	/**
	 * Returns the name of the UserTable object appropriately quoted
	 * 
	 * @param ut
	 *            - UserTable object
	 * @return String - name appropriately quoted for use in a SQL statement
	 */
	public String getNameForQuery(PersistentTable ut) {
		return quoteIdentifier(ut.getPersistentName());
	}

	/**
	 * Returns the name of the UserColumn object appropriately quoted
	 * 
	 * @param uc
	 *            - UserColumn object
	 * @return String - name appropriately quoted for use in a SQL statement
	 */
	public String getNameForQuery(UserColumn uc) {
		return quoteIdentifier(uc.getQueryName());
	}
	public String getNameForQuery(ColumnMetadata cm) {
		return quoteIdentifier(cm.getQueryName());
	}

	/**
	 * Returns a string appropriately quoted
	 * 
	 * @param String
	 * @return String - appropriately quoted for use in a SQL statement
	 */
	public String quoteIdentifier(String name) {
		return new StringBuffer(getIdentifierQuoteChar()).append(name).append(getIdentifierQuoteChar()).toString();
	}

	public String quoteLiteral(String name) {
		return new StringBuffer(getLiteralQuoteChar()).append(name).append(getLiteralQuoteChar()).toString();
	}

	public static class DBNativeFactory {
		public static DBNative newInstance(DBType dbType) throws PEException {
			DBNative dbNative = null;

			switch (dbType) {
			case MYSQL:
				dbNative = new MysqlNative();
				break;
			case MARIADB:
				dbNative = new MariaDBNative();
				break;
			default:
				throw new PEException("Attempt to create new instance using invalid database type " + dbType);
			}
			return dbNative;
		}
	}

	/**
	 * Returns the native type name for use in SQL statement. Will include
	 * proper syntax for size, precision and scale as appropriate.
	 * 
	 * @param uc
	 *            - UserColumn object
	 * @return String - Native Type name
	 * @throws PEException
	 */
	public abstract String getDataTypeForQuery(UserColumn uc) throws PEException;

	public abstract String getColumnDefForQuery(UserColumn uc) throws PEException;

	public void convertColumnMetadataToUserColumn(ColumnMetadata cm, UserColumn uc) throws PEException {
		uc.setName(cm.getName());
		uc.setDataType(cm.getDataType());
		uc.setNativeTypeName(cm.getNativeTypeName());
		uc.setNativeTypeModifiers(cm.getNativeTypeModifiers());
		uc.setSize(cm.getSize());
		uc.setAutoGenerated(cm.getAutoGenerated());
		uc.setDefaultValue(cm.getDefaultValue());
		uc.setHasDefault(cm.getHasDefault());
		uc.setDefaultValueIsConstant(cm.getDefaultValueIsConstant());
		uc.setPrecision(cm.getPrecision());
		uc.setScale(cm.getScale());
		uc.setHashPosition(cm.getHashPosition());
		uc.setNullable(cm.getNullable());
		uc.setOnUpdate(cm.getOnUpdate());
	}
	
	// TODO these need to be investigated and probably removed/changed
	public String getCharsetEncoding() {
		return charsetEncoding;
	}

	public void setCharsetEncoding(String charsetEncoding) {
		this.charsetEncoding = charsetEncoding;
	}

	public abstract String getEmptyCatalogName();

	public abstract String getPasswordForAuth(User user, SSConnection ssConn) throws Exception;

	public abstract String getSessionVariableConfigName();

	public abstract String getStatusVariableConfigName();

	public abstract String getSetSessionVariableStatement(String assignmentClause);

	public abstract String getSetAutocommitStatement(String value);

	public abstract void assertValidCollation(String value) throws PEException;

	public abstract int convertTransactionIsolationLevel(String in) throws PEException;

	public abstract String convertTransactionIsolationLevel(int level) throws PEException;

	public abstract String getDefaultServerCharacterSet();

	public abstract String getDefaultServerCollation();

	public abstract String getDefaultServerBinaryCollation();
	
	/**
	 * Returns true if executing a "begin" statement should cause an implicit
	 * commit of any in-flight transactions.
	 * 
	 * @return true if begin implies commit
	 */
	public abstract boolean beginImpliesCommit();

	/**
	 * Returns true if executing a DDL statement should cause an implicit commit
	 * of any in-flight transactions.
	 * 
	 * @return true if DDL statements imply commit
	 */
	public abstract boolean ddlImpliesCommit();

	public abstract boolean exceptionAbortsTxn(PEException e);

	public abstract String getTableRenameStatement(String existingTempTableName, String tempTableName);

	public abstract int getMaxAliasNameLen();

	public int getMaxNumColsInIndex() {
		return Integer.MAX_VALUE;
	}

	/**
	 * Default ON DELETE FK referential action.
	 */
	public abstract ForeignKeyAction getDefaultOnDeleteAction();

	/**
	 * Default ON UPDATE FK referential action.
	 */
	public abstract ForeignKeyAction getDefaultOnUpdateAction();
}
