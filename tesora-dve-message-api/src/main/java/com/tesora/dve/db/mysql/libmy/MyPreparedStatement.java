package com.tesora.dve.db.mysql.libmy;

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

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.exceptions.PEException;


public class MyPreparedStatement<StmtIdType> {

//	String ssStmtId;
	StmtIdType stmtId;
	byte[] query;
	boolean newParameterList = true;
	int numColumns = 0;
	int numParams = 0;
	List<MyParameter> parameters = new ArrayList<MyParameter>();
	
	public MyPreparedStatement(StmtIdType stmtId) {
		this.stmtId = stmtId; 
	}
	
	public StmtIdType getStmtId() {
		return stmtId;
	}
	public void setStmtId(StmtIdType stmtId) {
		this.stmtId = stmtId;
	}
	public byte[] getQuery() {
		return query;
	}
	public void setQuery(byte[] query) {
		this.query = query;
	}
	public int getNumColumns() {
		return numColumns;
	}
	public void setNumColumns(int numColumns) {
		this.numColumns = numColumns;
	}
	public int getNumParams() {
		return numParams;
	}
	public void setNumParams(int numParams) {
		this.numParams = numParams;
	}
	
	public void clearParameters() {
		parameters = new ArrayList<MyParameter>();
		newParameterList = true;
	}
	
	public void addParameter(MyParameter param) throws PEException {
		if ( parameters.size() == numParams ) {
			throw new PEException( "Cannot add new parameter; list already contains number of parameters specified on prepare");
		}
		parameters.add(param);
	}
	
	public void setParameter(int index, MyParameter param) {
		parameters.set(index - 1, param);
	}
	
	public MyParameter getParameter(int index) {
		return parameters.get(index - 1);
	}
	
	public List<MyParameter> getParameters() {
		return parameters;
	}
	
	public void clearNewParameterFlag() {
		newParameterList = false;
	}
	
	public boolean isNewParameterList() {
		return newParameterList;
	}

	@Override
	public String toString() {
		return super.toString() + "{" + stmtId + "}";
	}
	
}
