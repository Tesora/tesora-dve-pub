package com.tesora.dve.common.catalog;

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

import java.util.Collections;
import java.util.List;

import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

@Entity
@Table(name="server")
@Cacheable(value=false)
public class ServerRegistration implements CatalogEntity {

	private static final long serialVersionUID = 1L;
	
	@Id
	@GeneratedValue
	@Column(name="server_id")
	int id;
	
	@Column(name="ipAddress")
	String ipAddress;
	
	@Column(name="name")
	String name;
	
	public ServerRegistration(String name, String ipAddress) {
		this.name = name;
		this.ipAddress = ipAddress;
	}

	public ServerRegistration(int id, String name, String ipAddress) {
		this.id = id;
		this.name = name;
		this.ipAddress = ipAddress;
	}

	public ServerRegistration() {
	}

	@Override
	public int getId() {
		return this.id;
	}
	
	public String getIpAddress() {
		return this.ipAddress;
	}
	
	public String getName() {
		return this.name;
	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo) throws PEException {
		return null;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo) throws PEException {
		return null;
	}

	@Override
	public void removeFromParent() throws Throwable {
		// no-op
	}

	@Override
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c) throws Throwable {
		return Collections.emptyList();
	}

	@Override
	public void onUpdate() {
		// no-op

	}

	@Override
	public void onDrop() {
		// no-op
	}

	@Override
	public String toString() {
		return this.getClass().getSimpleName()+"("+id+","+name+","+ipAddress+")";
	}

}
