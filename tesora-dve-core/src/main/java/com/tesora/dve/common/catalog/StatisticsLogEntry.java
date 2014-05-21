// OS_STATUS: public
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

import java.util.Calendar;

import javax.persistence.Cacheable;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.Index;

import com.tesora.dve.server.statistics.SiteStatKey.OperationClass;


@Entity
@Table(name="statistics_log")
@Cacheable(value=false)
@SuppressWarnings("UCD")
public class StatisticsLogEntry {

	@Id @GeneratedValue
	int id;
	
	@Index(name = "EntryDate") @Temporal(TemporalType.TIMESTAMP) 
	Calendar timestamp = Calendar.getInstance();
	String type;
	String name;
	@Enumerated(EnumType.STRING) 
	OperationClass opClass;
	float responseTime;
	int opCount;
	

	public StatisticsLogEntry(String type, String name,
			OperationClass opClass, float responseTime, int opCount) {
		super();
		this.type = type;
		this.name = name;
		this.opClass = opClass;
		this.responseTime = responseTime;
		this.opCount = opCount;
	}
	
}
