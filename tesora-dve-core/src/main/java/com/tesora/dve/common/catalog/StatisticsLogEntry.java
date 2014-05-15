// OS_STATUS: public
package com.tesora.dve.common.catalog;

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
