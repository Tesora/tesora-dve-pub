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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import javax.persistence.AttributeOverride;
import javax.persistence.AttributeOverrides;
import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.annos.ColumnView;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.variable.VariableValueConverter;

@InfoSchemaTable(logicalName="dynamic_policy",
		views={})
@Entity
@Table(name="dynamic_policy")
public class DynamicPolicy implements CatalogEntity, IDynamicPolicy {

	private static final long serialVersionUID = 1L;

	public static final String AGGREGATION = PEConstants.AGGREGATION;
	public static final String SMALL = PEConstants.SMALL;
	public static final String MEDIUM = PEConstants.MEDIUM;
	public static final String LARGE = PEConstants.LARGE;
	
	public static final String STRICT = PEConstants.STRICT;
	
	@Id
	@GeneratedValue
	@Column( name="policy_id" )
	int id;
	
	@Column(name="name",nullable=false)
	String name;
	
	@Column(name="strict", nullable=false)
	boolean strict;
	
	@Embedded
	@AttributeOverrides({
		@AttributeOverride(name="provider",column=@Column(name="aggregate_provider")),
		@AttributeOverride(name="className",column=@Column(name="aggregate_class")),
		@AttributeOverride(name="count",column=@Column(name="aggregate_count"))
	})
	DynamicGroupClass aggregationClass;
	@Embedded
	@AttributeOverrides({
		@AttributeOverride(name="provider",column=@Column(name="small_provider")),
		@AttributeOverride(name="className",column=@Column(name="small_class")),
		@AttributeOverride(name="count",column=@Column(name="small_count"))
	})
	DynamicGroupClass smallClass;
	@Embedded
	@AttributeOverrides({
		@AttributeOverride(name="provider",column=@Column(name="medium_provider")),
		@AttributeOverride(name="className",column=@Column(name="medium_class")),
		@AttributeOverride(name="count",column=@Column(name="medium_count"))
	})
	DynamicGroupClass mediumClass;
	@Embedded
	@AttributeOverrides({
		@AttributeOverride(name="provider",column=@Column(name="large_provider")),
		@AttributeOverride(name="className",column=@Column(name="large_class")),
		@AttributeOverride(name="count",column=@Column(name="large_count"))
	})
	DynamicGroupClass largeClass;

	private transient ColumnSet showColumnSet = null;
	
	public DynamicPolicy() {
	}
	
	public DynamicPolicy(String name, boolean strict,
			String aggProvider, String aggClass, int aggCount,
			String smallProvider, String smallClass, int smallCount,
			String mediumProvider, String mediumClass, int mediumCount, 
			String largeProvider, String largeClass, int largeCount) {
		super();
		this.name = name;
		this.strict = strict;
		this.smallClass = new DynamicGroupClass(SMALL, smallProvider, smallClass, smallCount);
		this.aggregationClass = new DynamicGroupClass(AGGREGATION, aggProvider, aggClass, aggCount);
		this.mediumClass = new DynamicGroupClass(MEDIUM, mediumProvider, mediumClass, mediumCount);
		this.largeClass = new DynamicGroupClass(LARGE, largeProvider, largeClass, largeCount);
	}

	@InfoSchemaColumn(logicalName="name",fieldName="name",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo)
			throws PEException {
		if (showColumnSet == null) {
			showColumnSet = new ColumnSet();
			showColumnSet.addColumn("Name", 255, "varchar", Types.VARCHAR);
			showColumnSet.addColumn("Config", 255, "varchar", Types.VARCHAR);
		}
		return showColumnSet;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo)
			throws PEException {
		ResultRow rr = new ResultRow();
		rr.addResultColumn(getName());
		rr.addResultColumn(getConfigString());
		return rr;
	}

	@Override
	public void removeFromParent() throws Throwable {
	}

	@Override
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c)
			throws Throwable {
		return Collections.emptyList();
	}

	public DynamicGroupClass getAggregationClass() {
		return aggregationClass;
	}
	
	public void setAggregationClass(DynamicGroupClass aggregationClass) {
		this.aggregationClass = aggregationClass;
	}

	public DynamicGroupClass getSmallClass() {
		return smallClass;
	}

	public void setSmallClass(DynamicGroupClass smallClass) {
		this.smallClass = smallClass;
	}

	public DynamicGroupClass getMediumClass() {
		return mediumClass;
	}

	public void setMediumClass(DynamicGroupClass mediumClass) {
		this.mediumClass = mediumClass;
	}

	public DynamicGroupClass getLargeClass() {
		return largeClass;
	}

	public void setLargeClass(DynamicGroupClass largeClass) {
		this.largeClass = largeClass;
	}

	@InfoSchemaColumn(logicalName="strict",fieldName="strict",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={})
	public boolean getStrict() {
		return strict;
	}
	
	public void setStrict(boolean strict) {
		this.strict = strict;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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
		DynamicPolicy other = (DynamicPolicy) obj;
		if (id != other.id)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

	@Override
	public int getId() {
		return id;
	}

	public static DynamicPolicy parse(String name, Properties props) throws PEException {
		DynamicPolicy ndp = new DynamicPolicy();
		ndp.setName(name);
		ndp.parse(props);
		return ndp;
	}
	
	public static DynamicPolicy parse(String name, String configVal) throws PEException {
		DynamicPolicy ndp = new DynamicPolicy();
		ndp.setName(name);
		ndp.parse(configVal);
		return ndp;
	}
	
	public void parse(Properties props) throws PEException {
		String val = props.getProperty(DynamicPolicy.AGGREGATION);
		if (val == null)
			throw new PEException("'" + DynamicPolicy.AGGREGATION + "' property not specified");
		setAggregationClass(new DynamicGroupClass(DynamicPolicy.AGGREGATION, val));

		val = props.getProperty(DynamicPolicy.SMALL);
		if (val == null)
			throw new PEException("'" + DynamicPolicy.SMALL + "' property not specified");
		setSmallClass(new DynamicGroupClass(DynamicPolicy.SMALL, val));

		val = props.getProperty(DynamicPolicy.MEDIUM);
		if (val == null)
			throw new PEException("'" + DynamicPolicy.MEDIUM + "' property not specified");
		setMediumClass(new DynamicGroupClass(DynamicPolicy.MEDIUM, val));

		val = props.getProperty(DynamicPolicy.LARGE);
		if (val == null)
			throw new PEException("'" + DynamicPolicy.LARGE + "' property not specified");
		setLargeClass(new DynamicGroupClass(DynamicPolicy.LARGE, val));
		
		val = props.getProperty(DynamicPolicy.STRICT);
		boolean bVal = true;
		if(val != null) {
			bVal = VariableValueConverter.toInternalBoolean(val);
		}
		setStrict(bVal);
	}

	public void parse(String configVal) throws PEException {
		StringReader reader = new StringReader(configVal);
		Properties props = new Properties();
		try {
			props.load(reader);
		} catch (IOException ioe) {
			throw new PEException("Invalid format for configuration",ioe);
		}
		parse(props);
	}

	// undoes what the previous two methods do
	public Properties toProperties() {
		Properties props = new Properties();
		props.setProperty(DynamicPolicy.AGGREGATION, getAggregationClass().buildConfigString());
		props.setProperty(DynamicPolicy.SMALL, getSmallClass().buildConfigString());
		props.setProperty(DynamicPolicy.MEDIUM, getMediumClass().buildConfigString());
		props.setProperty(DynamicPolicy.LARGE, getLargeClass().buildConfigString());
		props.setProperty(DynamicPolicy.STRICT, VariableValueConverter.toExternalString(getStrict()));
		return props;
	}

	@InfoSchemaColumn(logicalName="config",fieldName="",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.GroupPolicy.CONFIG),
			       @ColumnView(view=InfoView.INFORMATION, name="config")})
	public String getConfigString() throws PEException {
		// blech, can't use the store method on properties - always emits the date
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		try {
			pw.println(DynamicPolicy.AGGREGATION + "=" + getAggregationClass().buildConfigString());
			pw.println(DynamicPolicy.SMALL + "=" + getSmallClass().buildConfigString());
			pw.println(DynamicPolicy.MEDIUM + "=" + getMediumClass().buildConfigString());
			pw.println(DynamicPolicy.LARGE + "=" + getLargeClass().buildConfigString());
			pw.println(DynamicPolicy.STRICT + "=" + getStrict());
			pw.close();
			sw.close();
		} catch (IOException ioe) {
			throw new PEException("Unable to build configuration format",ioe);
		}
		return sw.toString();
	}
	
	// used in alter support - just take the types
	public void take(DynamicPolicy otherPolicy) throws PEException {
		setAggregationClass(otherPolicy.getAggregationClass());
		setSmallClass(otherPolicy.getSmallClass());
		setMediumClass(otherPolicy.getMediumClass());
		setLargeClass(otherPolicy.getLargeClass());
		setStrict(otherPolicy.getStrict());
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}
		
}
