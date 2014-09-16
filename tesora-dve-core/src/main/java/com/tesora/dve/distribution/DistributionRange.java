package com.tesora.dve.distribution;

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

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;
import javax.persistence.Table;

import org.hibernate.annotations.ForeignKey;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.CatalogQueryOptions;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.StorageGroupGeneration;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.annos.ColumnView;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.annos.TableView;
import com.tesora.dve.worker.WorkerGroup.MappingSolution;

@InfoSchemaTable(logicalName="distribution_range",
		views={@TableView(view=InfoView.SHOW, name="range", pluralName="ranges", extension=true, priviledged=true,
					columnOrder={ ShowSchema.Range.NAME, ShowSchema.Range.PERSISTENT_GROUP, ShowSchema.Range.SIGNATURE }),
			   @TableView(view=InfoView.INFORMATION, name="range_distribution", pluralName="", extension=true, priviledged=true,
					columnOrder={ "name", "storage_group", "signature" })})   
@Entity
@Table(name="distribution_range")
public class DistributionRange implements CatalogEntity {

	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(DistributionRange.class);

	@Id
	@GeneratedValue
	@Column(name = "range_id")
	private int id;

	@Column(name = "name", nullable = false)
	private String name;

	@ForeignKey(name="fk_range_group")
	@ManyToOne(optional = false)
	@JoinColumn(name = "persistent_group_id")
	private PersistentGroup storageGroup;
	
	@OneToMany(mappedBy="distributionRange", cascade=CascadeType.ALL)
	@OrderBy("version")
	List<GenerationKeyRange> rangeGenerations = new ArrayList<GenerationKeyRange>();
	
	private String signature;
	
	private transient ColumnSet showColumnSet = null;

	public DistributionRange() {
	}
	
	public DistributionRange(String name, PersistentGroup sg, String signature) {
		this.name = name;
		this.storageGroup = sg;
		this.signature = signature;
	}
	
	/**
	 * @throws PEException
	 */
	public void fullyLoad() throws PEException {
		// a hibernate workaround to load the data structures  
		for(StorageGroupGeneration sgg : storageGroup.getGenerations())
			sgg.getStorageSites().size();
		rangeGenerations.size();
	}
	
	public MappingSolution mapKeyToGeneration(IKeyValue key) throws PEException {
		StorageGroupGeneration groupGen = storageGroup.getLastGen();
		
		setComparatorClass(key);
		ListIterator<GenerationKeyRange> i = rangeGenerations.listIterator(rangeGenerations.size());
		while (i.hasPrevious()) {
            //TODO: this traverses from youngest to oldest, but returns oldest match or lastGen(), which can be confusing.  -sgossard
			GenerationKeyRange genKeyRange = i.previous();
			if (genKeyRange.isInRange(key))
				groupGen = genKeyRange.getStorageGroupGeneration();
			else
				break;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Mapping key " + key.toString() + " to generation " + groupGen.getVersion() + " using range " + this + " which has " + rangeGenerations.size() + " generations and is rep'd by object " + System.identityHashCode(this));
		}
		
		List<PersistentSite> siteList = groupGen.getStorageSites();
		int selectedMember = Math.abs(key.hashCode()) % siteList.size();
		return new MappingSolution(siteList.get(selectedMember));
	}

	private void setComparatorClass(IKeyValue key) {
		Map<String, String> typesNeedingComparator = getSignatureComparatorMap(signature);
		
		if (typesNeedingComparator.size() > 0) {
			Map<String, ? extends IColumnDatum> vals = key.getValues();
			for(String colName : vals.keySet()) {
				IColumnDatum icd = vals.get(colName);
				String nativeType = icd.getNativeType();
				if (typesNeedingComparator.containsKey(nativeType)) {
					icd.setComparatorClassName(typesNeedingComparator.get(nativeType));
				}
			}
		}
	}
	
	@InfoSchemaColumn(logicalName="name", fieldName="name",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.Range.NAME,orderBy=true,ident=true),
				   @ColumnView(view=InfoView.INFORMATION, name="name", orderBy=true, ident=true)})
	public String getName() {
		return name;
	}

	@InfoSchemaColumn(logicalName="signature", fieldName="signature",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.Range.SIGNATURE),
				   @ColumnView(view=InfoView.INFORMATION, name="signature")})
	public String getSignature() {
		return signature;
	}

	@InfoSchemaColumn(logicalName="storage_group", fieldName="storageGroup",
			sqlType=java.sql.Types.VARCHAR,sqlWidth=255,
			views={@ColumnView(view=InfoView.SHOW, name=ShowSchema.Range.PERSISTENT_GROUP),
				   @ColumnView(view=InfoView.INFORMATION, name="storage_group")})
	public PersistentGroup getStorageGroup() {
		return storageGroup;
	}
	
	public void addRangeGeneration(GenerationKeyRange generationKeyRange) {
		rangeGenerations.add(generationKeyRange);
	}

	public GenerationKeyRange getRangeForGeneration(StorageGroupGeneration newGen) {
		GenerationKeyRange theKeyRange = null;
		for (GenerationKeyRange keyRange : rangeGenerations) {
			if (keyRange.getStorageGroupGeneration().equals(newGen)) {
				theKeyRange = keyRange;
				break;
			}
		}
		return theKeyRange;
	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo) {
		if (showColumnSet == null) {
			showColumnSet = new ColumnSet();
			showColumnSet.addColumn("Name", 255, "varchar", Types.VARCHAR);
			showColumnSet.addColumn("StorageGroup", 255, "varchar", Types.VARCHAR);
			showColumnSet.addColumn("Signature", 255, "varchar", Types.VARCHAR);
		}
		return showColumnSet;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo) {
		ResultRow rr = new ResultRow();
		rr.addResultColumn(this.name, false);
		rr.addResultColumn(this.storageGroup.getName(), false);
		rr.addResultColumn(this.signature);
		return rr;
	}

	@Override
	public void removeFromParent() {
		// TODO Actually implement the removal of this instance from the parent
	}

	@Override
	public List<CatalogEntity> getDependentEntities(CatalogDAO c) throws Throwable {
		// TODO Return a valid list of dependents
		return Collections.emptyList();
	}
	
	@Override
	public String toString() {
		return "DistributionRange(" + id + ", " + name + ", " + signature + ")"; 
 	}
	
	@Override
	@InfoSchemaColumn(logicalName="id",fieldName="id",
			sqlType=java.sql.Types.INTEGER,
			views={})
	public int getId() {
		return id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
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
		DistributionRange other = (DistributionRange) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

	@Override
	public void onUpdate() {
		// do nothing
	}

	@Override
	public void onDrop() {
		// do nothing
	}
	
	private static Map<String, Map<String, String>> signatureComparatorMap = new ConcurrentHashMap<String, Map<String, String>>();
	
	public Map<String, String> getSignatureComparatorMap(String signatureKey) {
		Map<String, String> comparatorMap = signatureComparatorMap.get(signatureKey);
		if (comparatorMap == null) {
			comparatorMap = decodeSignatureIntoNativeTypeComparatorMap(signatureKey);
			signatureComparatorMap.put(signatureKey, comparatorMap);
		}
		return comparatorMap;
	}

	public static Map<String, String> decodeSignatureIntoNativeTypeComparatorMap(String signature) {
		Map<String, String> nativeTypeToComparatorMap = new HashMap<String, String>();
		String[] colSignatures = signature.split(",");
		for(String colSignature : colSignatures) {
			if (colSignature.contains(":")) {
				String[] parts = colSignature.split(":");
				if (!StringUtils.isEmpty(parts[1])) {
					// modify key value with the name of the comparator to use
					nativeTypeToComparatorMap.put(parts[0], parts[1]);
				}
			}
		}
		return nativeTypeToComparatorMap;
	}
}
