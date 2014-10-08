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


import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OrderBy;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

import org.hibernate.annotations.ForeignKey;

import com.tesora.dve.common.PECollectionUtils;
import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.distribution.PELockedException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

@Entity
@Table(name="storage_generation")
@XmlAccessorType(XmlAccessType.NONE)
public class StorageGroupGeneration implements CatalogEntity {

	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column(name="generation_id")
	int id;
	
	@Column(name="version", nullable=false)
	int version;
	
	@Column(name="locked", nullable=false)
	boolean locked ;
	
	@XmlElement(name="StorageSite")
	@ManyToMany(fetch=FetchType.LAZY)
	@JoinTable(name="generation_sites",
			joinColumns=
				@JoinColumn(name="generation_id"),
				inverseJoinColumns=
					@JoinColumn(name="site_id")
	)
	@OrderBy("id ASC")
	List<PersistentSite> groupMembers;

	@ForeignKey(name="fk_sg_gen_group")
	@ManyToOne
	@JoinColumn(name="persistent_group_id",nullable=false)
	PersistentGroup storageGroup;
	
	@Override
	public boolean equals(Object o) {
		StorageGroupGeneration other = (StorageGroupGeneration) o;
		boolean isEqual = false;
		if (this.version == other.version
				&& this.locked == other.locked
				&& this.storageGroup.id == other.storageGroup.id
				&& this.groupMembers.size() == other.groupMembers.size()) {
			isEqual = true;
			for (int i = 0; i < groupMembers.size(); ++i) {
				if (this.groupMembers.get(i).id != other.groupMembers.get(i).id) {
					isEqual = false;
					break;
				}
			}
		}
		return isEqual;
	}
	
	public StorageGroupGeneration() {
	}
	
	public StorageGroupGeneration(PersistentGroup sg, int ver, List<PersistentSite> newGenSites) {
		version = ver;
		locked = false;
		groupMembers = new ArrayList<PersistentSite>(newGenSites);
		storageGroup = sg;
	}
	
	public StorageGroupGeneration(PersistentGroup sg, int ver) {
		version = ver;
		locked = false;
		groupMembers = new ArrayList<PersistentSite>();
		storageGroup = sg;
	}

	/**
	 * Adds the {@link PersistentSite} specified by <em>ds</em>
	 * to the <b>StorageGroup</b>
	 * 
	 * @param ds site to add
	 * @return itself for inline invocation
	 * @throws PELockedException if group is locked
	 */
	public void addStorageSite(PersistentSite ds) throws PELockedException {
		abortIfLocked();
		groupMembers.add(ds);
	}
	
	/**
	 * Removes the {@link PersistentSite} specified by <em>ds</em> from
	 * the <b>StorageGroup</b>.
	 * 
	 * @param ds site to remove
	 * @throws PELockedException if group is locked
	 */
	public void removeStorageSite(PersistentSite ds) throws PELockedException {
		abortIfLocked();
		groupMembers.remove(ds);
	}

	/**
	 * Returns a non-modifiable list of {@link PersistentSite} instances 
	 * in the <b>StorageGroup</b>
	 * 
	 * @return list of sites in group
	 */
	public final List<PersistentSite> getStorageSites() {
		return groupMembers;
	}
	
	/**
	 * Returns a temporary <b>StorageGroup</b> consisting of a single
	 * {@link PersistentSite} picked at random from this group.
	 * @return
	 */
	public PersistentSite anySite() {
		return PECollectionUtils.selectRandom(groupMembers);
	}

	public boolean isLocked() {
		return locked;
	}

	void abortIfLocked() throws PELockedException {
		if (locked)
			throw new PELockedException("StorageGroup '"+storageGroup.getName()+"' is locked and cannot be modified");
	}

	/**
	 * Returns the version number of the persistent <b>StorageGroup</b>
	 * @return the version
	 */
	public int getVersion() {
		return version;
	}

	void toString(StringBuffer sb) {
		PEStringUtils.toString(Integer.toString(version), groupMembers);
	}
	
	@Override
	public String toString() {
		return PEStringUtils.toString(Integer.toString(version), groupMembers);
	}

	void add(Collection<PersistentSite> allSites) throws PELockedException {
		if (locked)
			throw new PELockedException("Cannot add sites to locked generation of StorageGroup " + storageGroup.toString());
		groupMembers.addAll(allSites);
	}

	public void lock() {
		locked = true;
	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo) {
		ColumnSet showColumnSet = new ColumnSet();
		showColumnSet.addColumn("ID",3,"integer",Types.INTEGER);
		showColumnSet.addColumn(PersistentGroup.PERSISTENT_GROUP_CS_HEADER_VAL,255,"varchar",Types.VARCHAR);
		showColumnSet.addColumn("Version",3,"integer",Types.INTEGER);
		showColumnSet.addColumn("Locked",3,"varchar",Types.VARCHAR);
		return showColumnSet;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo) {
		ResultRow rr = new ResultRow();
		rr.addResultColumn(this.id, false);
		rr.addResultColumn(this.storageGroup.name, false);
		rr.addResultColumn(this.version, false);
		rr.addResultColumn((this.locked ? "Yes" : "No"), false);
		return rr;
	}

	@Override
	public void removeFromParent() throws Throwable {
		// TODO Actually implement the removal of this instance from the parent
		//	storageGroup.removeGeneration(this);
	}

	@Override
	public List<CatalogEntity> getDependentEntities(CatalogDAO c) throws Throwable {
		// TODO Return a valid list of dependents
		return Collections.emptyList();
	}

	@Override
	public int getId() {
		return id;
	}

	public PersistentGroup getStorageGroup() {
		return storageGroup;
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}
}
