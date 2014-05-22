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


import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.hibernate.annotations.ForeignKey;

import com.tesora.dve.common.catalog.StorageGroupGeneration;
import com.tesora.dve.exceptions.PEException;

@Entity
@Table(name="generation_key_range")
public class GenerationKeyRange {

	@Id
	@GeneratedValue
	@Column(name = "key_gen_id")
	private int id;
	
	@SuppressWarnings("unused")
	private int version;
	
	@ForeignKey(name="fk_gen_key_range_range")
	@ManyToOne(optional = false, fetch=FetchType.EAGER)
	@JoinColumn(name = "range_id")
	private DistributionRange distributionRange;

	@ForeignKey(name="fk_gen_key_range_gen")
	@ManyToOne(optional = false)
	@JoinColumn(name = "generation_id")
	private StorageGroupGeneration generation;

	@Lob @Column(name="range_start", nullable=false)
	String rangeStartPersisted;
	
	@Lob @Column(name="range_end", nullable=false)
	String rangeEndPersisted;
	
	@Transient
	RangeLimit rangeStart = null;
	
	@Transient
	RangeLimit rangeEnd;
	
	public GenerationKeyRange() {
	}
	
	public GenerationKeyRange(DistributionRange distRange, StorageGroupGeneration gen, KeyValue dvStart, KeyValue dvEnd) throws PEException {
		distributionRange = distRange;
		generation = gen;
		version = gen.getVersion();
		setRange(dvStart, dvEnd);
	}

	public DistributionRange getDistributionRange() {
		return distributionRange;
	}

	public StorageGroupGeneration getStorageGroupGeneration() {
		return generation;
	}

	public boolean isInRange(IKeyValue key) throws PEException {
		extractRange();
		return key.compare(rangeStart) >= 0 && key.compare(rangeEnd) <= 0;
	}
	
	/**
	 * @param start
	 * @param end
	 * @throws PEException
	 */
	public void setRange(KeyValue start, KeyValue end) throws PEException {
		rangeStart = new RangeLimit(start);
		rangeEnd = new RangeLimit(end);
		persistRange();
	}

	private void persistRange() {
		rangeStartPersisted = rangeStart.toString();
		rangeEndPersisted = rangeEnd.toString();
	}
	
	private void extractRange() throws PEException {
		if (rangeStart == null) {
			rangeStart = RangeLimit.parseLimit(rangeStartPersisted);
			rangeEnd = RangeLimit.parseLimit(rangeEndPersisted);
		}
	}

	public void mergeRange(KeyValue dvStart, KeyValue dvEnd) throws PEException {
		extractRange();
		if (dvStart.compare(rangeStart) < 0)
			rangeStart = new RangeLimit(dvStart);
		if (dvEnd.compare(rangeEnd) > 0)
			rangeEnd = new RangeLimit(dvEnd);
		persistRange();
	}
	
	@Override
	public String toString() {
		try {
			extractRange();
			return this.getClass().getSimpleName()+"("+distributionRange+",["+rangeStart+","+rangeEnd+"])";
		} catch (PEException e) {
			return this.getClass().getSimpleName();
		}
	}
}
