package com.tesora.dve.sql.schema;

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

import com.tesora.dve.sql.schema.DistributionVector.Model;

public class TempTableCreateOptions {

	private long rowcount = -1;
	private VectorRange range = null;
	private List<Integer> invisibleColumns = Collections.emptyList();
	private List<Integer> dvCols = Collections.emptyList();
	private final PEStorageGroup group;
	private final Model model;
	
	public TempTableCreateOptions(Model model, PEStorageGroup group) {
		this.group = group;
		this.model = model;
	}
	
	public TempTableCreateOptions withRowCount(long rc) {
		rowcount = rc;
		return this;
	}
	
	public TempTableCreateOptions withRange(VectorRange vr) {
		range = vr;
		return this;
	}
	
	public TempTableCreateOptions withInvisibleColumns(List<Integer> invisibleColumns) {
		this.invisibleColumns = invisibleColumns;
		return this;
	}
	
	public TempTableCreateOptions distributeOn(List<Integer> cols) {
		dvCols = cols;
		return this;
	}

	public long getRowcount() {
		return rowcount;
	}

	public VectorRange getRange() {
		return range;
	}

	public List<Integer> getInvisibleColumns() {
		return invisibleColumns;
	}

	public List<Integer> getDistVectColumns() {
		return dvCols;
	}

	public PEStorageGroup getGroup() {
		return group;
	}

	public Model getModel() {
		return model;
	}
	
}
