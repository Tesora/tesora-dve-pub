// OS_STATUS: public
package com.tesora.dve.sql.schema;

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
