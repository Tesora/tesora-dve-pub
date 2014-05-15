// OS_STATUS: public
package com.tesora.dve.variable;

import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.CatalogQueryOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

@Entity
@Table(name="config")
public class GlobalConfig implements CatalogEntity {

	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column( name="config_id" )
	int id;
	
	String name;
	String value;
	
	public GlobalConfig() {
		super();
	}

	public GlobalConfig(String name, String value) {
		super();
		this.name = name;
		this.value = value;
	}

	@Override
	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo)
			throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo)
			throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeFromParent() throws Throwable {
		// TODO Auto-generated method stub

	}

	@Override
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c)
			throws Throwable {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void onUpdate() {
	}

	@Override
	public void onDrop() {
	}

}
