// OS_STATUS: public
package com.tesora.dve.common.catalog;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import com.tesora.dve.distribution.KeyTemplate;

@Entity
@Table(name = "foreign_key")
public class ForeignKey implements Serializable {
	
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column(name = "fk_id")
	private int id;

	@Column(name = "name", nullable = false)
	private String name;
	
	@ManyToOne
	@JoinColumn(name="user_table_id",nullable=false)
	UserTable sourceTable;
	
	@OneToMany(mappedBy="foreignKey")
	List<FKKeyMap> columnMap = new ArrayList<FKKeyMap>();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getId() {
		return id;
	}

	public void setMapping(UserColumn sourceColumn, UserColumn targetColumn) {
		FKKeyMap fkMap = new FKKeyMap();
		fkMap.foreignKey = this;
		fkMap.sourceTableColumn = sourceColumn;
		fkMap.targetTableColumn = targetColumn;
		columnMap.add(fkMap);
	}
	
	public KeyTemplate getSourceKey() {
		KeyTemplate key = new KeyTemplate();
		for (FKKeyMap map : columnMap) {
			key.add(map.sourceTableColumn);
		}
		return key;
	}

	public KeyTemplate getTargetKey() {
		KeyTemplate key = new KeyTemplate();
		for (FKKeyMap map : columnMap) {
			key.add(map.targetTableColumn);
		}
		return key;
	}

}
