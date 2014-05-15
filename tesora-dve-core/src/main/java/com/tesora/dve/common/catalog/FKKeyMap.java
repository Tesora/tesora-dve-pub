// OS_STATUS: public
package com.tesora.dve.common.catalog;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

@Entity 
@Table(name="table_fk")
public class FKKeyMap {
	@Id
	@GeneratedValue
	private int id;
	
	@ManyToOne
	@JoinColumn(name="fk_id",nullable=false)
	ForeignKey foreignKey;
	
	@ManyToOne
	@JoinColumn(name="source_table_id",nullable=false)
	UserColumn sourceTableColumn;
	
	@ManyToOne
	@JoinColumn(name="target_table_id",nullable=false)
	UserColumn targetTableColumn;


	public int getId() {
		return id;
	}
}