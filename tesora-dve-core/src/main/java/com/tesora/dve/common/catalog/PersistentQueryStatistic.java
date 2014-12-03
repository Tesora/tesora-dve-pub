package com.tesora.dve.common.catalog;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.hibernate.annotations.ForeignKey;


import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.ResultRow;

@Entity
@Table(name="qstat")
public class PersistentQueryStatistic implements CatalogEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue
	@Column(name = "stat_id")
	private int id;
	
	// for now we're only going to keep stats on queries on a single database,
	// no cross db queries
	@ForeignKey(name="fk_qstat_db")
	@ManyToOne(optional= false)
	@JoinColumn(name = "user_database_id")
	UserDatabase userDatabase;

	// the tables this query is on, as a comma separated list of names
	@Column(name = "subj_tab", nullable = false)
	@Lob
	String tables;
	
	@Column(name = "subj_query", nullable = false)
	@Lob
	String query;
	
	@Column(name = "avgcard", nullable = false)
	long card;
	
	@Column(name = "calls", nullable = false)
	long calls;
	
	public PersistentQueryStatistic() {
		// TODO Auto-generated constructor stub
	}

	public PersistentQueryStatistic(UserDatabase ofDb, String tabs, String query, long card, long calls) {
		this.userDatabase = ofDb;
		this.tables = tabs;
		this.query = query;
		this.card = card;
		this.calls = calls;
	}
	
	@Override
	public int getId() {
		return id;
	}

	@Override
	public ColumnSet getShowColumnSet(CatalogQueryOptions cqo)
			throws PEException {
		return null;
	}

	@Override
	public ResultRow getShowResultRow(CatalogQueryOptions cqo)
			throws PEException {
		return null;
	}

	@Override
	public void removeFromParent() throws Throwable {
		// TODO Auto-generated method stub

	}

	@Override
	public List<? extends CatalogEntity> getDependentEntities(CatalogDAO c)
			throws Throwable {
		return Collections.EMPTY_LIST;
	}

	@Override
	public void onUpdate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void onDrop() {
		// TODO Auto-generated method stub

	}

	public UserDatabase getUserDatabase() {
		return userDatabase;
	}
	
	public String getTables() {
		return tables;
	}
	
	// this is the stylized query
	public String getQuery() {
		return query;
	}
	
	public long getAvgCard() {
		return card;
	}
	
	public long getCalls() {
		return calls;
	}
	
	public void updateMeasurement(long card, long calls) {
		this.card = card;
		this.calls = calls;
	}
	
	private static final long oneMillionDollars = 1000000;
	
	public double getWeight() {
		return calls * 1.0 * (card == 0 ? oneMillionDollars : card);
	}
}
