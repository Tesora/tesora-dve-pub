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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.persistence.EntityManager;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.ejb.HibernateEntityManager;
import org.hibernate.jdbc.Work;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.PersistProvider;
import com.tesora.dve.persist.PersistedInsert;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.statement.dml.DMLStatement;

public class DAOPersistProvider implements PersistProvider {

	Session session;
	
	public DAOPersistProvider(CatalogDAO dao) throws PEException {
		EntityManager em = dao.getEntityManager();
		HibernateEntityManager hem = (HibernateEntityManager) em;
		session = hem.getSession();
	}
	
	@Override
	public Long insert(PersistedInsert pi) throws PEException {
		String sql = pi.getSQL();
		InsertWork iw = new InsertWork(sql);
		try {
			session.doWork(iw);
			return iw.getId();
		} catch (HibernateException sqle) {
			throw new PEException("Unable to execute '" + sql + "'",sqle);
		}
	}

	@Override
	public List<ResultRow> query(DMLStatement dmls) throws PEException {
		return null;
	}
	
	private static class InsertWork implements Work {

		private String sql;
		private Long id;
		
		public InsertWork(String s) {
			sql = s;
			id = null;
		}
		
		public Long getId() {
			return id;
		}
		
		@Override
		public void execute(Connection connection) throws SQLException {
			Statement stmt = connection.createStatement();
			try {
				stmt.execute(sql, Statement.RETURN_GENERATED_KEYS);
				ResultSet rs = stmt.getGeneratedKeys();
				if (rs.next()) {
					id = rs.getLong(1);
				}
			} finally {
				stmt.close();
			}
		}
		
	}

}
