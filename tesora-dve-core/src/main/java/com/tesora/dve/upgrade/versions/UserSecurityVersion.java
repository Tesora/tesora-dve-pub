// OS_STATUS: public
package com.tesora.dve.upgrade.versions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PECryptoUtils;
import com.tesora.dve.common.PEXmlUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.siteprovider.onpremise.jaxb.OnPremiseSiteProviderConfig;
import com.tesora.dve.siteprovider.onpremise.jaxb.PoolConfig;
import com.tesora.dve.siteprovider.onpremise.jaxb.PoolConfig.Site;
import com.tesora.dve.sql.util.Pair;

public class UserSecurityVersion extends ComplexCatalogVersion {

	private static final String[] before = new String[] {
		"alter table site_instance add column `user` varchar(255) not null after `status`, add column `password` varchar(255) not null after `name`",
		"alter table project add column `root_user_id` integer",
		"alter table project add index FKED904B1975A123A8 (root_user_id), add constraint FKED904B1975A123A8 foreign key (root_user_id) references user (id)",
		"alter table project drop column `group_tag`",
		"alter table user add column `grantPriv` bit(1) after `admin_user`",
	};
	
	private static final String[] after = new String[] {
	};

	public UserSecurityVersion(int v) {
		super(v, true);
	}

	@Override
	public void upgrade(DBHelper helper) throws PEException {
		execQuery(helper, before);

		String user = helper.getUserName();
		String password = PECryptoUtils.encrypt(helper.getPassword());

		Pair<Long, Long> bounds = getSimpleBounds(helper, "site_instance", "id");
		for (long id = bounds.getFirst(); id <= bounds.getSecond(); id++) {
			addUserAndPasswordSiteInstance(helper, user, password, id);
		}
		
		setRootUser(helper, user);

		bounds = getSimpleBounds(helper, "provider", "id");
		for (long id = bounds.getFirst(); id <= bounds.getSecond(); id++) {
			addUserAndPasswordProvider(helper, user, password, id);
		}

		execQuery(helper,after);
	}
	
	private void setRootUser(DBHelper helper, String user) throws PEException {
		Integer def = null;
		try {
			ResultSet rs = null;
			try {
				helper.executeQuery("select id from user where name = '" + user + "'");
				rs = helper.getResultSet();
				if (rs.next()) {
					def = rs.getInt(1);
				}
			} finally {
				rs.close();
			}
		} catch (Throwable sqle) {
			throw new PEException("Unable to get root user id for user " + user, sqle);
		}

		if (def == null)
			return;
		
		try {
			List<Object> params = new ArrayList<Object>();
			params.add(def);

			helper.prepare("update project set root_user_id = ?");
			helper.executePrepared(params);
		} catch (SQLException sqle) {
			throw new PEException("Unable to set root user id for user " + user);
		}

		try {
			List<Object> params = new ArrayList<Object>();
			params.add(def);

			helper.prepare("update user set grantPriv=1 where id = ?");
			helper.executePrepared(params);
		} catch (SQLException sqle) {
			throw new PEException("Unable to set grantPriv for root user " + user);
		}
	}

	private void addUserAndPasswordSiteInstance(DBHelper helper, String user, String password, long id) throws PEException {
		try {
			List<Object> params = new ArrayList<Object>();
			params.add(user);
			params.add(password);
			params.add(id);

			helper.prepare("update site_instance set user = ?, password = ? where id = ?");
			helper.executePrepared(params);
		} catch (SQLException sqle) {
			throw new PEException("Unable to add user and password for site_instance id " + id);
		}
	}
	
	private void addUserAndPasswordProvider(DBHelper helper, String user, String password, long id) throws PEException {
		String config = null;
		try {
			ResultSet rs = null;
			try {
				helper.executeQuery("select config from provider where id = " + id + " and plugin = 'com.tesora.dve.siteprovider.onpremise.OnPremiseSiteProvider'");
				rs = helper.getResultSet();
				if (rs.next()) {
					config = rs.getString(1);
				}
			} finally {
				rs.close();
			}
		} catch (SQLException sqle) {
			throw new PEException("Unable to get existing config for provider id " + id, sqle);
		}

		if (config == null)
			return;

		// Unmarshall the configuration
		OnPremiseSiteProviderConfig jaxbConfig = PEXmlUtils.unmarshalJAXB(config, OnPremiseSiteProviderConfig.class);
		
		if(jaxbConfig.getPool() != null) {
			for(PoolConfig pool : jaxbConfig.getPool()) {
				if(pool.getSite() != null) {
					for(Site site : pool.getSite()) {
						site.setUser(user);
						site.setPassword(password);
					}
				}
			}
		}
		
		// Marshal with nice new user / passwords in there
		config = PEXmlUtils.marshalJAXB(jaxbConfig);

		try {
			List<Object> params = new ArrayList<Object>();
			params.add(config);
			params.add(id);

			helper.prepare("update provider set config = ? where id = ?");
			helper.executePrepared(params);
		} catch (SQLException sqle) {
			throw new PEException("Unable to update config for provider id " + id);
		}
	}
}
