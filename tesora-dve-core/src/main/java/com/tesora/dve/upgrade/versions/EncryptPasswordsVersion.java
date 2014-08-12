package com.tesora.dve.upgrade.versions;

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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.InformationCallback;
import com.tesora.dve.common.PECryptoUtils;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.util.Pair;

public class EncryptPasswordsVersion extends ComplexCatalogVersion {

	private static String[] obsoleteVariables = new String[] {
		"aws_access_key",
		"aws_ami_name",
		"aws_connectivity",
		"aws_key_pair",
		"aws_poll_interval",
		"aws_secret_key",
		"aws_security_group",
		"aws_supported"
	};

	
	public EncryptPasswordsVersion(int v) {
		super(v, false);
	}

	@Override
	public void upgrade(DBHelper helper, InformationCallback stdout) throws PEException {
		Pair<Long, Long> bounds = getSimpleBounds(helper, "user", "id");
		for (long id = bounds.getFirst(); id <= bounds.getSecond(); id++) {
			encryptPassword(helper, id);
		}
		
        execQuery(helper, "alter table user change column `plaintext` `password` varchar(255)");
		
        dropVariables(helper, Arrays.asList(obsoleteVariables));	
	}

	private void encryptPassword(DBHelper helper, long id) throws PEException {
		String def = null;
		try {
			ResultSet rs = null;
			try {
				helper.executeQuery("select plaintext from user where id = " + id);
				rs = helper.getResultSet();
				if (rs.next()) {
					def = rs.getString(1);
				}
			} finally {
				rs.close();
			}
		} catch (SQLException sqle) {
			throw new PEException("Unable to get existing password for id " + id, sqle);
		}

		if (def == null)
			return;

		try {
			List<Object> params = new ArrayList<Object>();
			params.add(PECryptoUtils.encrypt(def));
			params.add(id);

			helper.prepare("update user set plaintext = ? where id = ?");
			helper.executePrepared(params);
		} catch (SQLException sqle) {
			throw new PEException("Unable to update password for id " + id);
		}
	}

}
