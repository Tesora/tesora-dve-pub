// OS_STATUS: public
package com.tesora.dve.common;

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

import static org.junit.Assert.*;

import java.util.List;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.exceptions.PEException;

public class PEFileUtilsTest {

	@Test
	public void propertiesConfigurationTest() throws PEException {

		PropertiesConfiguration pc = PEFileUtils.loadPropertiesConfigFromClasspath(getClass(), "fileutils.properties");
		List<Object> propList = pc.getList("propList");
		assertEquals(3, propList.size());
		
		assertEquals("singleValue", pc.getString("singleProp"));
	}

}
