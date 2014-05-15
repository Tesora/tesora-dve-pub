// OS_STATUS: public
package com.tesora.dve.common;

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
