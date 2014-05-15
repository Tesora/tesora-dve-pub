// OS_STATUS: public
package com.tesora.dve.siteprovider.onpremise;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.tesora.dve.common.PECryptoUtils;
import com.tesora.dve.common.PEUrl;
import com.tesora.dve.common.PEXmlUtils;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.StorageSite;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.messaging.GetWorkerRequest;
import com.tesora.dve.siteprovider.AbstractSiteProvider;
import com.tesora.dve.siteprovider.CmdSiteManagerCommand;
import com.tesora.dve.siteprovider.ConfigSiteManagerCommand;
import com.tesora.dve.siteprovider.DynamicSiteClass;
import com.tesora.dve.siteprovider.DynamicSiteClassMap;
import com.tesora.dve.siteprovider.DynamicSiteInfo;
import com.tesora.dve.siteprovider.DynamicSiteStatus;
import com.tesora.dve.siteprovider.onpremise.jaxb.OnPremiseSiteProviderConfig;
import com.tesora.dve.siteprovider.onpremise.jaxb.PoolConfig;
import com.tesora.dve.variable.ScopedVariableHandler;
import com.tesora.dve.variable.VariableConfig;
import com.tesora.dve.variable.VariableValueConverter;
import com.tesora.dve.worker.SiteManagerCommand;

public class OnPremiseSiteProvider extends AbstractSiteProvider {

	public static final Logger log = Logger.getLogger(OnPremiseSiteProvider.class);

	public static final String DEFAULT_NAME = "OnPremise";
	public static final String DEFAULT_POLICY_NAME = DEFAULT_NAME + "Policy";

	private static final String OPTION_SITE = "site";
	private static final String OPTION_POOL = "pool";
	private static final String OPTION_URL = "url";
	private static final String OPTION_USER = "user";
	private static final String OPTION_PASSWORD = "password";
	private static final String OPTION_MAX_QUERIES = "maxQueries";
	private final static String OPTION_STATUS = "status";
	private final static String OPTION_ALTERNATE_POOL = "alternatePool";

	private DynamicSiteClassMap siteClassMap = null;
	private OnPremiseSiteProviderConfig jaxbConfig = null;

	public OnPremiseSiteProvider() throws PEException {
	}

	@Override
	public void initialize(SiteProviderContext ctxt, String name, boolean isEnabled, String config) throws PEException {
		super.initialize(name, isEnabled);

		if (config != null && config.length() > 0) {
			applyConfigurationChanges(unmarshallJAXB(config));
		} else {
			jaxbConfig = new OnPremiseSiteProviderConfig();
			siteClassMap = new DynamicSiteClassMap();
		}
	}

	@Override
	public void returnSitesByClass(SiteProviderContext ctxt, String siteCls, Collection<? extends StorageSite> sites)
			throws PEException {
		DynamicSiteClass dsc = siteClassMap.get(siteCls);

		if (dsc == null)
			throw new PEException("Provider '" + getProviderName() + "' requested pool '" + siteCls
					+ "' that doesn't exist");

		List<StorageSite> remaining = dsc.returnSites(sites, false);

		// If we have some remaining then maybe we used an alternate pool
		if (remaining.size() > 0) {

			String alternate = dsc.getAlternate();

			if (alternate != null) {
				// Try returning to the alternate pool
				dsc = siteClassMap.get(alternate);

				if (dsc == null)
					throw new PEException("Provider '" + getProviderName() + "' requested alternate pool '" + alternate
							+ "' that doesn't exist");

				remaining = dsc.returnSites(remaining, false);
			}
		}

		if (remaining.size() > 0)
			log.error("Provider '" + getProviderName() + "' failed to return " + remaining.size() + " sites the pool");
	}

	@Override
	public void provisionWorkerRequest(SiteProviderContext ctxt, GetWorkerRequest req) throws PEException {
		int count = req.getSiteCount();
		boolean strict = req.getIsStrict();

		Collection<? extends StorageSite> sites = allocateSites(req.getSiteClassName(), count, strict);

		// In strict mode we will get back zero sites if we can't
		// satisfy the request.
		// In non-strict mode we will get back what's available

		if (count > 0 && sites.size() == 0) {
			// If we have no sites - then try the alternate if available
			DynamicSiteClass dsc = siteClassMap.get(req.getSiteClassName());

			if (dsc.getAlternate() != null)
				sites = allocateSites(dsc.getAlternate(), count, strict);
		}

		if (count > 0 && sites.size() == 0)
			throw new PEException("Provider '" + getProviderName() + "' failed to obtain " + count
					+ " sites from pool '" + req.getSiteClassName() + "'");

		req.fulfillGetWorkerRequest(sites);
	}

	private Collection<? extends StorageSite> allocateSites(String className, int count, boolean strict)
			throws PEException {
		DynamicSiteClass dsc = siteClassMap.get(className);

		if (dsc == null)
			throw new PEException("Provider '" + getProviderName() + "' requested pool '" + className
					+ "' that doesn't exist");

		Collection<? extends StorageSite> sites = dsc.allocateSites(count);

		// In Strict mode we want exactly the requested number of sites.
		// Therefore if we don't get back the requested number return them
		if (strict && count > 0 && sites.size() != count) {
			dsc.returnSites(sites, true);
			sites.clear();
		}
		return sites;
	}

	@Override
	public void close() {
		if (siteClassMap != null)
			siteClassMap.clear();
	}

	// ------------------------------------------------------------------------

	@Override
	protected List<CatalogEntity> onShowSites(SiteManagerCommand smc) throws PEException {
		return siteClassMap.toCatalogEntity(getProviderName());
	}

	@Override
	public Collection<? extends StorageSite> getAllSites() {
		List<StorageSite> out = new ArrayList<StorageSite>();
		for (DynamicSiteClass dsc : siteClassMap.getAll())
			out.addAll(dsc.getSites());
		return out;
	}

	private String getStringOption(CmdSiteManagerCommand csmc, String option) throws PEException {
		return getStringOption(csmc, option, true);
	}

	private String getStringOption(CmdSiteManagerCommand csmc, String option, boolean required) throws PEException {
		String val = (String) findOption(csmc, option);
		if (required && val == null)
			throw new PEException(option + "='<value>' not specified");

		return val;
	}

	private long getLongOption(CmdSiteManagerCommand csmc, String option) throws PEException {
		Long val = (Long) findOption(csmc, option);
		if (val == null)
			throw new PEException(option + "='<value>' not specified");

		return val;
	}

	// ------------------------------------------------------------------------
	// ALTER DYNAMIC SITE PROVIDER <provider> command=<something>
	//
	@Override
	protected CmdSiteManagerCommand onPrepareCommand(SiteManagerCommand smc, String command) throws PEException {
		// alter dynamic site provider `name` cmd='<command>' <options>
		// available commands and options are:
		// cmd='set status' site='<name>' pool='<pool>' status='<ONLINE | OFFLINE>'
		// cmd='add site' site='<name>' pool='<pool>' url='<url>' user='<user>' password='<password>' maxQueries=<max>
		// cmd='drop site' site='<name>' pool='<pool>'
		// cmd='alter site' site='<name>' pool='<pool>' maxQueries=<max>

		CmdSiteManagerCommand csmc = new CmdSiteManagerCommand(smc, command);

		if ("add site".equalsIgnoreCase(command)) {
			// cmd='add site', site'<name>', pool='<pool>', url='<url>',
			// maxQueries=<max>

			String site = getStringOption(csmc, OPTION_SITE);
			String pool = getStringOption(csmc, OPTION_POOL);
			String url = getStringOption(csmc, OPTION_URL);
			String user = getStringOption(csmc, OPTION_USER);
			long maxQueries = getLongOption(csmc, OPTION_MAX_QUERIES);

			// Validate the URL
			PEUrl.fromUrlString(url);
			
			// Encrypt the password
			String encryptedPassword = PECryptoUtils.encrypt(getStringOption(csmc, OPTION_PASSWORD));

			// Clone existing configuration so that we can modify it
			// Simple clone is to marshall / re-marshall
			OnPremiseSiteProviderConfig copy = unmarshallJAXB(marshallJAXB(jaxbConfig));

			// Add the new site to the configuration
			PoolConfig poolCfg = findSitePool(copy, pool);
			if (poolCfg == null)
				throw new PEException("Pool '" + pool + "' does not exist in provider configuration");

			PoolConfig.Site newSite = findSite(poolCfg, site);
			if (newSite != null)
				throw new PEException("Site '" + site + "' in pool '" + pool
						+ "' already exists in provider configuration");

			// Add new site to the pool
			newSite = new PoolConfig.Site();
			newSite.setName(site);
			newSite.setUrl(url);
			newSite.setUser(user);
			newSite.setPassword(encryptedPassword);
			newSite.setMaxQueries((int) maxQueries);
			poolCfg.getSite().add(newSite);

			csmc.getTarget().setConfig(marshallJAXB(copy));

		} else if ("alter site".equalsIgnoreCase(command)) {
			// cmd='alter site', site='<name>', pool='<pool>', maxQueries=<max>

			String site = getStringOption(csmc, OPTION_SITE);
			String pool = getStringOption(csmc, OPTION_POOL);
			long maxQueries = getLongOption(csmc, OPTION_MAX_QUERIES);

			// Clone existing configuration so that we can modify it
			// Simple clone is to marshall / re-marshall
			OnPremiseSiteProviderConfig copy = unmarshallJAXB(marshallJAXB(jaxbConfig));

			// Find the pool
			PoolConfig poolCfg = findSitePool(copy, pool);
			if (poolCfg == null)
				throw new PEException("Pool '" + pool + "' does not exist in provider configuration");

			// Find the site in the pool
			PoolConfig.Site siteCfg = findSite(poolCfg, site);
			if (siteCfg == null)
				throw new PEException("Site '" + site + "' in pool '" + pool
						+ "' does not exist in provider configuration");

			siteCfg.setMaxQueries((int) maxQueries);

			csmc.getTarget().setConfig(marshallJAXB(copy));

		} else if ("drop site".equalsIgnoreCase(command)) {

			// cmd='drop site', site='<name>', pool='<pool>'

			String site = getStringOption(csmc, OPTION_SITE);
			String pool = getStringOption(csmc, OPTION_POOL);

			// Clone existing configuration so that we can modify it
			// Simple clone is to marshall / re-marshall
			OnPremiseSiteProviderConfig copy = unmarshallJAXB(marshallJAXB(jaxbConfig));

			// Find the pool
			PoolConfig poolCfg = findSitePool(copy, pool);
			if (poolCfg == null)
				throw new PEException("Pool '" + pool + "' does not exist in provider configuration");

			// Find the site in the pool
			PoolConfig.Site siteCfg = findSite(poolCfg, site);
			if (siteCfg == null)
				throw new PEException("Site '" + site + "' in pool '" + pool
						+ "' does not exist in provider configuration");

			// Remove the site from the pool
			poolCfg.getSite().remove(siteCfg);

			csmc.getTarget().setConfig(marshallJAXB(copy));

		} else if ("add pool".equalsIgnoreCase(command)) {

			// cmd='add pool', pool='<pool>', alternatePool='<pool>'

			String pool = getStringOption(csmc, OPTION_POOL);
			String alternate = getStringOption(csmc, OPTION_ALTERNATE_POOL, false);

			// Clone existing configuration so that we can modify it
			// Simple clone is to marshall / re-marshall
			OnPremiseSiteProviderConfig copy = unmarshallJAXB(marshallJAXB(jaxbConfig));

			// Check to see if the pool already exists
			if (findSitePool(copy, pool) != null)
				throw new PEException("Pool '" + pool + "' already exists in provider configuration");

			// Check that the specified alternative also exists
			if (alternate != null) {
				if (findSitePool(copy, alternate) == null)
					throw new PEException("Alternate pool '" + alternate + "' does not exist in provider configuration");
			}

			PoolConfig poolCfg = new PoolConfig();
			poolCfg.setName(pool);
			poolCfg.setAlternatePool(alternate);
			copy.getPool().add(poolCfg);

			csmc.getTarget().setConfig(marshallJAXB(copy));

		} else if ("alter pool".equalsIgnoreCase(command)) {

			// cmd='alter pool', pool='<pool>', alternatePool='<pool>'

			String pool = getStringOption(csmc, OPTION_POOL);
			String alternate = getStringOption(csmc, OPTION_ALTERNATE_POOL, false);

			// Clone existing configuration so that we can modify it
			// Simple clone is to marshall / re-marshall
			OnPremiseSiteProviderConfig copy = unmarshallJAXB(marshallJAXB(jaxbConfig));

			// Find the pool
			PoolConfig poolCfg = findSitePool(copy, pool);
			if (poolCfg == null)
				throw new PEException("Pool '" + pool + "' does not exist in provider configuration");

			// Also make sure the specified alternate is a valid pool
			if (alternate != null) {
				if (findSitePool(copy, alternate) == null)
					throw new PEException("Alternate pool '" + alternate + "' does not exist in provider configuration");
			}

			poolCfg.setAlternatePool(alternate);

			csmc.getTarget().setConfig(marshallJAXB(copy));

		} else if ("drop pool".equalsIgnoreCase(command)) {

			// cmd='drop pool', pool='<pool>'

			String pool = getStringOption(csmc, OPTION_POOL);

			// Clone existing configuration so that we can modify it
			// Simple clone is to marshall / re-marshall
			OnPremiseSiteProviderConfig copy = unmarshallJAXB(marshallJAXB(jaxbConfig));

			// Find the pool
			PoolConfig poolCfg = findSitePool(copy, pool);
			if (poolCfg == null)
				throw new PEException("Pool '" + pool + "' does not exist in provider configuration");

			copy.getPool().remove(poolCfg);

			csmc.getTarget().setConfig(marshallJAXB(copy));
		} else if ("set status".equalsIgnoreCase(command)) {
			// cmd='set status', site'<name>', pool='<pool>', status='<ONLINE |
			// OFFLINE>'

			String site = getStringOption(csmc, OPTION_SITE);
			String pool = getStringOption(csmc, OPTION_POOL);
			String status = getStringOption(csmc, OPTION_STATUS);

			DynamicSiteClass dsc = siteClassMap.get(pool);
			if (dsc == null)
				throw new PEException("Pool '" + pool + "' does not exist in provider configuration");

			// check that the specified site exists
			DynamicSiteInfo existingSiteInfo = dsc.getSiteByShortName(site);

			if (existingSiteInfo == null)
				throw new PEException("Site '" + site + "' in pool '" + pool
						+ "' does not exist in provider configuration");

			csmc.setSite(existingSiteInfo);

			// This will throw an exception if the value isn't valid
			DynamicSiteStatus dss = DynamicSiteStatus.fromString(status);
			if (!dss.isValidUserOption())
				throw new PEException("You are not allowed to set the status to '" + status + "'");

			csmc.setStatus(dss);
		} else
			throw new PEException("Unable to parse ALTER command");

		return csmc;
	}

	@Override
	protected int onUpdateCommand(CmdSiteManagerCommand csmc) throws PEException {
		// Bit of a temporary hack to special case the enable site command
		// Eventually we should probably be persisting the site state into the
		// configuration and storing it in the catalog
		if (csmc.getCommand().equalsIgnoreCase("set status")) {
			csmc.getSite().setState(csmc.getStatus(), true);
			return 1;
		}
		return applyConfigurationChanges(unmarshallJAXB(csmc.getTarget().getConfig()));
	}

	@Override
	protected void onRollbackCommand(CmdSiteManagerCommand asmc) throws PEException {
		// TODO
	}

	// ------------------------------------------------------------------------
	// ALTER DYNAMIC SITE PROVIDER <provider> config=<something>
	//
	@Override
	protected ConfigSiteManagerCommand onPrepareConfig(SiteManagerCommand smc, String enabled, String config)
			throws PEException {

		ConfigSiteManagerCommand csmc = new ConfigSiteManagerCommand(smc, enabled, config);

		if (config != null) {
			// This is just a test to check for valid syntax
			unmarshallJAXB(config);

			csmc.getTarget().setConfig(config);
		}

		if (enabled != null) {
			csmc.getTarget().setIsEnabled(VariableValueConverter.toInternalBoolean(enabled));
		}
		return csmc;
	}

	@Override
	protected int onUpdateConfig(ConfigSiteManagerCommand csmc) throws PEException {
		int ret = 0;

		if (csmc.getConfig() != null) {
			ret = applyConfigurationChanges(unmarshallJAXB(csmc.getTarget().getConfig()));
		}

		if (csmc.getEnabled() != null) {
			setEnabled(csmc.getTarget().isEnabled());
			ret = 1;
		}
		return ret;
	}

	@Override
	protected void onRollbackConfig(ConfigSiteManagerCommand csmc) throws PEException {
		// TODO
	}

	// ------------------------------------------------------------------------

	private int applyConfigurationChanges(OnPremiseSiteProviderConfig config) throws PEException {

		jaxbConfig = config;

		// Convert jaxb config into a DynamicSiteClassMap so that we can compare
		// it to our existing configuration
		DynamicSiteClassMap newMap = configToSiteClassMap(config);

		if (siteClassMap == null) {
			// We don't already have a siteClassMap
			siteClassMap = newMap;
		} else {
			// For each site class in the newMap
			for (DynamicSiteClass newDsc : newMap.getAll()) {
				DynamicSiteClass existingDsc = siteClassMap.get(newDsc.getName());

				if (existingDsc == null)
					siteClassMap.put(newDsc);
				else
					applyConfigurationChanges(newDsc, existingDsc);
			}

			// Find any site classes that have been removed
			for (DynamicSiteClass existingDsc : siteClassMap.getAll()) {
				if (newMap.get(existingDsc.getName()) == null) {
					// Site class is no longer in the configuration so
					// mark all sites in that class as ready for removal
					existingDsc.markForRemoval();
				}
			}
		}

		// Iterate over the classmap cleaning up any sites that are no longer
		// needed
		for (Iterator<DynamicSiteClass> iter = siteClassMap.getAll().iterator(); iter.hasNext();) {
			DynamicSiteClass dsc = iter.next();

			dsc.cleanupAndRemove();

			// After cleaning up the site class - check to see if we can remove
			// the siteclass altogether
			if (dsc.getSites().size() == 0)
				iter.remove();
		}
		return 1;
	}

	private void applyConfigurationChanges(DynamicSiteClass newDsc, DynamicSiteClass existingDsc) {

		existingDsc.setAlternate(newDsc.getAlternate());

		for (DynamicSiteInfo newSite : newDsc.getSites()) {
			DynamicSiteInfo existingSite = existingDsc.getSite(newSite.getFullName());

			if (existingSite != null) {
				// Already exists. Right now we only let you change the
				// maxQueries
				if (existingSite.getMaxQueries() != newSite.getMaxQueries()) {
					existingSite.setMaxQueries(newSite.getMaxQueries());
				}
			} else {
				// Doesn't exist - so add it
				existingDsc.addSite(newSite);
			}
		}

		// now lets see if there are any sites that need to be deleted
		for (DynamicSiteInfo existingSite : existingDsc.getSites()) {
			// if we have an existing site that isn't in the new list
			// then we need to terminate it. Set its state and it will get
			// removed at the appropriate time when there are no longer any
			// open queries
			if (newDsc.getSite(existingSite.getFullName()) == null)
				existingSite.markForRemoval();
		}
	}

	private DynamicSiteClassMap configToSiteClassMap(OnPremiseSiteProviderConfig slc) throws PEException {
		DynamicSiteClassMap map = new DynamicSiteClassMap();

		// Sites configured on specific urls
		if (slc.getPool() != null) {
			for (PoolConfig pool : slc.getPool()) {

				DynamicSiteClass dsc = new DynamicSiteClass(pool.getName(), pool.getAlternatePool());

				for (PoolConfig.Site site : pool.getSite()) {
					// This is where we decrypt the password 
					String decryptedPassword = PECryptoUtils.decrypt(site.getPassword());
					
					dsc.addSite(new DynamicSiteInfo(getProviderName(), dsc.getName(), site.getName(), site.getUrl(),
							site.getUser(), decryptedPassword, site.getMaxQueries()));
				}
				map.put(dsc);
			}
		}
		return map;
	}

	private PoolConfig findSitePool(OnPremiseSiteProviderConfig slc, String name) {
		for (PoolConfig pool : slc.getPool()) {
			if (pool.getName().equalsIgnoreCase(name))
				return pool;
		}
		return null;
	}

	private PoolConfig.Site findSite(PoolConfig pool, String name) {
		for (PoolConfig.Site site : pool.getSite()) {
			if (site.getName().equalsIgnoreCase(name))
				return site;
		}
		return null;
	}

	// ------------------------------------------------------------------------

	private OnPremiseSiteProviderConfig unmarshallJAXB(String str) throws PEException {
		return PEXmlUtils.unmarshalJAXB(str, OnPremiseSiteProviderConfig.class);
	}

	private String marshallJAXB(OnPremiseSiteProviderConfig slc) throws PEException {
		return PEXmlUtils.marshalJAXB(slc);
	}

	@Override
	public VariableConfig<ScopedVariableHandler> getVariableConfiguration() {
		return new VariableConfig<ScopedVariableHandler>();
	}
}
