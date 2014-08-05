package com.tesora.dve.tools;

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


import java.io.File;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.ObjectName;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.PECryptoUtils;
import com.tesora.dve.common.PEFileUtils;
import com.tesora.dve.common.PEUrl;
import com.tesora.dve.common.PEXmlUtils;
import com.tesora.dve.common.SiteInstanceStatus;
import com.tesora.dve.common.catalog.DynamicPolicy;
import com.tesora.dve.common.catalog.ExternalService;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.PersistentSite;
import com.tesora.dve.common.catalog.Provider;
import com.tesora.dve.common.catalog.SiteInstance;
import com.tesora.dve.common.catalog.VariableConfig;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.groupmanager.HazelcastCoordinationServices;
import com.tesora.dve.groupmanager.LocalhostCoordinationServices;
import com.tesora.dve.siteprovider.onpremise.jaxb.OnPremiseSiteProviderConfig;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.schema.VariableScopeKind;
import com.tesora.dve.sql.template.jaxb.Template;
import com.tesora.dve.sql.transexec.CatalogHelper;
import com.tesora.dve.standalone.Main;
import com.tesora.dve.tools.field.FieldAction;
import com.tesora.dve.tools.field.FieldActionManager;
import com.tesora.dve.tools.jaxb.persistent.PersistentConfig;
import com.tesora.dve.tools.jaxb.persistent.PersistentGroupCfgList;
import com.tesora.dve.tools.jaxb.persistent.PersistentSiteCfgList;
import com.tesora.dve.tools.jaxb.policy.PolicyConfig;
import com.tesora.dve.upgrade.CatalogSchemaVersion;
import com.tesora.dve.upgrade.CatalogVersions;
import com.tesora.dve.variable.VariableConstants;

public class DVEConfigCLI extends CLIBuilder {

	private static final String PERSISTENT_SCHEMA = "persistent.xsd";
	private static final String POLICY_SCHEMA = "policy.xsd";
	private static final String ONPREMISE_SITE_PROVIDER_SCHEMA = "onPremiseSiteProvider.xsd";
	private static final String TEMPLATE_SCHEMA = "template.xsd";

	private static final String DEFAULT_JMX_HOST = "127.0.0.1";
	private static final Integer DEFAULT_JMX_PORT = 1234;

	private static final FilenameFilter TEMPLATE_FILE_NAME_FILTER = new FilenameFilter() {
		@Override
		public boolean accept(File dir, String name) {
			return name.toLowerCase().endsWith(DVEAnalyzerCLI.TEMPLATE_FILE_EXTENSION);
		}
	};

	private CatalogHelper catHelper;
	private DBHelper dbHelper;

	public DVEConfigCLI(String[] args) throws PEException {
		super(args, "Configuration Tool");

		registerCommand(new CommandType(new String[] { "locate", "catalog" }, "<url> <user> <password> [<database>]",
				"Specify the location of the DVE catalog and the user / password to use when connecting."));
		registerCommand(new CommandType(new String[] { "encrypt" }, "<text>", "Internal - Encrypt the specified text.",
				true));
		registerCommand(new CommandType(new String[] { "setup", "catalog" },
				"<persistent file> <dynamic file> <policy file>",
				"Create an initial catalog required to run the server using the specified files for configuration."));
		registerCommand(new CommandType(new String[] { "setup", "localhost", "catalog" },
				"[<num persistent site>] [<num dynamic sites>]",
				"Create an initial catalog required to run the server with the specified number of sites on localhost."));
		registerCommand(new CommandType(new String[] { "delete", "catalog" }, "Delete the DVE catalog."));
		registerCommand(new CommandType(new String[] { "upgrade", "catalog" }, "Upgrade the catalog."));
		registerCommand(new CommandType(new String[] { "check", "catalog", "version" }, "Check the catalog version."));
		registerCommand(new CommandType(new String[] { "truncate", "servers" },
				"Remove all registered servers from the catalog."));

		registerCommand(new CommandType(new String[] { "set", "root", "user" }, "<user> <password>",
				"Set the username and password for the root user"));

		registerCommand(new CommandType(new String[] { "set", "default", "pg" }, "<name>",
				"Set the default persistent group."));
		registerCommand(new CommandType(new String[] { "show", "pg" }, "<name>",
				"Show the contents of the specified persistent group."));

		registerCommand(new CommandType(new String[] { "add", "provider" }, "<name> <plugin> [<filename>]",
				"Add a new provider with the specified plugin and configuration."));
		registerCommand(new CommandType(new String[] { "configure", "provider" }, "<name> <filename>",
				"Configure the specified dynamic site provider using the contents of the file."));
		registerCommand(new CommandType(new String[] { "show", "provider" }, "<name>",
				"Show the configuration for the specified dynamic site provider."));
		registerCommand(new CommandType(new String[] { "remove", "provider" }, "<name>",
				"Remove the specified dynamic site provider."));

		registerCommand(new CommandType(new String[] { "add", "policy" }, "<filename>",
				"Add a new dynamic policy with the specifed configuration."));
		registerCommand(new CommandType(new String[] { "configure", "policy" }, "<filename>",
				"Configure the specified dynamic policy using the contents of the file."));
		registerCommand(new CommandType(new String[] { "show", "policy" }, "<name>",
				"Show the configuration for the specified dynamic policy."));
		registerCommand(new CommandType(new String[] { "set", "default", "policy" }, "<name>", "Set the default policy"));

		registerCommand(new CommandType(new String[] { "set", "groupService" }, "<Hazelcast | Localhost>",
				"Set the group service."));

		registerCommand(new CommandType(new String[] { "add", "es" },
				"<name> <plugin> <use data store> [<connect user>] [<filename>]",
				"Add a new external service with the specified plugin and configuration."));
		registerCommand(new CommandType(new String[] { "configure", "es" }, "<name> <filename>",
				"Configure the specified external service using the contents of the file."));
		registerCommand(new CommandType(new String[] { "show", "es" }, "<name>",
				"Show the configuration for the specified external service."));
		registerCommand(new CommandType(new String[] { "remove", "es" }, "<name>",
				"Remove the specified external service."));

		registerCommand(new CommandType(new String[] { "dump" }, "[verbose]", "Display the contents of the catalog."));

		registerCommand(new CommandType(new String[] { "action", "list" }, "List all available action modules.", true));
		registerCommand(new CommandType(new String[] { "action", "report" }, "<name>",
				"Run the specified action report.", true));
		registerCommand(new CommandType(new String[] { "action", "run" }, "<name>", "Run the specified action process.",
				true));

		// The following commands require an active connection to the DVE server.
		registerCommand(new CommandType(new String[] { "connect" }, "[<url> <user> <password>]",
				"Connect to a DVE server (default=" + PEConstants.MYSQL_URL + " " + PEConstants.ROOT + " "
						+ PEConstants.PASSWORD + ")"));
		registerCommand(new CommandType(new String[] { "disconnect" }, "Disconnect from a DVE server."));

		registerCommand(new CommandType(new String[] { "add", "template" }, "<template file> [<match>]",
				"Add a new template to the server using the specified file."));
		registerCommand(new CommandType(new String[] { "add", "templates" }, "<templates folder>",
				"Add a new set of templates to the server from the specified folder."));
		registerCommand(new CommandType(new String[] { "update", "template" }, "<template file> [<match>]",
				"Alter an existing template using the specified file."));
		registerCommand(new CommandType(new String[] { "update", "templates" }, "<template folder>",
				"Alter a set of existing template from the specified folder."));

		registerJMXCommands();

		try {
			catHelper = new CatalogHelper(Main.class);
		} catch (final Exception e) {
			throw new PEException("Failed to create catalog helper - " + e.getMessage(), e);
		}
	}

	protected CatalogHelper getCatalogHelper() {
		return catHelper;
	}

	@Override
	public void close() {
		if (catHelper != null) {
			catHelper.close();
			catHelper = null;
		}
		super.close();
	}

	public void cmd_dump(Scanner scanner) throws PEException {

		final boolean verbose = StringUtils.equalsIgnoreCase("VERBOSE", scan(scanner));

		printlnDots("Generating " + (verbose ? "verbose " : "") + "dump of catalog");

		try (final StringWriter sw = new StringWriter(); final PrintWriter pw = new PrintWriter(sw)) {
			catHelper.dumpCatalogInfo(pw, verbose);
			println(sw.toString());
		} catch (final PEException e) {
			throw e;
		} catch (final Exception e) {
			throw new PEException("Failed to generate dump - " + e.getMessage(), e);
		}
	}

	public void cmd_locate_catalog(Scanner scanner) throws PEException {
		if (scanner.hasNext()) {
			final String url = scan(scanner, "url");
			final String user = scan(scanner, "user");
			final String password = scan(scanner, "password");
			final String database = scan(scanner);

			PEUrl.isValidUrlWithPort(url);

			final DBHelper dbHelper = new DBHelper(url, user, password);
			try {
				dbHelper.connect();
			} catch (final Exception e) {
				throw new PEException("Failed to connect to DVE catalog database at '" + url + "' - "
						+ e.getMessage(), e);
			} finally {
				dbHelper.disconnect();
			}

			try {
				final Properties serverProps = PEFileUtils.loadPropertiesFromClasspath(Main.class,
						PEConstants.SERVER_FILE_NAME);

				serverProps.setProperty(DBHelper.CONN_URL, url);
				serverProps.setProperty(DBHelper.CONN_USER, user);
				serverProps.setProperty(DBHelper.CONN_PASSWORD, password);
				serverProps.setProperty(DBHelper.CONN_DBNAME, (database != null) ? database : PEConstants.CATALOG);

				PEFileUtils.savePropertiesToClasspath(Main.class, PEConstants.SERVER_FILE_NAME, serverProps);

				printlnDots("Updating properties file at: "
						+ PEFileUtils.convert(PEFileUtils.getClasspathURL(Main.class, PEConstants.SERVER_FILE_NAME)));

			} catch (final Exception e) {
				throw new PEException("Failed to update '" + PEConstants.SERVER_FILE_NAME + "' - " + e.getMessage(), e);
			}

			try {
				catHelper.close();
				catHelper = new CatalogHelper(Main.class);
			} catch (final Exception e) {
				throw new PEException("Failed to re-create catalog helper - " + e.getMessage(), e);
			}
		}

		final Properties props = catHelper.getProperties();

		printlnDots("DVE catalog location settings are:");
		printlnIndent("URL      = " + props.getProperty(DBHelper.CONN_URL, ""));
		printlnIndent("User     = " + props.getProperty(DBHelper.CONN_USER, ""));
		printlnIndent("Password = " + props.getProperty(DBHelper.CONN_PASSWORD, ""));
		printlnIndent("Database = " + props.getProperty(DBHelper.CONN_DBNAME, ""));
	}

	public void cmd_encrypt(Scanner scanner) throws PEException {
		final String value = scan(scanner, "text");
		printlnDots("encrypt '" + value + "' as '" + PECryptoUtils.encrypt(value));
	}

	public void cmd_delete_catalog() throws PEException {
		final Properties props = catHelper.getProperties();
		final String catalogLocation = props.getProperty(DBHelper.CONN_URL, "");

		if (StringUtils.isBlank(catalogLocation)) {
			throw new PEException("Catalog location not set. Use the locate command to specify catalog location first.");
		}

		println("You are about to completely remove the DVE catalog located at " + catalogLocation);
		askQuestion("Do you want to continue [y/N]? ", new QuestionCallback() {
			@Override
			public void answer(String line) throws PEException {
				if ((line != null) && (line.equalsIgnoreCase("y") || line.equalsIgnoreCase("yes"))) {
					catHelper.deleteCatalog();
					printlnDots("DVE catalog deleted from '" + catalogLocation + "'");
				}
			}
		});
	}

	public void cmd_set_root_user(Scanner scanner) throws PEException {
		final String user = scan(scanner, "user");
		final String password = scan(scanner, "password");

		catHelper.setRootCredentials(user, password);
	}

	public void cmd_setup_localhost_catalog(Scanner scanner) throws PEException {
		Integer persistent = scanInteger(scanner);
		Integer dynamic = scanInteger(scanner);

		if (persistent == null) {
			persistent = 1;
		}

		if (dynamic == null) {
			dynamic = 1;
		}

		printlnDots("Generating localhost catalog with " + persistent + " persistent site(s) and " + dynamic
				+ " dynamic site(s)");

		catHelper.createLocalhostCatalog("OnPremise", PEConstants.DEFAULT_GROUP_NAME, persistent, dynamic);

		printlnDots("DVE catalog created at '" + catHelper.getCatalogDBUrl() + "'");
	}

	public void cmd_setup_catalog(Scanner scanner) throws PEException {
		final File persistentFile = scanFile(scanner, "persistent sites");
		final File dynamicFile = scanFile(scanner, "dynamic sites");
		final File policyFile = scanFile(scanner, "policy");

		final String persistentContent = PEFileUtils.readToString(persistentFile);
		final String dynamicContent = PEFileUtils.readToString(dynamicFile);
		final String policyContent = PEFileUtils.readToString(policyFile);

		// Unmarshall XML for persistent site configuration.
		final PersistentConfig persistentConfig = PEXmlUtils.unmarshalJAXB(persistentContent,
				PersistentConfig.class, PEFileUtils.getClasspathURL(PersistentConfig.class, PERSISTENT_SCHEMA));

		// Create in memory representations of sites and groups.
		final List<PersistentSite> sites = createPersistentSites(persistentConfig);
		final List<PersistentGroup> groups = createPersistentGroups(persistentConfig, sites);

		// Determine default Persistent Group to set.
		PersistentGroup defaultGroup = null;
		if ((persistentConfig.getPersistentGroups() != null)
				&& (persistentConfig.getPersistentGroups().getDefaultGroup() != null)) {
			// Make sure it exists in the group list we created above.
			for (final PersistentGroup group : groups) {
				if (persistentConfig.getPersistentGroups().getDefaultGroup().equals(group.getName())) {
					defaultGroup = group;
					break;
				}
			}
			if (defaultGroup == null) {
				throw new PEException("Persistent group specified a default group that doesn't exist");
			}
		} else {
			defaultGroup = groups.get(0);
		}

		// Unmarshall XML for policy configuration.
		final DynamicPolicy policy = parsePolicyConfig(PEXmlUtils.unmarshalJAXB(policyContent, PolicyConfig.class,
				PEFileUtils.getClasspathURL(PolicyConfig.class, POLICY_SCHEMA)));

		OnPremiseSiteProviderConfig dynamic = PEXmlUtils.unmarshalJAXB(dynamicContent, OnPremiseSiteProviderConfig.class,
				PEFileUtils.getClasspathURL(OnPremiseSiteProviderConfig.class, ONPREMISE_SITE_PROVIDER_SCHEMA));

		// Encrypt any passwords.
		dynamic = CatalogHelper.encryptProviderPasswords(dynamic);

		printlnDots("Generating DVE catalog");

		catHelper.createStandardCatalog(sites, groups, defaultGroup, dynamic, policy);

		printlnDots("DVE catalog created at '" + catHelper.getCatalogDBUrl() + "'");
	}

	private List<PersistentSite> createPersistentSites(PersistentConfig config) throws PEException {
		final List<PersistentSite> sites = new ArrayList<PersistentSite>();
		if ((config.getPersistentSites() == null) || (config.getPersistentSites().getSite() == null)
				|| config.getPersistentSites().getSite().isEmpty()) {
			throw new PEException("Persistent site list needs to contain at least one persistent site");
		}

		for (final PersistentSiteCfgList.Site siteCfg : config.getPersistentSites().getSite()) {

			if (!PersistentSite.isValidHAType(siteCfg.getHaMode())) {
				throw new PEException("Persistent site contains an invalid haMode '" + siteCfg.getHaMode() + "'");
			}

			final PersistentSite site = new PersistentSite(siteCfg.getName(), siteCfg.getHaMode());

			// Create all of the site instances.
			SiteInstance master = null;
			for (final PersistentSiteCfgList.Site.SiteInstance inst : siteCfg.getSiteInstance()) {
				// Initially we will create all site as non-masters.
				final SiteInstance i = new SiteInstance(inst.getName(), inst.getUrl(), inst.getUser(), inst.getPassword(),
						false, inst.isEnabled() ? SiteInstanceStatus.ONLINE.name() : SiteInstanceStatus.OFFLINE.name());
				site.addInstance(i);

				// If the configuration specifies a master then remember it.
				if ((inst.isMaster() != null) && inst.isMaster()) {
					if (!inst.isEnabled()) {
						throw new PEException("Can't specify a disabled site instance as master for site '"
								+ site.getName() + "'");
					}

					// Unless we already have a master specified.
					if (master != null) {
						throw new PEException("Multiple instances specified as master for site '" + site.getName()
								+ "'");
					}
					master = i;
				}
			}

			// If we don't have a master defined then simply use the first
			// available.
			if (master == null) {
				for (final SiteInstance inst : site.getSiteInstances()) {
					if (inst.isEnabled() && inst.isMaster()) {
						master = inst;
						break;
					}
				}
			}

			// If master is still null then we don't have a viable master.
			if (master == null) {
				throw new PEException("No master site instance available for site '" + site.getName() + "'");
			}

			site.setMasterInstance(master);

			if (siteCfg.getHaMode().equalsIgnoreCase("single")) {
				if (site.getSiteInstances().size() != 1) {
					throw new PEException("Persistent site of haMode 'Single' should have exactly one site instance");
				}
			} else if (siteCfg.getHaMode().equalsIgnoreCase("mastermaster")) {
				if (site.getSiteInstances().size() != 2) {
					throw new PEException(
							"Persistent site of haMode 'MasterMaster' should have exactly two site instances");
				}
			}
			sites.add(site);
		}

		return sites;
	}

	private List<PersistentGroup> createPersistentGroups(PersistentConfig config, List<PersistentSite> sites)
			throws PEException {
		final List<PersistentGroup> groups = new ArrayList<PersistentGroup>();

		if ((config.getPersistentGroups() == null) || (config.getPersistentGroups().getGroup() == null)
				|| config.getPersistentGroups().getGroup().isEmpty()) {
			// We don't have a groups declaration therefore we are going to
			// create a default group containing all of the sites we know about.

			final PersistentGroup group = new PersistentGroup(PEConstants.DEFAULT_GROUP_NAME);
			group.addAllSites(sites);
			groups.add(group);
		} else {
			for (final PersistentGroupCfgList.Group groupCfg : config.getPersistentGroups().getGroup()) {
				final PersistentGroup group = new PersistentGroup(groupCfg.getName());

				if (groupCfg.getSite().isEmpty()) {
					throw new PEException("Persistent group contains a group with no sites");
				}

				for (final String siteName : groupCfg.getSite()) {
					// Iterate over available PersistentSites looking for the
					// one we want to add to the group.
					boolean found = false;
					for (final PersistentSite site : sites) {
						if (siteName.equals(site.getName())) {
							group.addStorageSite(site);
							found = true;
							break;
						}
					}
					if (!found) {
						throw new PEException("Persistent group contains site '" + siteName + "' that doesn't exist");
					}
				}
				groups.add(group);
			}
		}
		return groups;
	}

	public void cmd_set_default_pg(Scanner scanner) throws PEException {
		final String name = scan(scanner, "name");

		catHelper.setDefaultStorageGroup(name);

		printlnDots("Default persistent group set to '" + name + "'");
	}

	public void cmd_show_pg(Scanner scanner) throws PEException {
		final String name = scan(scanner, "name");

		final PersistentGroup pg = catHelper.getStorageGroup(name);
		final List<PersistentSite> sites = pg.getStorageSites();

		printlnDots("Persistent group '" + pg.getName() + "' size=" + sites.size());
		println("Sites:");
		for (final PersistentSite site : sites) {
			final StringBuffer siteInstances = new StringBuffer();
			for (final SiteInstance siteInstance : site.getSiteInstances()) {
				if (!siteInstance.isMaster()) {
					if (siteInstances.length() > 0) {
						siteInstances.append(", ");
					}
					siteInstances.append(siteInstance.getInstanceURL());
					if (!siteInstance.isEnabled()) {
						siteInstances.append("(disabled)");
					}
				}
			}

			final String instText = (siteInstances.length() > 0) ? ", [" + siteInstances.toString() + "]" : "";
			println(site.getName() + ", " + site.getHAType() + ", " + site.getMasterUrl() + instText);
		}
	}

	public void cmd_add_provider(Scanner scanner) throws PEException {
		final String name = scan(scanner, "name");
		final String plugin = scan(scanner, "plugin");
		final File providerFile = scanFile(scanner);

		final Provider provider = catHelper.createSiteProvider(name, plugin,
				(providerFile == null) ? null : PEFileUtils.readToString(providerFile));

		printlnDots("Site provider '" + provider.getName() + "' created");
	}

	public void cmd_configure_provider(Scanner scanner) throws PEException {
		final String name = scan(scanner, "name");
		final File providerFile = scanFile(scanner, "filename");

		final Provider provider = catHelper.setSiteProviderConfig(name, PEFileUtils.readToString(providerFile));

		printlnDots("Site provider '" + provider.getName() + "' configuration complete");
	}

	public void cmd_show_provider(Scanner scanner) throws PEException {
		final String name = scan(scanner, "name");

		final Provider provider = catHelper.getSiteProvider(name);

		printlnDots("Site provider '" + provider.getName() + "' plugin='" + provider.getPlugin() + "' enabled="
				+ provider.isEnabled());
		println("Configuration:");
		println(provider.getConfig());
	}

	public void cmd_remove_provider(Scanner scanner) throws PEException {
		final String name = scan(scanner, "name");

		catHelper.removeSiteProvider(name);
		printlnDots("Site provider '" + name + "' removed");
	}

	public void cmd_add_policy(Scanner scanner) throws PEException {
		final File policyFile = scanFile(scanner, "filename");

		final String policyContent = PEFileUtils.readToString(policyFile);
		final PolicyConfig policyConfig = PEXmlUtils.unmarshalJAXB(policyContent, PolicyConfig.class,
				PEFileUtils.getClasspathURL(PolicyConfig.class, POLICY_SCHEMA));
		final DynamicPolicy policy = parsePolicyConfig(policyConfig);

		catHelper.createDynamicPolicy(policy);

		printlnDots("Dynamic policy '" + policy.getName() + "' created");
	}

	public void cmd_configure_policy(Scanner scanner) throws PEException {
		final File policyFile = scanFile(scanner, "filename");

		final String policyContent = PEFileUtils.readToString(policyFile);
		final PolicyConfig policyConfig = PEXmlUtils.unmarshalJAXB(policyContent, PolicyConfig.class,
				PEFileUtils.getClasspathURL(PolicyConfig.class, POLICY_SCHEMA));
		final DynamicPolicy policy = parsePolicyConfig(policyConfig);

		catHelper.setDynamicPolicyConfig(policy);

		printlnDots("Dynamic policy '" + policy.getName() + "' configuration complete");
	}

	public void cmd_show_policy(Scanner scanner) throws PEException {
		final String name = scan(scanner, "name");

		final DynamicPolicy policy = catHelper.getDynamicPolicy(name);

		printlnDots("Dynamic policy '" + policy.getName() + "'");
		println("Configuration:");
		println("Aggregation : " + policy.getAggregationClass().toString());
		println("Small       : " + policy.getSmallClass().toString());
		println("Medium      : " + policy.getMediumClass().toString());
		println("Large       : " + policy.getLargeClass().toString());
	}

	public void cmd_set_default_policy(Scanner scanner) throws PEException {
		final String name = scan(scanner, "name");

		catHelper.setDefaultDynamicPolicy(name);

		printlnDots("Default dynamic policy set to '" + name + "'");
	}

	public void cmd_set_groupService(Scanner scanner) throws PEException {
		final String name = scan(scanner, "groupService");

		String type = null;

		if (name.equalsIgnoreCase(LocalhostCoordinationServices.TYPE)) {
			type = LocalhostCoordinationServices.TYPE;
		} else if (name.equalsIgnoreCase(HazelcastCoordinationServices.TYPE)) {
			type = HazelcastCoordinationServices.TYPE;
		}

		if (type == null) {
			throw new PEException("'" + name + "' isn't a recognized type for group service");
		}

		catHelper.setVariable(VariableConstants.GROUP_SERVICE_NAME, type, true);
	}

	@SuppressWarnings("unused")
	public void cmd_show_variables(Scanner scanner) throws PEException {
		final List<VariableConfig> variables = catHelper.getAllVariables();

		printlnDots("Variables size: " + variables.size());
		for (final VariableConfig variable : variables) {
			if (variable.getScopes().indexOf(VariableScopeKind.GLOBAL.name()) > -1)
				println(variable.getName() + " = " + variable.getValue());
		}
	}

	public void cmd_set_variable(Scanner scanner) throws PEException {
		final String variable = scan(scanner, "variable");

		final int index = variable.indexOf('=');

		if (index == -1) {
			throw new PEException("Variable should be in the form <key>=<value>");
		}

		final String key = variable.substring(0, index);
		final String value = variable.substring(index + 1);

		catHelper.setVariable(key, value, true);

		printlnDots("Successfully set variable '" + key + "' to value '" + value + "'");
	}

	public void cmd_add_es(Scanner scanner) throws PEException {
		final String name = scan(scanner, "name");
		final String plugin = scan(scanner, "plugin");
		final boolean usesDataStore = scanBoolean(scanner, "use data source");

		String connectUser = scan(scanner);
		if ((connectUser != null) && connectUser.equals("null")) {
			connectUser = null;
		}

		final File esFile = scanFile(scanner);

		final ExternalService externalService = catHelper.createExternalService(name, plugin, connectUser, usesDataStore,
				(esFile == null) ? null : PEFileUtils.readToString(esFile));

		printlnDots("External service '" + externalService.getName() + "' created");
	}

	public void cmd_configure_es(Scanner scanner) throws PEException {
		final String name = scan(scanner, "name");
		final File esFile = scanFile(scanner, "filename");

		final ExternalService externalService = catHelper.setExternalServiceConfig(name, PEFileUtils.readToString(esFile));

		printlnDots("External service '" + externalService.getName() + "' configuration complete");
	}

	public void cmd_show_es(Scanner scanner) throws PEException {
		final String name = scan(scanner, "name");

		final ExternalService externalService = catHelper.getExternalService(name);

		printlnDots("External service '" + externalService.getName() + "' plugin='" + externalService.getPlugin()
				+ "' auto start=" + externalService.isAutoStart() + "' uses datastore="
				+ externalService.usesDataStore() + " 'connect user='" + externalService.getConnectUser());
		println("Configuration:");
		println(externalService.getConfig());
	}

	public void cmd_remove_es(Scanner scanner) throws PEException {
		final String name = scan(scanner, "name");

		catHelper.removeExternalService(name);
		printlnDots("External service '" + name + "' removed");
	}

	public void cmd_upgrade_catalog() throws PEException {
		catHelper.checkURLAvailable();

		final Properties props = catHelper.getProperties();
		final String catalogLocation = props.getProperty(DBHelper.CONN_URL, "");

		boolean upgradeRequired = false;
		try {
			CatalogVersions.catalogVersionCheck(props);
		} catch (final PEException e) {
			upgradeRequired = true;
		}

		if (!upgradeRequired) {
			throw new PEException("DVE catalog at '" + catalogLocation
					+ "' is latest version. Upgrade not required.");
		}

		println("You are about to upgrade your DVE catalog located at '" + catalogLocation + "'");
		askQuestion("Do you want to continue [y/N]? ", new QuestionCallback() {
			@Override
			public void answer(String line) throws PEException {

				if ((line != null) && (line.equalsIgnoreCase("y") || line.equalsIgnoreCase("yes"))) {
					printlnDots("Starting DVE catalog upgrade at '" + catalogLocation + "'");
					CatalogVersions.upgradeToLatest(props);
					printlnDots("Catalog upgrade complete");
				}
			}
		});
	}

	public void cmd_check_catalog_version() throws PEException {
		catHelper.checkURLAvailable();

		final Properties props = catHelper.getProperties();
		final String catalogLocation = props.getProperty(DBHelper.CONN_URL, "");

		printlnDots("Checking version of DVE catalog at '" + catalogLocation + "'");

		final CatalogSchemaVersion version = CatalogVersions.catalogVersionCheck(props);
		printlnDots("DVE catalog at " + catalogLocation + " is at latest version (" + version.getSchemaVersion()
				+ ")");
	}

	public void cmd_truncate_servers() {
		println("You are about to completely remove all registered servers from the DVE catalog");
		askQuestion("Do you want to continue [y/N]? ", new QuestionCallback() {
			@Override
			public void answer(String line) throws PEException {
				if ((line != null) && (line.equalsIgnoreCase("y") || line.equalsIgnoreCase("yes"))) {
					catHelper.deleteAllServerRegistration();
					printlnDots("All registered servers removed from the DVE catalog");
				}
			}
		});
	}

	public void cmd_action_list() {
		printlnDots("Available actions are:");
		for (final FieldAction action : FieldActionManager.getAllActions()) {
			println(action.getName() + " - " + action.getDescription());
		}
	}

	public void cmd_action_report(Scanner scanner) throws PEException {
		final String name = scan(scanner, "name");

		final FieldAction action = getActionOnCurrentCatalog(name);
		action.report(catHelper.getProperties(), getPrintStream());
	}

	public void cmd_action_run(Scanner scanner) throws PEException {
		final String name = scan(scanner, "name");

		final FieldAction action = getActionOnCurrentCatalog(name);
		action.execute(catHelper.getProperties(), getPrintStream());
	}

	private FieldAction getActionOnCurrentCatalog(final String name) throws PEException {
		final FieldAction action = FieldActionManager.getAction(name);

		if (!action.isValid(CatalogVersions.getCurrentVersion())) {
			throw new PEException("Action '" + name + "' isn't available for the current catalog version");
		}

		return action;
	}

	private DynamicPolicy parsePolicyConfig(PolicyConfig config) {
		return new DynamicPolicy(config.getName(), config.isStrict(), config.getAggregation().getProvider(), config
				.getAggregation().getPool(), config.getAggregation().getCount(), config.getSmall().getProvider(),
				config.getSmall().getPool(), config.getSmall().getCount(), config.getMedium().getProvider(), config
						.getMedium().getPool(), config.getMedium().getCount(), config.getLarge().getProvider(), config
						.getLarge().getPool(), config.getLarge().getCount());
	}

	public void cmd_connect(Scanner scanner) throws PEException {
		cmd_disconnect();

		String url = PEConstants.MYSQL_URL;
		String user = PEConstants.ROOT;
		String password = PEConstants.PASSWORD;

		if (scanner.hasNext()) {
			url = scan(scanner, "url");
			user = scan(scanner, "user");
			password = scan(scanner, "password");
		}

		url = PEUrl.fromUrlString(url).getURL();

		dbHelper = new DBHelper(url, user, password).connect();

		printlnDots("You are now connected to the DVE server at '" + url + "'");
	}

	public void cmd_disconnect() {
		if (dbHelper != null) {
			dbHelper.disconnect();
			printlnDots("You are now disconnected from the DVE server");
		}
		dbHelper = null;
	}

	public void cmd_add_template(Scanner scanner) throws PEException {
		final File templateFile = scanFile(scanner, "filename");
		final String match = scan(scanner);

		addTemplate(templateFile, false, match);
		printlnDots("Template '" + templateFile.getAbsolutePath() + "' added to catalog");
	}

	public void cmd_add_templates(Scanner scanner) throws PEException {
		final File folder = scanFile(scanner, "folder");
		pushTemplatesToCatalog(folder, false);
	}

	public void cmd_update_template(Scanner scanner) throws PEException {
		final File templateFile = scanFile(scanner, "filename");
		final String match = scan(scanner);

		addTemplate(templateFile, true, match);
		printlnDots("Template '" + templateFile + "' updated in catalog");
	}

	public void cmd_update_templates(Scanner scanner) throws PEException {
		final File folder = scanFile(scanner, "folder");
		pushTemplatesToCatalog(folder, true);
	}

	private void pushTemplatesToCatalog(final File folder, final boolean updateExisting) throws PEException {
		if (!folder.isDirectory()) {
			throw new PEException("'" + folder + "' isn't a valid directory");
		}

		final File[] files = folder.listFiles(TEMPLATE_FILE_NAME_FILTER);
		for (final File file : files) {
			try {
				addTemplate(file, updateExisting, null);
				printlnDots("Template '" + file.getPath() + "' added to catalog");
			} catch (final Exception e) {
				printlnDots("Could not add template '" + file.getAbsolutePath() + "' - " + e.getMessage());
			}
		}
	}

	private String addTemplate(File templateFile, boolean update, String match) throws PEException {
		final String templateStr = PEFileUtils.readToString(templateFile);

		final Template template = PEXmlUtils.unmarshalJAXB(templateStr, Template.class, PEFileUtils.getClasspathURL(Template.class, TEMPLATE_SCHEMA));

		final String useName = template.getName();
		final String useComment = template.getComment();
		final String useMatch = StringUtils.isBlank(match) ? template.getMatch() : match;

		try {
			if (dbHelper != null) {
				addTemplateToServer(useName, templateStr, update, useMatch, useComment);
			} else {
				addTemplateToCatalog(useName, templateStr, update, useMatch, useComment);
			}

			return useName;
		} catch (final Exception e) {
			if (update) {
				throw new PEException("Failed to update template '" + template + "' in catalog - " + e.getMessage(), e);
			} else {
				throw new PEException("Failed to add template '" + template + "' to catalog - " + e.getMessage(), e);
			}
		}
	}

	private void addTemplateToServer(String name, String templateStr, boolean update, String match, String comment)
			throws Exception {
		final StringBuilder query = new StringBuilder();
		if (update) {
			query.append("ALTER TEMPLATE `").append(name).append("` SET XML='")
					.append(StringEscapeUtils.escapeSql(templateStr)).append("'");
		} else {
			query.append("CREATE TEMPLATE `").append(name).append("` XML='")
					.append(StringEscapeUtils.escapeSql(templateStr)).append("'");
		}

		if (StringUtils.isNotBlank(match)) {
			query.append(" MATCH='").append(match).append("'");
		}

		if (StringUtils.isNotBlank(comment)) {
			query.append(" COMMENT='").append(comment).append("'");
		}

		dbHelper.executeQuery(query.toString());
	}

	private void addTemplateToCatalog(String name, String templateStr, boolean update, String match, String comment)
			throws Exception {
		if (update) {
			catHelper.updateTemplate(name, templateStr, match, comment);
		} else {
			catHelper.createTemplate(name, templateStr, match, comment);
		}
	}

	private JMXMBeanConnector jmxConnector = null;
	private String jmxDomain = null;
	private ObjectName jmxMBean = null;
	private MBeanInfo jmxMBeanInfo = null;

	public void registerJMXCommands() {
		// The following commands are for JMX mode
		final CommandMapType map = createCommandMap("jmx");

		map.registerCommand(new CommandType(new String[] { "connect" }, "[<host> <port>]"));
		map.registerCommand(new CommandType(new String[] { "list", "domains" }, ""));
		map.registerCommand(new CommandType(new String[] { "domain" }, "<domain>"));
		map.registerCommand(new CommandType(new String[] { "list", "mbeans" }, ""));
		map.registerCommand(new CommandType(new String[] { "mbean" }, "<mbean>"));
		map.registerCommand(new CommandType(new String[] { "get", "attribute" }, "<attribute>"));
		map.registerCommand(new CommandType(new String[] { "get", "attributes" }, ""));
		map.registerCommand(new CommandType(new String[] { "list", "operations" }, ""));
		map.registerCommand(new CommandType(new String[] { "invoke" }, "[<arg 1> ... <arg n>]"));

	}

	public void cmd_jmx_connect(Scanner scanner) throws PEException {
		String host = scan(scanner);
		Integer port = scanInteger(scanner);

		if (host == null) {
			host = DEFAULT_JMX_HOST;
		}

		if (port == null)
		{
			port = DEFAULT_JMX_PORT;
		}

		jmxConnector = new JMXMBeanConnector(host, port);

		printlnDots("You are now connected to the MBean server at '" + jmxConnector.serviceUrl + "'");

		cmd_jmx_list_domains();
	}

	public void cmd_jmx_disconnect() throws PEException {
		checkJMXConnected();

		jmxConnector.close();

		this.jmxConnector = null;
		this.jmxDomain = null;
		this.jmxMBean = null;
		this.jmxMBeanInfo = null;
	}

	public void cmd_jmx_list_domains() throws PEException {
		checkJMXConnected();

		final String[] domains = jmxConnector.getBeanDomains();

		printlnDots("Available MBean domain names are:");

		for (final String domain : domains) {
			println(domain);
		}
	}

	public void cmd_jmx_domain(Scanner scanner) throws PEException {
		checkJMXConnected();

		final String domain = scan(scanner, "domain");

		jmxConnector.getBeanNames(domain);

		jmxDomain = domain;
		jmxMBean = null;
		jmxMBeanInfo = null;

		printlnDots("Domain '" + jmxDomain + "' selected");

		cmd_jmx_list_mbeans();
	}

	public void cmd_jmx_list_mbeans() throws PEException {
		checkJMXConnected();
		checkJMXDomain();

		final Set<ObjectName> beans = jmxConnector.getBeanNames(jmxDomain);

		printlnDots("Available MBean(s) in domain '" + jmxDomain + "' are:");

		for (final ObjectName bean : beans) {
			println(bean.getCanonicalKeyPropertyListString());
		}
	}

	public void cmd_jmx_mbean(Scanner scanner) throws PEException {
		checkJMXConnected();
		checkJMXDomain();

		final String mbean = scan(scanner, "mbean");

		final ObjectName on = jmxConnector.makeObjectName(jmxDomain, mbean);

		final MBeanInfo info = jmxConnector.getBeanInfo(on);

		jmxMBean = on;
		jmxMBeanInfo = info;

		printlnDots("MBean '" + jmxMBean + "' selected");

		println("Attributes = " + (info.getAttributes() == null ? "N/A" : info.getAttributes().length));
		println("Operations = " + (info.getOperations() == null ? "N/A" : info.getOperations().length));
	}

	public void cmd_jmx_get_attribute(Scanner scanner) throws PEException {

		checkJMXConnected();
		checkJMXDomain();
		checkJMXMBean();

		final String attribute = scan(scanner, "attribute");

		printlnDots("Attribute value for '" + attribute + "' in mbean '" + jmxMBean + "' is:");

		println(attribute + " = " + jmxConnector.getAttribute(jmxMBean, attribute));
	}

	public void cmd_jmx_get_attributes() throws PEException {

		checkJMXConnected();
		checkJMXDomain();
		checkJMXMBean();

		for (final MBeanAttributeInfo attrib : jmxMBeanInfo.getAttributes()) {
			if (attrib.isReadable()) {
				println(attrib.getName() + " = " + jmxConnector.getAttribute(jmxMBean, attrib.getName()));
			}
		}
	}

	public void cmd_jmx_list_operations() throws PEException {

		checkJMXConnected();
		checkJMXDomain();
		checkJMXMBean();

		for (final MBeanOperationInfo operation : jmxMBeanInfo.getOperations()) {
			final String name = operation.getName();
			final StringBuilder sParam = new StringBuilder();
			for (final MBeanParameterInfo param : operation.getSignature()) {
				if (sParam.length() > 0) {
					sParam.append(", ");
				}
				sParam.append(formatType(param.getType())).append(" ").append(param.getName());
			}

			final StringBuilder builder = new StringBuilder();
			println(builder.append(name).append("(").append(sParam.toString()).append(")").toString());
		}
	}

	public void cmd_jmx_invoke(Scanner scanner) throws PEException {

		checkJMXConnected();
		checkJMXDomain();
		checkJMXMBean();

		final String opName = scan(scanner, "operation");

		// First lookup the operation to ensure it exists
		MBeanOperationInfo found = null;
		for (final MBeanOperationInfo operation : jmxMBeanInfo.getOperations()) {
			if (operation.getName().equals(opName)) {
				found = operation;
				break;
			}
		}

		if (found == null) {
			throw new PEException("'" + opName + "' isn't a valid operation on '" + jmxMBean + "'");
		}

		final MBeanParameterInfo[] params = found.getSignature();

		final Object[] paramO = params.length == 0 ? null : new Object[params.length];
		final String[] paramT = params.length == 0 ? null : new String[params.length];

		for (int i = 0; i < params.length; i++) {
			// For each parameter we expect to have a matching item in the
			// scanner

			final String val = scan(scanner, params[i].getName());

			paramO[i] = jmxConnector.stringToObject(val, params[i].getType());
			paramT[i] = params[i].getType();
		}
		jmxConnector.invokeOperation(jmxMBean, found, paramO, paramT);
	}

	private String formatType(String type) {
		if (type.startsWith("[")) {
			switch (type.charAt(1)) {
			case 'C':
				return "char[]";
			case 'D':
				return "double[]";
			case 'F':
				return "float[]";
			case 'I':
				return "int[]";
			case 'J':
				return "long[]";
			case 'S':
				return "short[]";
			case 'Z':
				return "boolean[]";
			case 'L':
				return type.substring(2, type.indexOf(";"));
			}
		}
		return type;
	}

	private void checkJMXConnected() throws PEException {
		if (jmxConnector == null) {
			throw new PEException("Your aren't connected to a JMX MBean server");
		}
	}

	private void checkJMXDomain() throws PEException {
		if (jmxDomain == null) {
			throw new PEException("Your haven't selected a JMX MBean domain");
		}
	}

	private void checkJMXMBean() throws PEException {
		if ((jmxMBean == null) || (jmxMBeanInfo == null)) {
			throw new PEException("Your haven't selected a JMX MBean");
		}
	}

	public static void main(String[] args) {
		try {
			new DVEConfigCLI(args).start();
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}
}