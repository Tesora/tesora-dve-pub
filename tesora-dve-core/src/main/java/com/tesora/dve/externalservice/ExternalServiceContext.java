// OS_STATUS: public
package com.tesora.dve.externalservice;

import java.sql.SQLException;

import com.tesora.dve.common.catalog.ExternalService;
import com.tesora.dve.dbc.ServerDBConnectionParameters;
import com.tesora.dve.exceptions.PEException;

public interface ExternalServiceContext {
	/**
	 * Set the {@link ExternalService} auto start attribute
	 * 
	 * @param autoStart new value
	 * @throws PEException if problems occur accessing the DVE catalog
	 */
	public void setServiceAutoStart(boolean autoStart) throws PEException;

	/**
	 * Get the {@link ExternalService} auto start attribute
	 * 
	 * @return value of auto start attribute
	 * @throws PEException if problems occur accessing the DVE catalog
	 */
	public boolean getServiceAutoStart() throws PEException;
	
	/**
	 * Set the configuration for the {@link ExternalService}
	 * 
	 * @param config a reference to {@link ExternalServiceConfig} containing the  
	 * new configuration of the {@link ExternalService}  
	 * @throws PEException if problems occur accessing the DVE catalog
	 */
	public void setServiceConfig(ExternalServiceConfig config) throws PEException;
	
	/**
	 * Get the configuration for the {@link ExternalService}
	 * @parem config a reference to {@link ExternalServiceConfig} where the configuration
	 * of the {@link ExternalService} is to end up
	 * @throws PEException if problems occur accessing the DVE catalog
	 */
	public void getServiceConfig(ExternalServiceConfig config) throws PEException;
	
	/**
	 * Get the name of the {@link ExternalService}
	 * 
	 * @return the name
	 * @throws PEException if problems occur accessing the DVE catalog
	 */
	public String getServiceName() throws PEException;
	
	/**
	 * Get the name of the {@link ExternalService} plugin
	 * 
	 * @return the plugin name
	 * @throws PEException if problems occur accessing the DVE catalog
	 */
	public String getPlugin() throws PEException;
	
	/**
	 * Used to get instance of {@link Object} object.
	 * 
	 * @return {@link Object} reference
	 * @throws SQLException if connection to PE server failed
	 * @throws PEException 
	 */
	public Object getServerDBConnection() throws SQLException, PEException;

	/**
	 * Used to set connection database context to the service's 
	 * datastore
	 *  
	 * @return {@link Object} reference
	 * @throws PEException - if the service isn't configured
	 * to use a datastore or the datastore can't be found
	 * @throws SQLException - if setting the database context 
	 * on the server fails
	 */
	public Object useServiceDataStore() throws SQLException, PEException;
	
	/**
	 * Used to get the parameters on the server connection
	 * 
	 * @return {@link ServerDBConnectionParameters} reference
	 */
	public ServerDBConnectionParameters getSvrDBParams();

	/**
	 * Used to set the parameters on the server connection
	 * 
	 * @param svrDBParams
	 */
	public void setSvrDBParams(ServerDBConnectionParameters svrDBParams);

	/**
	 * Called when the external service is closing
	 * 
	 * @throws PEException
	 */
	public void close() throws PEException;

	/**
	 * Closes the connection to the service's datastore
	 * 
	 * @throws PEException
	 */
	void closeServerDBConnection() throws PEException;
}