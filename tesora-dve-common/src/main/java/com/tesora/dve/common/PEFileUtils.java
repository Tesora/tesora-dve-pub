// OS_STATUS: public
package com.tesora.dve.common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;

public class PEFileUtils {

	public static final String PEFU_EOL_MATCH = "\\r?\\n|\\r";

	static Logger logger = Logger.getLogger(PEFileUtils.class);

	protected PEFileUtils() {
	}

	/**
	 * Load a properties file from the classpath relative to the specified class
	 * and return a <code>Properties</code> object.
	 * 
	 * @param testClass
	 *            <code>Class</code> of caller
	 * @param fileName
	 *            name of file to load
	 * @return <code>Properties</code> object
	 * @throws PEException
	 *             if the file cannot be found or loaded
	 */
	public static Properties loadPropertiesFile(Class<?> testClass, String fileName) throws PEException {
		return loadPropertiesFile(testClass, fileName, false);
	}

	/**
	 * Load a properties file from the classpath relative to the specified class
	 * and return a <code>Properties</code> object. Optionally decrypt passwords
	 * in the file
	 * 
	 * @param testClass
	 *            <code>Class</code> of caller
	 * @param fileName
	 *            name of file to load
	 * @param skipDecryption
	 *            whether to decrypt password fields or not
	 * @return <code>Properties</code> object
	 * @throws PEException
	 *             if the file cannot be found or loaded
	 */
	public static Properties loadPropertiesFile(Class<?> testClass, String fileName, boolean skipDecryption)
			throws PEException {
		return doLoad(getResourceStream(testClass, fileName), fileName, skipDecryption);
	}

	/**
	 * Save the specified properties back to a file
	 * 
	 * @param testClass
	 * @param fileName
	 * @param props
	 * @throws PEException
	 */
	public static void savePropertiesToClasspath(Class<?> testClass, String fileName, Properties props)
			throws PEException {
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(new File(testClass.getClassLoader().getResource(fileName).toURI()));

			// We will encrypt on a cloned copy of the properties so that we can
			// continue using the properties we have
			Properties clonedProps = (Properties) props.clone();

			encrypt(clonedProps);

			clonedProps.store(fos, "This file was last updated by DVE on:");
		} catch (Exception e) {
			throw new PEException("Error saving properties file '" + fileName + "'", e);
		} finally {
			if (fos != null) {
				try {

					fos.close();
				} catch (Exception e) {
					// ignore
				}
			}
		}
	}

	/**
	 * Load properties from the classpath root
	 * 
	 * @param testClass
	 * @param fileName
	 * @return
	 * @throws PEException
	 */
	public static Properties loadPropertiesFromClasspath(Class<?> testClass, String fileName) throws PEException {
		return doLoad(getClasspathResourceStream(testClass, fileName), fileName, false);
	}

	/**
	 * Helper function to load a properties file and return a
	 * <code>Properties</code> object.
	 * 
	 * @param fileName
	 *            name of file to load
	 * @return <code>Properties</code> object
	 * @throws PEException
	 *             if the file cannot be found or loaded
	 */
	public static Properties loadPropertiesFile(String fileName) throws PEException {
		return loadPropertiesFile(fileName, false);
	}

	public static Properties loadPropertiesFile(String fileName, boolean skipDecryption) throws PEException {
		if (logger.isDebugEnabled())
			logger.debug("loadPropertiesFile '" + fileName + "'");

		try (final InputStream is = new FileInputStream(fileName)) {
			return doLoad(is, fileName, skipDecryption);
		} catch (IOException e) {
			throw new PEException("Error loading properties file '" + fileName + "'", e);
		}
	}

	private static Properties doLoad(InputStream is, String name, boolean skipDecryption) throws PEException {
		Properties props = new PEProperties();

		try {
			props.load(is);

			if (!skipDecryption)
				decrypt(props);

			if (logger.isDebugEnabled())
				logger.debug("Loaded properties file with identifier: "
						+ props.getProperty("properties.identifier", "Not specified"));

		} catch (Exception e) {
			throw new PEException("Error loading properties file '" + name + "'", e);
		}

		return props;
	}

	private static Properties encrypt(Properties props) throws PEException {
		// we are going to iterate over all properties looking for any that
		// contain the string "password"
		for (String key : props.stringPropertyNames()) {
			if (StringUtils.containsIgnoreCase(key, "password")) {
				String value = props.getProperty(key);

				if (!StringUtils.isBlank(value)) {
					props.setProperty(key, PECryptoUtils.encrypt(value));
				}
			}
		}
		return props;
	}

	private static Properties decrypt(Properties props) throws PEException {
		// we are going to iterate over all properties looking for any that
		// contain the string "password"
		for (String key : props.stringPropertyNames()) {
			if (StringUtils.containsIgnoreCase(key, "password")) {
				String value = props.getProperty(key);

				if (!StringUtils.isBlank(value)) {
					props.setProperty(key, PECryptoUtils.decrypt(value));
				}
			}
		}
		return props;
	}

	public static PropertiesConfiguration loadPropertiesConfigFromClasspath(Class<?> testClass, String fileName)
			throws PEException {
		PropertiesConfiguration props = new PropertiesConfiguration();
		try {
			props.load(getResourceStream(testClass, fileName));
		} catch (ConfigurationException e) {
			throw new PEException("Error loading properties file '" + fileName + "'", e);
		}

		return props;
	}

	/**
	 * Helper function to load a file from the classpath relative to the
	 * specified class and return it as an <code>InputStream</code>.
	 * 
	 * @param testClass
	 *            <code>Class</code> of caller
	 * @param fileName
	 *            name of file to load
	 * @return <code>InputStream</code> of file
	 * @throws PEException
	 *             if file cannot be located
	 */
	public static InputStream getResourceStream(Class<?> testClass, String fileName) throws PEException {

		InputStream is = testClass.getResourceAsStream(fileName);

		validateFileResource(fileName, is);

		return is;
	}
	
	/**
	 * Helper function to load a file from the classpath relative to the root
	 * classpath and return it as an <code>InputStream</code>.
	 * 
	 * @param testClass
	 *            <code>Class</code> of caller
	 * @param fileName
	 *            name of file to load
	 * @return <code>InputStream</code> of file
	 * @throws PEException
	 *             if file cannot be located
	 */
	public static InputStream getClasspathResourceStream(Class<?> testClass, String fileName) throws PEException {

		InputStream is = testClass.getClassLoader().getResourceAsStream(fileName);

		validateFileResource(fileName, is);

		return is;
	}

	public static URL getResourceURL(final Class<?> testClass, final String fileName) throws PEException {
		final URL resource = testClass.getResource(fileName);

		validateFileResource(fileName, resource);

		return resource;
	}

	public static URL getClasspathURL(final Class<?> testClass, final String fileName) throws PEException {
		final URL resource = testClass.getClassLoader().getResource(fileName);

		validateFileResource(fileName, resource);

		return resource;
	}

	private static void validateFileResource(final String fileName, final Object resource) throws PEException {
		if (resource == null) {
			throw new PEException("Cannot find resource file '" + fileName + "'");
		}
	}

	public static File getResourceFile(final Class<?> testClass, final String fileName) throws PEException {
		return new File(convert(getResourceURL(testClass, fileName)));
	}

	public static File getClasspathFile(final Class<?> testClass, final String fileName) throws PEException {
		return new File(convert(getClasspathURL(testClass, fileName)));
	}

	public static URI convert(final URL url) throws PEException {
		try {
			return url.toURI();
		} catch (final URISyntaxException e) {
			throw new PEException("Malformed resource URI '" + url.toString() + "'", e);
		}
	}

	public static String readToString(Class<?> testClass, String fileName) throws PEException {
		return readToString(getResourceStream(testClass, fileName));
	}

	public static String readToString(Class<?> testClass, String fileName, Charset cs) throws PEException {
		return readToString(getResourceStream(testClass, fileName), cs);
	}

	public static String readToString(final String path) throws PEException {
		return readToString(path, null);
	}

	public static String readToString(final String path, final Charset cs) throws PEException {
		return readToString(new File(path), cs);
	}

	public static String readToString(final File file) throws PEException {
		return readToString(file, null);
	}

	public static String readToString(final File file, final Charset charset) throws PEException {
		try {
			return FileUtils.readFileToString(file, charset);
		} catch (final Exception e) {
			throw new PEException("Error reading file '" + file.getAbsolutePath() + "' - " + e.getMessage(), e);
		}
	}

	public static String readToString(InputStream istream) throws PEException {
		return readToString(istream, Charset.defaultCharset());
	}

	public static String readToString(InputStream istream, Charset cs) throws PEException {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(istream, cs));
			return readToString(reader);
		} catch (IOException e) {
			throw new PEException("Error reading inputStream - " + e.getMessage(), e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (Exception e) {
					// ignore
				}
			}
		}
	}

	private static String readToString(BufferedReader reader) throws IOException {
		String line = null;
		StringBuilder stringBuilder = new StringBuilder();
		String ls = System.getProperty("line.separator");
		while ((line = reader.readLine()) != null) {
			stringBuilder.append(line);
			stringBuilder.append(ls);
		}
		return stringBuilder.toString();
	}

	public static void writeToFile(String fileName, String text, boolean overwriteContents) throws PEException {
		writeToFile(new File(fileName), text, overwriteContents);
	}

	/**
	 * Write the text out to the specified filename.
	 * 
	 * @param fileName
	 * @param text
	 * @param overwriteContents
	 * @throws Exception
	 */
	public static void writeToFile(File file, String text, boolean overwriteContents) throws PEException {
		BufferedWriter bw = null;
		try {
			if (file.exists() && !overwriteContents)
				throw new PEException("File '" + file.getCanonicalPath() + "' already exists");

			createDirectory(file.getParent());

			bw = new BufferedWriter(new FileWriter(file));
			bw.write(text);
		} catch (Exception e) {
			throw new PEException("Failed to write to file", e);
		} finally {
			if (bw != null) {
				try {
					bw.close();
				} catch (Exception e) {
					// Eat it
				}
			}
		}
	}

	/**
	 * Reads the contents of fileName and returns it in String array format.
	 * 
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public static String[] readToArray(String fileName) throws PEException {
		String fileContents = readToString(fileName);
		return fileContents.split(PEFU_EOL_MATCH);
	}

	/**
	 * Creates directory dirName. Returns true if the directory is created
	 * successfully, or if it already exists.
	 * 
	 * @param dirName
	 * @return
	 */
	public static boolean createDirectory(String dirName) {
		if (dirName == null) {
			return false;
		}

		boolean success = true;
		File dir = new File(dirName);
		if (!dir.exists()) {
			success = dir.mkdirs();
		} else if (!dir.isDirectory()) {
			return false;
		}
		return success;
	}

	public static String getCanonicalResourcePath(final Class<?> clazz, final String fileName) throws PEException {
		return getCanonicalPathFromURL(getResourceURL(clazz, fileName));
	}

	public static String getCanonicalClasspathPath(final Class<?> clazz, final String fileName) throws PEException {
		return getCanonicalPathFromURL(getClasspathURL(clazz, fileName));
	}

	private static String getCanonicalPathFromURL(final URL url) throws PEException {
		File resourceFile;
		try {
			final String os = System.getProperty("os.name");
			if (StringUtils.containsIgnoreCase(os, "win")) {
				return url.getFile();
			}

			resourceFile = new File(url.toURI());
		} catch (final URISyntaxException e) {
			resourceFile = new File(url.getPath());
		}

		try {
			return resourceFile.getCanonicalPath();
		} catch (final IOException e) {
			throw new PEException("Could not canonicalize the path '" + resourceFile.getAbsolutePath() + "'", e);
		}
	}

}
