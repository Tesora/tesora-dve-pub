# Tesora Database Virtualization Engine

The Tesora Database Virtualization Engine (DVE) lets you scale on demand with 
the power of multiple database servers acting as one.  Tesora's Database Virtualization Engine 
sits between your application and a group of relational database servers. The software exposes 
native MySQL interfaces and your application sees a single "virtual database".  It will seamlessly 
support operational workloads that exceed the capabilities of a single database server, 
while only provisioning, consuming and paying for the capacity that you need at any given instant.

### Building DVE
1. Install [Oracle](http://www.oracle.com/technetwork/java/javase/downloads/index.html) or [OpenJDK](http://openjdk.java.net/install/index.html) Java SDK 7.
2. Install Maven 3.0.3 or later.
3. Download the following library [jFuzzyLogic V3.0](http://sourceforge.net/projects/jfuzzylogic/files/jfuzzylogic/jFuzzyLogic_v3.0.jar/download).  Add the jFuzzyLogic library to the local Maven repository using the following command.
	
	```
 	mvn install:install-file -Dfile=jFuzzyLogic_v3.0.jar -DgroupId=net.sourceforge -DartifactId=jFuzzyLogic -Dversion=3.0 -Dpackaging=jar
 	```
 	
4. Obtain the source code from the git repository.
5. Run `mvn install -DskipTests` to build the project and place the newly built jar files in the local Maven repository.

### Running Unit Tests
1. Install MySQL 5.5 or greater. *It is recommended to change the port on the native MySQL instance from the default 3306 to 3307 so as not to conflict with DVE running on 3306*.
2. Configure a settings.xml in the Maven local repository to specify the catalog user/password combination to the native MySQL instance.  The following is a sample file.
		
	```
	<settings>
	    <profiles>
	        <profile>
	            <activation>
	                <activeByDefault>true</activeByDefault>
	            </activation>
	            <properties>
	                <jdbc.mysql.user>root</jdbc.mysql.user>
	                <jdbc.mysql.password>password</jdbc.mysql.password>
	                <log4j.logger.com.parelastic>WARN</log4j.logger.com.parelastic>
	            </properties>
	       </profile>
	    </profiles>
	</settings>
	```
		
3. From the command line run `mvn test` to run all the unit tests in the project.

### Issues
If you have questions post them to the [Developer Forum](https://groups.google.com/forum/#!forum/tesora-dve-dev) or [User Forum](https://groups.google.com/forum/#!forum/tesora-dve-user). 

Talk to us on our IRC channel `#Tesora-DVE` (freenode).

### Links
Find out more about [Tesora](http://www.tesora.com).

Follow us [@tesoracorp](http://twitter.com/tesoracorp).



