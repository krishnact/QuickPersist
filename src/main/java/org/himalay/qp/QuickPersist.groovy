package org.himalay.qp

import java.io.File

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException

import org.codehaus.groovy.reflection.ReflectionUtils;
import org.codehaus.groovy.tools.xml.DomToGroovy;
import org.h2.mvstore.cache.CacheLongKeyLIRS.Config
import org.h2.tools.Server;
import org.hibernate.Session
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException

import groovy.text.SimpleTemplateEngine;;
class QuickPersist {
	static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(QuickPersist.class) 
	private static def profileMaps =[
		"h2.local" :[host:'127.0.0.1', port: 3743,dbPath:"quick_persist",showSQL: false],
		"h2.file" :[host:'127.0.0.1', port: 3743,dbPath:"quick_persist",showSQL: false ],
		"h2.memory" : [dbPath:"quick_persist",showSQL: false],
		"hsqldb.memory" :[dbPath:"quick_persist",showSQL: false]
	]
	static def serversList = []
	SessionFactory sessionFactory;
	Session        session;
	Configuration  conf;
	/**
	 * Creates a QuickPersis object. 
	 * @param profile Profile. One of h2.local, h2.mem, h2.file, hsqldb.memory,postgresql
	 * @param params  Parameters. If params is null then default parameters are used. They are different for each profile.
	 */
	QuickPersist(String profile, Map<String, String> params)
	{
		conf = getHibernateConf(profile, params);
	}
	
	
	/**
	 * Creates a QuickPersis object.
	 * @param profile Hibernate config file
	 */
	QuickPersist(File confFile)
	{
		conf = getHibernateConf(confFile);
	}
	
	/**
	 * Creates a QuickPersis object.
	 * @param profile Hibernate Conf
	 */
	QuickPersist(Configuration conf)
	{
		this.conf = conf
	}

	public String getConnectionURL()
	{
		return this.conf.getProperty("connection.url")
	}
	
	public String getDriverClass()
	{
		return this.conf.getProperty("connection.driver_class")
	}
	
	/**
	 * Insert into database when you already have Objects of entity Class
	 * @param objects
	 * @return
	 */
	public static QuickPersist writeIntoDatabase(
		List<Object> objects, 
		List<Class> classes, 
		File hibernateConfigFile )
	{
		
		Configuration cfg = null;
		SessionFactory sessionFactory = null
		cfg = getHibernateConf(hibernateConfigFile)
		
		return writeIntoDatabase(objects,classes,cfg,true);
	}

	private Session open()
	{

		sessionFactory =   this.conf.buildSessionFactory();
		Session session = sessionFactory.openSession();
		return session;
	}
	
	public Session writeIntoDatabaseEx(
		def objects,
		List<Class> classes,
		boolean clean = false)
	{
		
		classes.each{
			conf.addAnnotatedClass(it)
		}
		session = open()

		session.beginTransaction();
		objects.each {
			session.saveOrUpdate(it);
		}
		session.getTransaction().commit();
		if ( clean == true)
		{
			this.close()
		}
		return session;
	}
	
	public void close()
	{
		if ( this.sessionFactory != null)
		{
			session.close()
			sessionFactory.close()
			this.sessionFactory = null;
		}
	}
	
	/**
	 * Insert into database when you already have Objects of entity Class
	 * @param objects
	 * @return
	 */
	public static QuickPersist writeIntoDatabase(
		List<Object> objects, 
		List<Class> classes, 
		Configuration cfg = null, 
		boolean clean = false)
	{
		SessionFactory sessionFactory = null
		
		classes.each{
			cfg.addAnnotatedClass(it)
		}

		QuickPersist qp = new QuickPersist(cfg);
		qp.writeIntoDatabaseEx(objects,classes,clean)
		return qp
	}

	public void cleanSession(Session sess)
	{
		
	}
	
	/**
	 * Insert into database when you already have Objects of entity Class
	 * session Hibernate session to use
	 */
	public static Session writeIntoDatabaseLite(def objects, Session session)
	{
		
		session.beginTransaction();
		objects.each {
			//println "Inserting $it"
			session.save(it);
		}
		session.getTransaction().commit();
		return session;
	}

//	/**
//	 * Writes into database
//	 * @param cb The class builder
//	 * @param objects The list of Objects
//	 * @param hibernateConfigFile Hibernate config file
//	 */
//	public static QuickPersist writeIntoDatabaseLite(ClassBuilder cb,def objects)
//	{
//		
////		Configuration cfg = null;
////		SessionFactory sessionFactory = null
////		cfg = getHibernateConf(hibernateConfigFile)
////		cb.loadedClasses.each{
////			cfg.addAnnotatedClass(it)
////		}
////		sessionFactory =   cfg.buildSessionFactory();
////		Session session = sessionFactory.openSession();
////
////		session.beginTransaction();
////		objects.each {
////			session.save(it);
////		}
////		session.getTransaction().commit();
//		
//		Configuration cfg = getHibernateConf(hibernateConfigFile)
//		return writeIntoDatabaseLite(cb,objects,cfg);
//	}
//	
	
	public static QuickPersist writeIntoDatabaseLite(ClassBuilder cb,def objects, Configuration cfg)
	{
		
		SessionFactory sessionFactory = null
		cb.loadedClasses.each{
			cfg.addAnnotatedClass(it)
		}
		return writeIntoDatabase(objects, cb.loadedClasses,cfg)
	}
	
//	/**
//	 * Insert into database when you already have Objects of entity Class
//	 * @param objects
//	 * @return Session
//	 */
//	public static QuickPersist writeIntoDatabaseLite(def objects, File hibernateConfigFile = null, boolean clean = true)
//	{
//		return writeIntoDatabase(objects, [objects[0].class], hibernateConfigFile, clean)
//	}

	/**
	 * Get Hibernate Configuration from Hibernate config xml
	 * @param hibernateConfigFile
	 * @return Configuration
	 */
	public static Configuration getHibernateConf(File hibernateConfigFile)
	{
		Configuration cfg = null;
		if ( hibernateConfigFile == null)
		{
			hibernateConfigFile = new File(System.getProperty("HIBERNATE_CONF"))
		}
		cfg = new Configuration().configure( hibernateConfigFile)
		
		return cfg
	}
	
	/**
	 * Writes an array of maps into database
	 * @param arrayOfObjects
	 * @param objectTypeName
	 * @return
	 */
	public static ClassBuilder WriteToDBPart1(def arrayOfObjects, String objectTypeName, boolean printClasses=false)
	{
		ClassBuilder cb =  new ClassBuilder("org.himalay.autoorm")
		cb.analyze(arrayOfObjects, objectTypeName)
		cb.createClassesText();
		if ( printClasses == true)
		{
			cb.printClassesText();
		}
		cb.loadedClasses= cb.loadClasses()
		Thread.currentThread().setContextClassLoader( cb.parser);
		//writeIntoDatabaseLite(objs)
		return cb
	}

//	/**
//	 * This method just takes an array of hashmaps and writes into database. It creates necessary tables
//	 */
//	public static WriteToDB(def arrayOfObjects, String objectTypeName, boolean printClasses=false)
//	{
//		ClassBuilder cb = WriteToDBPart1(arrayOfObjects, objectTypeName, printClasses)
//		// Now create Objects
//		def objs = cb.createObjects(objectTypeName,arrayOfObjects)
//
//		writeIntoDatabaseLite(cb, objs)
//
//	}
	
	/**
	 * This method just takes an array of hashmaps and writes into database. It creates necessary tables
	 * @param arrayOfObjects An array of Maps
	 * @param objectTypeName The tableName
	 * @param hibernateCfg hibernate configuration
	 */
	public static QuickPersist WriteToDB(
		List<Object> arrayOfObjects, 
		String objectTypeName, 
		Configuration hibernateCfg = QuickPersist.getHibernateConf("h2.memory")
	)
	{
		ClassBuilder cb = WriteToDBPart1(arrayOfObjects, objectTypeName, false)
		// Now create Objects
		def objs = cb.createObjects(objectTypeName,arrayOfObjects)

		QuickPersist retVal = writeIntoDatabaseLite(cb, objs, hibernateCfg)
		return retVal
	}
	

	public static Configuration getHibernateConf(String profile, def map = null)
	{
		
		Configuration retVal = null
		if (map == null)
		{
			map = [:]
		}
		def myMap = [:]
		
		def storedParamsMap = profileMaps[profile]
		if (storedParamsMap == null)
		{
			String profiles = profileMaps.keySet().join(", ")
			throw new RuntimeException("Unknown profile '${profile}'. Please use one of these as profile: ${profiles} ")
		}else{
			storedParamsMap.each{entry->
				myMap[entry.key] = entry.value
			}
		}
		map.each{entry->
			myMap[entry.key] = entry.value
		}

		retVal= getFromResource("${profile}.hibernate.cfg.xml",myMap)
		
		if ( profile.startsWith("h2"))
		{
			if (myMap.tcpserver != null )
			{
				Server server = Server.createTcpServer(myMap.tcpserver.split(/[\s]+/)).start()
				serversList << server
			}
			if (myMap.webserver != null )
			{
				String[] args = new String[0];
				if (myMap.webserver != ""){
					args = myMap.webserver.split(/[\s]+/)
				}
				Server server = Server.createWebServer(args).start()
				serversList << server
				if ( !(myMap.noBrowser == true))
				{
					Server.openBrowser(server.getURL())
				}
			}
			
			
		}
		return retVal
	}
	
	public static void waitForServers()
	{
		serversList.each{Server server->
			while (server.isRunning(false))
			{
				sleep(1000)
			}
		}
		serversList =[]
	}
	
	public static Configuration getFromResource(String resourceName, def map)
	{
		// Load the template
		String fullResName = "/org/himalay/qp/"+resourceName
		Class callingClass = ReflectionUtils.getCallingClass(0)
		LOGGER.info "${callingClass} reading ${fullResName}"
		InputStream template = callingClass.getResourceAsStream(fullResName) //QuickPersist.class.getResourceAsStream(fullResName)
		InputStreamReader isr = new InputStreamReader(template)
		SimpleTemplateEngine ste = new SimpleTemplateEngine();
		String configFileText = ste.createTemplate(isr).make(map).toString()
		Configuration cfg = null;

		cfg = new Configuration();
		ByteArrayInputStream bais = new ByteArrayInputStream(configFileText.getBytes())
		XmlParser parser = new XmlParser()
		//DomToGroovy dtg = new DomToGroovy();
		parser.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false);
		parser.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
		//parser.parseText(configFileText)
		File tmpFile = File.createTempFile("qpww",".xml")
		tmpFile.text = configFileText
		org.w3c.dom.Document doc = DomToGroovy.parse(tmpFile )
		cfg.configure(tmpFile);//bais, resourceName)
		tmpFile.delete()
		return cfg
	}

}
