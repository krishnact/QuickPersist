package org.himalay.qp

import java.io.File

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException

import org.codehaus.groovy.reflection.ReflectionUtils;
import org.codehaus.groovy.tools.xml.DomToGroovy;
import org.h2.mvstore.cache.CacheLongKeyLIRS.Config;
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
	/**
	 * Insert into database when you already have Objects of entity Class
	 * @param objects
	 * @return
	 */
	public static Session writeIntoDatabase(def objects, Class[] classes, File hibernateConfigFile = null, boolean clean = true)
	{
		
		Configuration cfg = null;
		SessionFactory sessionFactory = null
		cfg = getHibernateConf(hibernateConfigFile)
		
		classes.each{
			cfg.addAnnotatedClass(it)
		}

		sessionFactory =   cfg.buildSessionFactory();
		Session session = sessionFactory.openSession();

		session.beginTransaction();
		objects.each {
			//println "Inserting $it"
			session.saveOrUpdate(it);
		}
		session.getTransaction().commit();
		if ( clean == true)
		{
			session.close()
			sessionFactory.close()
		}
		return session;
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

	public static void writeIntoDatabaseLite(ClassBuilder cb,def objects, File hibernateConfigFile=null)
	{
		
		Configuration cfg = null;
		SessionFactory sessionFactory = null
		cfg = getHibernateConf(hibernateConfigFile)
		cb.loadedClasses.each{
			cfg.addAnnotatedClass(it)
		}
		sessionFactory =   cfg.buildSessionFactory();
		Session session = sessionFactory.openSession();

		session.beginTransaction();
		objects.each {
			//println "Inserting $it"
			session.save(it);
		}
		session.getTransaction().commit();
	}
	
	
	public static Session writeIntoDatabaseLite(ClassBuilder cb,def objects, Configuration cfg)
	{
		
		SessionFactory sessionFactory = null
		cb.loadedClasses.each{
			cfg.addAnnotatedClass(it)
		}
		sessionFactory =   cfg.buildSessionFactory();
		Session session = sessionFactory.openSession();

		session.beginTransaction();
		objects.each {
			//println "Inserting $it"
			session.save(it);
		}
		session.getTransaction().commit();
		return session
	}
	
	/**
	 * Insert into database when you already have Objects of entity Class
	 * @param objects
	 * @return
	 */
	public static Session writeIntoDatabaseLite(def objects, File hibernateConfigFile = null, boolean clean = true)
	{
		return writeIntoDatabase(objects, [objects[0].class], hibernateConfigFile, clean)
	}

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
	public static Session WriteToDB(def arrayOfObjects, String objectTypeName, Configuration hibernateCfg = QuickPersist.getHibernateConf("h2mem"), boolean printClasses=false)
	{
		ClassBuilder cb = WriteToDBPart1(arrayOfObjects, objectTypeName, printClasses)
		// Now create Objects
		def objs = cb.createObjects(objectTypeName,arrayOfObjects)

		Session retVal = writeIntoDatabaseLite(cb, objs, hibernateCfg)
		return retVal
	}
	

	public static Configuration getHibernateConf(String profile, def map = null)
	{
		
		Configuration retVal = null
		switch (profile){
			case "h2local" :
				if ( map == null)
				{
					map =[host:'127.0.0.1', port: 3743,dbPath:"quick_persist",showSQL: false]
				}
				retVal= getFromResource("h2.tcp.hibernate.cfg.xml",map)
				break;
			case "h2file" :
				if ( map == null)
				{
					map =[host:'127.0.0.1', port: 3743,dbPath:"quick_persist",showSQL: false]
				}
				retVal= getFromResource("h2.file.hibernate.cfg.xml",map)
				break;
			case "h2mem" :
				if ( map == null)
				{
					map =[dbPath:"quick_persist",showSQL: false]
				}
				retVal= getFromResource("h2.memory.hibernate.cfg.xml",map)
				break;
			case "hsqldbmem" :
				if ( map == null)
				{
					map =[dbPath:"quick_persist"]
				}
				retVal= getFromResource("hsqldb.memory.hibernate.cfg.xml",map)
				break;
			
			default :
				break;
		}
		
		return retVal
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
