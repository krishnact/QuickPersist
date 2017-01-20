package org.himalay.qp



import java.lang.annotation.RetentionPolicy
import java.lang.reflect.Field;


import org.codehaus.groovy.util.StringUtil
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration
import org.slf4j.LoggerFactory;;
//@java.lang.annotation.Retention(RetentionPolicy.RUNTIME)
public class ClassBuilder {
	static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(QuickPersist.class) 
		GroovyClassLoader parser
		def classList
		String packageName
		def createdClasses 
		@SuppressWarnings("rawtypes")
		List<Class> loadedClasses;
		int pos = 0
		String idColName = "id_"
		/**
		 * If a class has less then this number of fields then it will be an embeddable class
		 */
		int embeddableThresold =4

		/**
		 * Analyzes a list of objects to create class metadata. Next Step is to create classesText
		 * @param listOfMaps
		 * @param className
		 */
		public void analyze(def listOfMaps, String className)
		{
			ClassMetadata topLevel = new ClassMetadata()
			topLevel.setName(className)
			listOfMaps.each{
				topLevel.analyze(it)
			}
		}
	


		private Object createClassesText()
		{
			def retVal = []
			classList.sort{	t1,t2 -> 
					-1*t1.value.metadata.pos
			}.each{String classFullName, it->
				retVal.push ( [name : classFullName, classText: it.metadata.createClassText() ] )
			}
			this.createdClasses = retVal
			return retVal
		}
		
		public Object createObjects(String clazzName, def listOfMaps)
		{
			return createObjects(clazzName,listOfMaps,this.packageName)
		}	
		
		public Object createObjects(String clazzName, def listOfMaps, String packageName)
		{
			String fullName = "${packageName}.${clazzName}"
			ClassMetadata cm = this.classList[fullName].metadata
			def	retVal = listOfMaps.collect{
				cm.createObject(it)
			}
			
			return retVal ;
		}
//		public static Object objectFromClass(String className, def map)
//		{
//			Object obj = Class.forName().newInstance()
//			map.each{key,value->
//				if (value.getClass().getName() == 'java.util.LinkedHashMap')
//				{
//					obj."$key" = objectFromClass()
//				}else{
//					obj."$key" = value
//				}
//			}
//			return obj
//		}
				
		public List<Class> findNeededClasses(Class aClass)
		{
			
			
			def neededClasses = [aClass]
			def declaredClasses = aClass.getDeclaredFields().collect{Field it -> 
				return 	it.type
				};
			declaredClasses = declaredClasses.findAll{Class it -> it.isAnnotationPresent(javax.persistence.Entity.class)};
			declaredClasses = declaredClasses.findAll{!neededClasses.contains(it)}.each{Class aClz ->
				neededClasses += findNeededClasses(aClz);
			}
			
			return neededClasses
		}

		public List<Class> loadClasses()
		{
			def retVal = []
			this.createdClasses.each{ it ->
				LOGGER.info "Loading ${it.name}"
				String classText =  it.classText
				Class clazz = parser.parseClass(classText)
				classList[it.name]["clazz"] = clazz
				retVal.push(clazz)
			}

			
			return retVal;
		}
		
		ClassBuilder(String packName)
		{
			parser =new GroovyClassLoader()
			this.classList = [:]
			this.packageName = packName
			createdClasses = []
			//classOrder = []
		}
		class ClassMetadata
		{
			String name
			def imports
			def fields
			def methods
			def annotation
			def attributes

			def ClassMetadata() {
				imports = []
				annotation = [:]
				fields = [:]
				methods = [:]
				attributes = [:]
			}
		
			def setName(String name) {
				this.name = name
			}
		
			def addImport(String importClass) {
				imports << "${importClass}" 
			}
		
			private String getJavaName(String originalName)
			{
				return originalName.replaceAll(/[^A-Za-z0-9_]/, "_")
			}
			void addField(String name, Class type) {

					fields[getJavaName(name)] = type.name
					annotation[getJavaName(name)] = ""
			}
			
			void addField(String name, String type, String annot) {
					fields[getJavaName(name)] = type
					annotation[getJavaName(name)] = annot
			}
			
			def addFieldList(String name, String type, String annot) {
				fields[getJavaName(name)] = "List<${type}>"
				annotation[getJavaName(name)] = annot
			}
		
			def addMethod(String name, Closure closure) {
				methods[getJavaName(name)] = closure
			}
			def addPackage(String pack)
			{
				this.packageName = pack
			}
			
			String fullName()
			{
				return packageName + "." + this.name
			}
			def createClassText() {
				
				def templateText = '''
package $packageName
<%imports.each {%>import $it\n <% } %> 
import groovy.transform.ToString;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Embeddable;
import javax.persistence.GeneratedValue;
import javax.persistence.CascadeType;
import javax.persistence.OneToOne;
import javax.persistence.OneToMany;
import javax.persistence.ManyToOne;
import javax.persistence.ElementCollection  ;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity ${attributes['embeddable']}
@ToString
public class $name
{
@Id 
@GeneratedValue
long ${idColName}
<%fields.each {%> ${annotation[it.key]}   $it.value $it.key \n<% } %>
}
'''
				def data = [name: name, attributes: attributes,imports: imports, fields: fields, packageName : packageName, annotation: annotation, idColName: idColName]
		
				def engine = new groovy.text.SimpleTemplateEngine()
				def template = engine.createTemplate(templateText)
				def result = template.make(data)
				//println result
				//Class cls = parser.parseClass(result.toString())
//				methods.each {
//					cls.metaClass."$it.key" = it.value
//				}
				return result.toString()
			}
			
			public void analyze(Object obj)
			{
				Class clazz = null
				// if we can create an instance of this class then do it.
				if(classList["${packageName}.${name}"] == null)
				{
					classList["${packageName}.${name}"] = [metadata: this, position : pos++]
					//classOrder = ["${packageName}.${name}"]+classOrder
				}
				obj.each{key,value->
					
					if (value.getClass().getName() == 'java.util.ArrayList'){
						Object val = value.getAt(0)
						if ( val.getClass().getName() == 'java.util.LinkedHashMap' )
						{
							String possibleName = org.apache.commons.lang3.text.WordUtils.capitalizeFully(key)
							String possibleClassName = "${packageName}.${possibleName}"
							ClassMetadata cb = classList[possibleClassName]?.metadata
							if (cb == null)
							{
								cb = new ClassMetadata()
							}
							cb.setName(possibleName)
							cb.analyze(val)
							addFieldList(key, possibleClassName,"@OneToMany(fetch=FetchType.EAGER,cascade=CascadeType.ALL)")
							addImport(possibleClassName)
						}else{
							addFieldList(key, val.getClass().getName(),"@ElementCollection  ")
						}
	
					}
					else if (value.getClass().getName() == 'java.util.LinkedHashMap')
					{
						String possibleName = WordUtils.capitalizeFully(key)
						String possibleClassName = "${packageName}.${possibleName}"
						ClassMetadata cb = classList[possibleClassName]?.metadata
						if (cb == null)
						{
							cb = new ClassMetadata()
						}  
						cb.setName(possibleName)
						cb.analyze(value)
						addField(key, possibleClassName,"@OneToOne(cascade=CascadeType.ALL)")
						addImport(possibleClassName)
					} else if (value.getClass().getName() == 'java.lang.String'){
						addField(key, value.getClass().getName(),"@Column( length = 500000)")
					} else if (value != null){
						addField(key, value.getClass())
					}
				}
				if ( this.fields.size() < embeddableThresold)
				{
					this.attributes["embeddable"]= "@Embeddable"
				}else{
					this.attributes["embeddable"]= ""
				}
			}
	
			public Object createObject(Object obj)
			{
				String classFullName = fullName()
				Object retVal  = classList[classFullName]["clazz"].newInstance()
				obj.each{key,value->
					if (value.getClass().getName() == 'java.util.ArrayList'){
						Object val = value.getAt(0)
						if ( val.getClass().getName() == 'java.util.LinkedHashMap' )
						{
							String fullName = packageName + "." + WordUtils.capitalizeFully(key)
							ClassMetadata cmdt = classList[fullName].metadata
							
							//Class memberClass = Class.forName(createdClasses[fullName()]["clazz"]).newInstance()
							def memVal = value.collect{ it ->
								def aVal = cmdt.createObject(it)
								return aVal
							}
							retVal."$key" = memVal;
						}else{
							retVal."$key" = value;
						}
	
					}
					else if (value.getClass().getName() == 'java.util.LinkedHashMap')
					{
						Object memberObj = retVal
						value.each{key1,val->
							if ( val.getClass().getName() == 'java.util.LinkedHashMap' )
							{
								String fullName = packageName + "." + WordUtils.capitalizeFully(key)
								ClassMetadata cmdt = classList[fullName].metadata
								
								retVal."$key" = cmdt.createObject(val);
							}else{
								retVal."$key" = value;
							}
						}
					}else{
						retVal."$key" = value;
					}
				}
				
				return retVal
			}
	
		}
		
		public void printClassesText()
		{

			this.createdClasses.each{
					LOGGER.info "//" + it.name
					LOGGER.info it.classText
				}
		}
		
		
		public static Object createObjectStatic(String clazzName, def aMap, String packageName)
		{
			String classFullName = "${packageName}.${clazzName}"
			Object retVal  = Class.forName(classFullName).newInstance();
			aMap.each{key,value->
				if (value.getClass().getName() == 'java.util.ArrayList'){
					Object val = value.getAt(0)
					if ( val.getClass().getName() == 'java.util.LinkedHashMap' )
					{
						String memberClazzName = WordUtils.capitalizeFully(key)
					
						//Class memberClass = Class.forName(createdClasses[fullName()]["clazz"]).newInstance()
						def memVal = value.collect{ it ->
							def aVal = createObjectStatic(memberClazzName, it, packageName)
							return aVal
						}
						retVal."$key" = memVal;
					}else{
						retVal."$key" = value;
					}

				}
				else if (value.getClass().getName() == 'java.util.LinkedHashMap')
				{
					Object memberObj = retVal
					value.each{key1,val->
						if ( val.getClass().getName() == 'java.util.LinkedHashMap' )
						{
							String memberClazzName = WordUtils.capitalizeFully(key)
							retVal."$key" = createObject(memberClazzName,val, packageName);
						}else{
							retVal."$key" = value;
						}
					}
				}else{
					retVal."$key" = value;
				}
			}
			
			return retVal
		}
	
}

