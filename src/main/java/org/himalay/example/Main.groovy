package org.himalay.example

import org.himalay.qp.ClassBuilder
import org.himalay.qp.QuickPersist
import org.himalay.qp.Table;

import groovy.json.JsonSlurper;
import org.hibernate.Session
class Main {
	public static void main(String[] args)
	{
		def tt= [[key:"key",value:'val', updatedAt:new Date()]]
//		ClassBuilder cb = new ClassBuilder("org.himalay.ss.persist");
//		cb.analyze(tt, "CachedItem")
//		cb.createClassesText();
//		cb.printClassesText()
//		System.exit(0)
		testH2Mem()
//		testH2File()
//		testHSQLDBMem()
		
		System.exit(0)
	}
	
	public static testH2Mem()
	{

		Session sess= QuickPersist.WriteToDB(getData(), "people")
		groovy.sql.Sql sql = new groovy.sql.Sql(sess.connection())
		sql.rows("select * from people").each{row->
			println row
		}
		
		Table table = new Table("type_","gender")
		sql.rows("select type_, gender, count(*) cnt, avg(weight) as weight from people group by type_, gender").each{row->
			table.add(row)
		}
		sess.close()
		
		println table;
		println table.toHTML(
			[
				table:[id:'myTable', style:'myTableStyle', border:'1'],
				
			]){Map it->
			it.collect{"${it.key}=${it.value}"}.join(",")
		};
	}
	
	public static testH2File()
	{
		Session sess= QuickPersist.WriteToDB(getData(), "people", QuickPersist.getHibernateConf("h2file",[dbPath:"./qp"]))
		groovy.sql.Sql sql = new groovy.sql.Sql(sess.connection())
		sql.rows("select * from people").each{row->
			println row
		}
		
		sql.rows("select type_, gender, count(*) cnt, avg(weight) as weight from people group by type_, gender").each{row->
			println row
		}
		sess.close()
	}
	
	public static testHSQLDBMem()
	{
		Session sess= QuickPersist.WriteToDB(getData(), "people", QuickPersist.getHibernateConf("hsqldbmem"))
		groovy.sql.Sql sql = new groovy.sql.Sql(sess.connection())
		sql.rows("select * from people").each{row->
			println row
		}
		
		sql.rows("select type_, gender, count(*) cnt, avg(weight) as weight from people group by type_, gender").each{row->
			println row
		}
		sess.close()
	}
	public static def getData()
	{
		// For those in a hurry...
		def tableWithData = [
				[name:"Peter" , place: "USA"    , gender :'M', weight: 167.2 , lastSeen: new Date(), type_:'student'],
				[name:"Raghaw", place: "India"  , gender :'M', weight: 150 , lastSeen: new Date(), type_:'student'],
				[name:"Ming"  , place: "China"  , gender :'F', weight: 120 , lastSeen: new Date(), type_:'worker' ],
				[name:"Hofer" , place: "Hungary", gender :'M', weight: 180 , lastSeen: new Date(), type_:'unemployed'],
				[name:"Nomura", place: "Japan"  , gender :'M', weight: 120 , lastSeen: new Date(), type_:'worker' ],
				[name:"Boris" , place: "Russia" , gender :'M', weight: 160 , lastSeen: new Date(), type_:'worker'],
				[name:"Nikita", place: "Russia" , gender :'F', weight: 130 , lastSeen: new Date(), type_:'worker'],
			]
		return tableWithData
	}
}
