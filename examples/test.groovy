@Grapes([
@GrabConfig(systemClassLoader=true),
@Grab('org.grails:grails-core:3.1.10'                             ),
@Grab('org.grails:grails-datastore-gorm-hibernate4:5.0.4.RELEASE' ),
@Grab('postgresql:postgresql:9.1-901-1.jdbc4'                     ),
@Grab('com.h2database:h2:1.4.192'                                 ),
@Grab('org.javassist:javassist:3.21.0-GA'                         ),
@Grab('org.slf4j:slf4j-log4j12:1.7.14'                            ),
@Grab('org.hsqldb:hsqldb:2.3.4'                                   ),
@Grab('org.apache.commons:commons-lang3:3.5'                      ),
@Grab('org.hibernate:hibernate-core:5.2.5.Final'                  ),
@Grab('org.himalay:quick-persist:0.0.1-SNAPSHOT'                  )]
)

import java.text.SimpleDateFormat

import org.hibernate.Session;
import org.himalay.qp.QuickPersist;
import org.himalay.qp.Table;
import groovy.io.FileType;


def dir = new File(System.properties['java.io.tmpdir'])

SimpleDateFormat sdfYear = new SimpleDateFormat('EEE')
def fileStats = dir.listFiles().collect{ file ->
		String fileType = file.name.split(/\./)[-1]
		
		if ( fileType.length() > 5)
		{
			fileType =""
		}
	    [
			filename: file.name,
			filesize : file.length(),
			filetype: fileType,
			modified: file.lastModified(),
			//creared : file.
			modyear : sdfYear.format(new Date(file.lastModified()))
		]
}

Session sess= QuickPersist.WriteToDB(fileStats, "fileinfo")
groovy.sql.Sql sql = new groovy.sql.Sql(sess.connection())
// Now you can execute sqls
sql.rows("select filetype, avg(filesize) as avgSize from fileinfo group by filetype order by avgsize").each{ it->
	println it
}

// You can also create a two dimentionsional table using two columns
Table table  = new Table("filetype","modyear")
sql.eachRow("select filetype, modyear, count(*) as cnt, avg(filesize) as avgSize, sum(filesize) as total from fileinfo group by filetype,modyear order by avgsize"){row->
	table.add(row)
}
// Print csv
println table.toCSV(){
	return "${it.AVGSIZE}*${it.CNT}"
}

// Print html table
println table.toHTML(table:[id:'myTable', style:'myTableStyle', border:'1']){
	return "${it.AVGSIZE}*${it.CNT}=${it.TOTAL}"
}

sess.close()