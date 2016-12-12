

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

println "Session closed"