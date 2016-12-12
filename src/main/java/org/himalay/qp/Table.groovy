package org.himalay.qp

import java.sql.ResultSetMetaData;

import groovy.sql.GroovyResultSet;
import groovy.sql.GroovyRowResult;

class Table {
	def table = [:].withDefault {[:]}
	
	String row;
	String col;
	public Table(String row, String col, boolean useUpperCase = true) {
		super();
		this.row = row;
		this.col = col;
		if (useUpperCase)
		{
			this.row = this.row.toUpperCase()
			this.col = this.col.toUpperCase()
		}
	}
	
	public void add(GroovyRowResult  aRow)
	{
		Object colVal = aRow.get(col)
		Object rowVal = aRow.get(row)
		def cellVal = aRow.findAll{!( it.key == col || it.key == row)}.collectEntries {
			if (!( it.key == col || it.key == row))
			{
				return [it.key,  it.value]
			}
		}
		
		table[rowVal][colVal] = cellVal
	}

	public void add(GroovyResultSet aRow)
	{
		Object colVal = aRow.getAt(col)
		Object rowVal = aRow.getAt(row)
		
		ResultSetMetaData mt = aRow.getMetaData();
		int cols = mt.getColumnCount()
		def columns = (1..cols).collect{
			mt.getColumnName(it)
		}.findAll{!( it == col || it == row)}
		
		def cellVal = [:] 
		columns.each{colName ->
			cellVal[colName]= aRow.getAt(colName)
		}

		table[rowVal][colVal] = cellVal
	}
	public void add(Map  aRow)
	{
		Object colVal = aRow.get(col)
		Object rowVal = aRow.get(row)
		def cellVal = aRow.findAll{!( it.key == col || it.key == row)}.collectEntries {
			if (!( it.key == col || it.key == row))
			{
				return [it.key,  it.value]
			}
		}
	
		table[rowVal][colVal] = cellVal
	}
	
	
	public String toString()
	{
		// Find all columns
		def columns = table.values().collectMany{
			it.keySet()
		}.unique()
		
		// Print header
		String retVal = "," + columns.join(",") +"\n"
		retVal += table.collect{ it->
			it.key +"," +columns.collect{col-> 
				it.value[col]
			}.join(",")
		}.join("\n")
		
	}
	
	public String toCSV(Closure eachCell=null)
	{
		// Find all columns
		def columns = table.values().collectMany{
			it.keySet()
		}.unique()
		
		// Print header
		String retVal = "," + columns.join(",") +"\n"
		retVal += table.collect{ it->
			it.key+"," + columns.collect{col->
				if ( eachCell == null)
				{
					it.value[col].toString()
				}else{
					def cellVal = it.value[col]
					if ( cellVal != null){
						eachCell(cellVal)
					}else{
						""
					}
				}
			}.join(",")
		}.join("\n")
		
	}
	public String toHTML(Map attributes, Closure eachCell=null)
	{
		def tagAttributes = [:].withDefault {""}
		attributes.each { tagAttr->
			String tagName = tagAttr.key
			Map    attr    = tagAttr.value
			String attrStr = attr.collect{
				"${it.key}='${it.value}'"
			}.join(" ")
			
			tagAttributes[ tagName]= attrStr
		}
		// Find all columns
		def columns = table.values().collectMany{
			it.keySet()
		}.unique()
		
		// Print header
		def  tags = []
		tags << "table"  
		String retVal = "<table ${tagAttributes[tags.last()]}>"
		retVal +="<thead><tr><td></td><td>" + columns.join("</td><td>") +"</td></tr></thead>"
		retVal += "<tbody>"
		int trIdx = 1;
		retVal += table.collect{ it->
			int tdIdx = 1;
			"<tr id='${trIdx}'><td>"+
			it.key +"</td><td>" +columns.collect{col->
				if ( eachCell == null)
				{
					it.value[col].toString()
				}else{
					def cellVal = it.value[col]
					if ( cellVal != null){
						eachCell(cellVal)
					}else{
						""
					}
				}
			}.join("</td><td>") +"</td></tr>" 
		}.join("") ;
		retVal += "</tbody>";
		retVal +="</table>"
		return retVal
	}
}
