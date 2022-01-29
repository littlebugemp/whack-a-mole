import com.resolve.esb.MMsgOptions
import groovy.json.JsonSlurper
import net.sf.json.*;
import groovy.json.JsonOutput;
import groovy.json.JsonBuilder;
import java.util.Random;
import groovy.sql.Sql


def severity = "good"
def condition = "good";
def summary = "Thread Executor : ";
def detail = "";

def includeExclRows = [:]
def CR_NUMBER = INPUTS["CR_NUMBER"]?: "";
def orgName = INPUTS["ORG_NAME"]?:"None";
def precheck = INPUTS["PRECHECK"]?:"";
def IncExcJson = INPUTS["INCLUDE_EXCLUDE_JSON"]?:'';
def userId = INPUTS["USERID"]?:"";
includeExclRows = PARAMS["INCLUDE_EXCLUDE_MAP"]?:"";
def sysID = INPUTS["SYS_ID"]?:""
def hcFlag = "PRE";
def trigger = INPUTS["TRIGGER"]?:"";
cellCountFlag = false
sql = new Sql(DB)
finalJson = ""

try {
  
  summary += CR_NUMBER

  if (IncExcJson != null && IncExcJson.trim() != "") {
	
	def technology = ""
    def jsonSlurper = new JsonSlurper()
    def object1 = jsonSlurper.parseText(IncExcJson)
    includeExclRows = object1.get("IncludeExcludeJSON");
    
    // includeExclRows = assignRank(includeExclRows)

    def list = []
    def includeMap = includeExclRows.findAll {it.U_INCLUDE_EXCLUDE_FLAG.equalsIgnoreCase("true")}
    
    if(includeMap.size()<=0){
    	cellCountFlag = true
    }
    else{	
    	
    
    def vendorSpcMap = includeMap.groupBy{it.U_VENDOR}
    detail += vendorSpcMap //this map will be user for thread opening
    def threadCount = 0
    vendorSpcMap.each {
      vendor,v ->
      

      if (vendor.equalsIgnoreCase("Nokia")) {
      	
      	detail+="\n\n------------------------------------------NOKIA--------------------------------------\n"
      	
      	def teMap = v.groupBy{it.U_TECHNOLOGY}

		  teMap.each{
		  	k0,v0->
		  	// detail += "\nVendor\t$k\nTech\t$k0\n"
		  	// detail+=v0
		  	def lst = []
		  	def cells = []
		  	def btsList=[]
		  	
		  	// bsc_rnc, mgmt_ip, btsnode,
		  	
		  	v0.each{
		  		it->
		  		def tid=""
		  		// detail+="\nk0 is "+k0
		  		// detail += "\n " + returnTechName(k0)
		  		def tecH = returnTechName(k0)
		  		// def tecH = k0
		  		
		  		if(tecH.equalsIgnoreCase("2g"))
		  			tid = it.U_BSC_RNC + "_" + it.U_MGT_STATION_IP.trim()
		  		else if(tecH.equalsIgnoreCase("3g"))
		  			tid = it.U_RNC_ID + "_" + it.U_MGT_STATION_IP + "_" + it.U_BTS_NODE
		  		else if(tecH.equalsIgnoreCase("4g")){
		  			if(it.U_BSC_RNC != null){
		  				tid = it.U_BSC_RNC .replace("/","_")+ "_" + it.U_MGT_STATION_IP
		  				}
		  			}
		  		// else 	
		  			// tid = it.U_RNC_ID + "_" + it.U_MGT_STATION_IP
		  		if(tid != null || tid.trim() != ""){	
		  			it["THREAD_ID"] = tid.trim()
		  			lst.add(tid.trim())
		  			//cells.add(it.U_NETWORK_SECTOR_ID)
		  			btsList.add(it.U_BTS_NODE)
		  		}
		  		
		  		}
		  		
		  		
		  	v0.each {
          		it["THREAD_LIST"] = lst.unique().join(",")
        		}
      		
      		def rncGroup = v0.groupBy {
          		it.THREAD_ID
        		}
        	
        	 threadCount = rncGroup.size()

        	detail += "\nThread Count for $vendor : \t$threadCount "

        rncGroup.each {
          k1,v1 ->
          cells=[];
          v1.each {
          	 cells.add(it.U_NETWORK_SECTOR_ID)
          	}
         
          
          
		//code for nokia 2G
		
		 if(returnTechName(k0).equalsIgnoreCase("2g")){
		 	btsList=[];
		 	v1.each {
		 		 btsList.add(it.U_BTS_NODE)
		 		}
		 	}
			
			
          def params = [: ]

          def rncName = v1[0].U_RNC_ID //rncName
          def ossIp = v1[0].U_MGT_STATION_IP //ossIP
          //def vendor = k // vendor
          def bts = v1[0].U_BTS_NODE
          technology = v1[0].U_TECHNOLOGY
          def operator = v1[0].U_OPERATOR
          def bsc_rnc  = v1[0].U_BSC_RNC
          
          detail += technology
          //if (technology.equalsIgnoreCase("UMTS")) {
            params["OPERATOR"] = operator
            params["BTS"] = bts
            params["VENDOR"] = vendor
            params["TECHNOLOGY"]	=	returnTechName(technology)
            params["PROBLEMID"] = "NEW"
            params["USERID"] = USERID;
            params["WIKI"] = returnRbName(vendor,technology);
            params["RESOLVE.ORG_NAME"] = orgName;
            params["RNC_ID"] = rncName
            params["OSS_IP"] = ossIp
            params["REFERENCE"] = vendor + "-" + returnTechName(technology) + "-" + CR_NUMBER
            //params["INCLUDE_EXCLUDE_JSON"]	=	IncExcJson
            params["CR_NUMBER"] = CR_NUMBER
            params["THREAD_ID"] = v1[0].THREAD_ID.trim()
            params["THREAD_LIST"] = v1[0].THREAD_LIST
            params["HCFLAG"] = hcFlag
            params["SYS_ID"] = sysID
            params["CELLS"] = cells
            params["PRECHECK"] = precheck
            params["TRIGGER"]  =trigger
            params["BSC_NAME"] = bsc_rnc
            params["BTS_LIST"] = btsList.unique()
            // threadCount++;

            list.add(params) //adding map to list

            //	}

          //}
        }	
        		
        		def sqlQuery = "update HUA_NKA_SMG_LDP_TBL set u_" + returnColName(vendor) + "_" + returnTechName(technology) + "_tc = $threadCount, u_" + returnColName(vendor) +"_"+ returnTechName(technology)  + "_rc='0' where sys_id = '$sysID' and u_user = '$USERID'"
      detail += "\nQuery :\t$sqlQuery\nRecord Updated\t"
      detail += sql.executeUpdate(sqlQuery)
        		
		  		
		  	}
      	
      } else if (vendor.equalsIgnoreCase("Samsung")) {
      	
      	detail+="\n\n------------------------------------------Samsung--------------------------------------\n"

        def teMap = v.groupBy{it.U_TECHNOLOGY}
	
	  
		  teMap.each{
		  	k0,v0->
		  	
		  	detail += "\nVendor\t$vendor\nTech\t$k0\n"
		  	
		  	def lst = []
		  	def cells = []
		  	
		  	v0.each{
		  		it->
		  		def tid = it.U_BSC_RNC + "_" + it.U_MGT_STATION_IP
		  		it["THREAD_ID"] = tid.trim()
		  		lst.add(tid)
		  		cells.add(it.U_NETWORK_SECTOR_ID)
		  		}
		  		
		  		
		  	v0.each {
          		it["THREAD_LIST"] = lst.unique().join(",")
        		}
      		
      		def rncGroup = v0.groupBy {
          		it.THREAD_ID
        		}
        		
        	
        	
        	
        	 threadCount = rncGroup.size()

        detail += "\nThread Count for $vendor : \t$threadCount"

        rncGroup.each {
          k1,v1 ->
			
          def params = [: ]

          def bscRnc = v1[0].U_BSC_RNC //rncName
          def ossIp = v1[0].U_MGT_STATION_IP //ossIP
          //def vendor = k // vendor
          def sector = v1[0].U_NETWORK_SECTOR_ID
          technology = v1[0].U_TECHNOLOGY
         def bts = v1[0].U_BTS_NODE
          
          def operator = v1[0].U_OPERATOR
          
          params["NETWORK_SECTOR_ID"] = sector
          params["VENDOR"] = vendor
          params["PROBLEMID"] = "NEW"
          params["USERID"] = USERID;
          params["TECHNOLOGY"]	=	returnTechName(technology)
          params["WIKI"] = returnRbName(vendor,technology);
          params["RESOLVE.ORG_NAME"] = orgName;
          params["BSC_RNC"] = bscRnc
          params["OSS_IP"] = ossIp
          params["REFERENCE"] = vendor + "-" + returnTechName(technology) + "-" + CR_NUMBER
          params["OPERATOR"]	=	operator
          params["CR_NUMBER"] = CR_NUMBER
          params["THREAD_ID"] = v1[0].THREAD_ID.trim()
          params["THREAD_LIST"] = v1[0].THREAD_LIST
          params["HCFLAG"] = hcFlag
          params["SYS_ID"] = sysID
          params["CELLS"] = cells
          params["PRECHECK"] = precheck
          params["TRIGGER"]  =trigger
          params["BTS"]      =bts

          list.add(params) //adding map to list

        }
        
        def sqlQuery = "update HUA_NKA_SMG_LDP_TBL set u_" + returnColName(vendor) + "_" + returnTechName(technology) + "_tc = $threadCount, u_" + returnColName(vendor) +"_"+ returnTechName(technology)  + "_rc='0' where sys_id = '$sysID' and u_user = '$USERID'"
      detail += "\nQuery :\t$sqlQuery\nRecord Updated\t"
      detail += sql.executeUpdate(sqlQuery)
        
      }

      } else if (vendor.equalsIgnoreCase("Huawei")) {
      	detail+="\n\n------------------------------------------Huawei--------------------------------------\n"

      	def teMap = v.groupBy{it.U_TECHNOLOGY}
	  
		teMap.each{
		  	tech,v5->
		  	
		  	//new code 4-8-2021 --- addition of huawei 5g ee/h3g logic
		  	if(tech.equalsIgnoreCase("5g")){ //code for huawei 5g
		  	
		  			def opGroup = v5.groupBy {it.U_OPERATOR}
		  			opGroup.each{
		  				operator,values->
		  				def lst = []
		  				// detail += "\n$operator\nValues Size " + values.size()
		  				detail += "\nThread Count for $vendor $operator $tech "
		  				
		  				values.each{
			  				it->
					  		def tid = it.U_BSC_RNC +"_"+it.U_OPERATOR +"_"+ it.U_MGT_STATION_IP + "_" + it.U_TECHNOLOGY.toLowerCase()
					  		it["THREAD_ID"] = tid.trim()
					  		lst.add(tid.trim())
		  				}
		  				values.each {it["THREAD_LIST"] = lst.unique().join(",")}
		  				
		  				def rncGrp = values.groupBy {it.THREAD_ID}
		  				
		  				threadCount = rncGrp.size()
		  				
		  				detail += threadCount
		  				
		  				rncGrp.each{
		  					thread_id,val->
		  					
		  					def cells = []
		  					
		  					val.each{cells.add(it.U_NETWORK_SECTOR_ID)}
		  					
		  					// detail += "\nCells $cells"
		  					
							def params = [:]

					        def bscRnc	= 	val[0].U_BSC_RNC //rncName
					        def ossIp	= 	val[0].U_MGT_STATION_IP //ossIP
					        def sector	= 	val[0].U_NETWORK_SECTOR_ID
					          
					          params["NETWORK_SECTOR_ID"]	=	sector
					          params["VENDOR"]				=	vendor
					          params["PROBLEMID"]			=	"NEW"
					          params["USERID"]				=	USERID;
					          params["WIKI"]				=	returnRbName(vendor,tech)
					          params["TECHNOLOGY"]			=	returnTechName(tech)
					          params["RESOLVE.ORG_NAME"]	=	orgName;
					          params["BSC_RNC"]				=	bscRnc
					          params["OSS_IP"]				=	ossIp
					          params["REFERENCE"]			=	vendor + "-" + returnTechName(tech) + "-" + CR_NUMBER
					          //params["INCLUDE_EXCLUDE_JSON"]	=	IncExcJson
					          params["CR_NUMBER"]			=	CR_NUMBER
					          params["THREAD_ID"]			= 	val[0].THREAD_ID.trim()
					          params["THREAD_LIST"]			=	val[0].THREAD_LIST
					          params["HCFLAG"]				=	hcFlag
					          params["SYS_ID"]				=	sysID
					          params["CELLS"]				=	cells
					          params["PRECHECK"]			=	precheck
					          params["TRIGGER"]				=	trigger
							  params["OPERATOR"]			=	operator
					          list.add(params)
		  					
		  					}
		  					
		  					def sqlQuery = "update HUA_NKA_SMG_LDP_TBL set u_" + returnColName(vendor) +"_"+operator.toLowerCase()+"_" + returnTechName(tech) + "_tc = $threadCount, u_" + returnColName(vendor) +"_"+operator.toLowerCase()+"_"+ returnTechName(tech)  + "_rc='0' where sys_id = '$sysID' and u_user = '$USERID'"
      						detail += "\nQuery :\t$sqlQuery\nRecord Updated\t"
      						detail += sql.executeUpdate(sqlQuery) 
		  				}
		  		
		  		}else{ //code for 2g, 4g technology
				  	def lst = []
				  	// def cells = []
				  	
				  	v5.each{
				  		it->
				  		// def tid = it.U_BSC_RNC +"_" + it.U_MGT_STATION_IP + "_" + it.U_OPERATOR + "_" + it.U_TECHNOLOGY.toLowerCase()
				  		def tid = it.U_BSC_RNC +"_" + it.U_MGT_STATION_IP + "_" + it.U_TECHNOLOGY.toLowerCase()
				  		it["THREAD_ID"] = tid.trim()
				  		lst.add(tid.trim())
				  		// cells.add(it.U_NETWORK_SECTOR_ID)
				  		}
				  	v5.each {it["THREAD_LIST"] = lst.unique().join(",")}
		      		def rncGroup = v5.groupBy {it.THREAD_ID}
		        	threadCount = rncGroup.size() //threadcount
        			detail += "\nThread Count for $vendor $tech : \t$threadCount" 
        
        			rncGroup.each {
          				k1,v1 ->
          		
          			def cells = []
          			v1.each{cells.add(it.U_NETWORK_SECTOR_ID)}
          			// cells.add(it.U_NETWORK_SECTOR_ID)
          				def params = [: ]
          				
          				def sector = v1[0].U_NETWORK_SECTOR_ID
						
						if(tech.equalsIgnoreCase("2g"))
							sector = v1[0].U_CELL_ID 
						
				          def bscRnc = v1[0].U_BSC_RNC //rncName
				          def ossIp = v1[0].U_MGT_STATION_IP //ossIP
				          
				          technology	=	v1[0].U_TECHNOLOGY
				          operator		=	v1[0].U_OPERATOR
				          params["NETWORK_SECTOR_ID"] = sector
				          params["VENDOR"] = vendor
				          params["PROBLEMID"] = "NEW"
				          params["USERID"] = USERID;
				          params["WIKI"] = returnRbName(vendor,technology);
				          params["TECHNOLOGY"]	=	returnTechName(technology)
				          params["RESOLVE.ORG_NAME"] = orgName;
				          params["BSC_RNC"] = bscRnc
				          params["OSS_IP"] = ossIp
				          params["REFERENCE"] = vendor + "-" + returnTechName(technology) + "-" + CR_NUMBER
				          //params["INCLUDE_EXCLUDE_JSON"]	=	IncExcJson
				          params["CR_NUMBER"] = CR_NUMBER
				          params["THREAD_ID"] = v1[0].THREAD_ID.trim()
				          params["THREAD_LIST"] = v1[0].THREAD_LIST
				          params["HCFLAG"] = hcFlag
				          params["SYS_ID"] = sysID
				          params["CELLS"] = cells
				          params["PRECHECK"] = precheck
				          params["TRIGGER"]  =trigger
						  params["OPERATOR"]	=	operator
				          list.add(params) //adding map to list
          				}
          				def	sqlQuery = "update HUA_NKA_SMG_LDP_TBL set u_" + returnColName(vendor) +"_" + returnTechName(technology) + "_tc = $threadCount, u_" + returnColName(vendor) +"_"+ returnTechName(technology)  + "_rc='0' where sys_id = '$sysID' and u_user = '$USERID'"
      					detail += "\nQuery :\t$sqlQuery\nRecord Updated\t"
      					detail += sql.executeUpdate(sqlQuery)
		  			}
        }
        
      } else {

        detail += "Invalid Technology"
      }

      

    }

    }
    
    def t = 1
    def tidMap = [:]
    list.each{
    	it["TID"] = "T$t"
    	tidMap[it["TID"]] = "0"
    	t++
    	}
    
    detail += "\n\n--------Final Map--------\n" + list.join("\n") + "\n\n"
    finalJson = new JsonBuilder(list).toPrettyString()
    detail += finalJson
	
	detail += "\n\n\nTID MAP : $tidMap"
    def statData = new JsonBuilder(tidMap)
    def sqlQueryforStat = "update HUA_NKA_SMG_LDP_TBL set u_hua_5g_tc='" +statData+"' where sys_id = '" +sysID+ "'"
	detail += "\nQuery\n$sqlQueryforStat\nRecords Updated --> "+sql.executeUpdate(sqlQueryforStat)
   
    // detail += assignRank(IncExcJson)
    
  } else {
    detail += "No input provided for include exclude Json"
    // throw new Exception("No input provided for include exclude Json")
  }
} catch (Exception e) {
  condition = "bad";
  severity = "critical";
  summary = 'Exception Thrown : ' + e;
  detail += "<==FAIL==> " + e.getClass() + ": " + e.getMessage();
  for (def trace in e.getStackTrace()) {
    detail += "\n\t" + trace;
  }
  def nested = e.getCause();
  while (nested != null) {
    detail += "\ncaused by " + nested.getClass() + ": " + nested.getMessage();
    for (trace in nested.getStackTrace()) {
      detail += "\n\t" + trace;
    }
    nested = nested.getCause();
  }
}finally{
	sql.close()
	 OUTPUTS["FINAL_JSON"] = finalJson
    OUTPUTS["MAINJSON"] = assignRank(IncExcJson)
    OUTPUTS["CELLCOUNTFLAG"] = cellCountFlag
	}

def returnRbName(s,t) {
	t	=	returnTechName(t).toLowerCase()
	// detail += t
  def rbMap = ["nokia":["3g":"MBNL_MO_OF_UK.Nokia_3G_LDP_RB_PrePost_Check_Analysis","2g":"MBNL_MO_OF_UK.Nokia_2G_LDP_RB_PrePost_Check_Analysis","4g":"MBNL_MO_OF_UK.Nokia_4G_LDP_RB_PrePost_Check_Analysis"] , "huawei": ["5g":"MBNL_MO_OF_UK.Huawei_5G_LDP_RB_PrePost_Check_Alarm_Analysis","2g":"MBNL_MO_OF_UK.Huawei_2g_LDP_RB_PrePost_Check_Alarm_Analysis","4g":"MBNL_MO_OF_UK.Huawei_4G_LDP_RB_PrePost_Check_Alarm_Analysis"], "samsung": ["2g":"","3g":"","4g":"MBNL_MO_OF_UK.Samsung_4G_LDP_RB_PrePost_Check_Alarm_Analysis"]]
  return (rbMap.get(s.toLowerCase())==null)?"Value not present":(rbMap.get(s.toLowerCase()).get(t)==null)?"value not present":rbMap.get(s.toLowerCase()).get(t)

}

def returnColName(s) {
  def m = ["NOKIA": "nka", "SAMSUNG": "smg", "HUAWEI": "hua"]
  return m.get(s.toUpperCase())

}

def returnTechName(s) {
	s= s.toLowerCase()
	
	def map = ["gsm":"2g","umts":"3g","lte":"4g","nr":"5g"]
	
	// return map.get(s.toLowerCase())
	return s
	
	}
	
def assignRank(IncExcJson){
	def jsonSlurper = new JsonSlurper()
	def object1 = jsonSlurper.parseText(IncExcJson)
    def map = object1.get("IncludeExcludeJSON");
	map.each{
		it->
		def tech = returnTechName(it.U_TECHNOLOGY)+it.U_OPERATOR.trim().toLowerCase()
		it["rank"] = getRank(tech)
		}
		
	def newMap = [:]
	
	newMap["IncludeExcludeJSON"] = map
	return new JsonBuilder(newMap).toPrettyString()
	}	
	
	
def getRank(s){
	
	def m = ["2gee":1,"3gee":2,"4gee":3,"5gee":4,"2gh3g":5,"3gh3g":6,"4gh3g":7,"5gh3g":8]
	
	return m.get(s)
	
	}	

RESULT.condition = condition;
RESULT.severity = severity;
RESULT.summary = summary;
RESULT.detail = detail;