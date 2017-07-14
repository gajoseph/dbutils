/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import org.neo4j.driver.v1.*;

import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author TGAJ2 Copy postgres data to graph Db
 */
public class neo4cp extends idrive {

    protected iCopy licopy;
    protected org.neo4j.driver.v1.Driver objDesloc = null;
    protected Session session = null;
    protected String sLinks = "";
    protected String sChld = "";
    protected iDataloc objSrcloc;

    public String SRCdbDriver;
    public String SRC_DB_URL;
    public String SRC_DB_USER;
    public String SRC_DB_PASSWORD;
    
    public String DESdbDriver;
    public String DES_DB_URL;
    public String DES_DB_USER;
    public String DES_DB_PASSWORD;

    public ischema objfrmSchema;// = new ischema();
    public ischema objToSchema;// = new ischema();

    public Dbtables objDBts;

    protected List<String> itabpks;

    java.sql.ResultSet objSelrs = null;
    java.sql.PreparedStatement _selStatement = null;

    //public _selResultSet insert resultset 
    public neo4cp(String sSrcUrl, String sDesUrl) {

        // Get the URL from Propert file 
        SRC_DB_URL = lPropertyReader.getProperty("SRC.DB.URL");
        DES_DB_URL = lPropertyReader.getProperty("DES.DB.URL");

        SRC_DB_USER = lPropertyReader.getProperty("SRC.DB.USER");
        SRC_DB_PASSWORD = lPropertyReader.getProperty("SRC.DB.PASSWORD");

        DES_DB_USER = lPropertyReader.getProperty("DES.DB.USER");
        DES_DB_PASSWORD = lPropertyReader.getProperty("DES.DB.PASSWORD");

        // Get Db driver info property file        
        SRCdbDriver = lPropertyReader.getProperty("SRC.dbDriver");
        DESdbDriver = lPropertyReader.getProperty("DES.dbDriver");

        //Initaite Src Db location object    
        objSrcloc = new iDataloc(SRC_DB_URL, SRCdbDriver, SRC_DB_USER, SRC_DB_PASSWORD);
        _connSrc = objSrcloc._conn;

        //Initaite Des Db location object 
        
        objDesloc = GraphDatabase.driver(DES_DB_URL, AuthTokens.basic(DES_DB_USER, DES_DB_PASSWORD));
        session = objDesloc.session();

        //  initiate the schema Objects 
        objfrmSchema = new ischema();
        objToSchema = new ischema();

        // initiate the dbtables object 
        objDBts = new Dbtables();

        // initailzie List<String>
        itabpks = new ArrayList<String>();
    }

    protected void finalize() throws Throwable {
        try {
            objSrcloc.finalize();
            objDesloc.close();
            objfrmSchema.finalize();
            objToSchema.finalize();

        } catch (Exception e) {
            lSumBJCLogger.WriteErrorStack("Finalize Exception", e);

        } catch (Error e) {
            lSumBJCLogger.WriteErrorStack("Finalize Error ", e);

        } finally {
            // objIns = null;
            _selStatement = null;

            objSrcloc = null;
            objDesloc = null;

            super.finalize();
        }
    }

    public void GetDBPostgresmeta(String strFrmSchema, String strtoSchema) throws SQLException {
        try {
            lSumBJCLogger.WriteLog("Source conn1");
            getTabBySchema(strFrmSchema, strtoSchema);

        } catch (Error e) {
            lSumBJCLogger.WriteErrorStack("Error in ", e);
        }
    }

    public void getTabBySchema(String sFrmSchema, String sToSchema) {
        try {
            // Get the table list in a schema 
            objSelrs = objSrcloc._conn
                                .getMetaData()
                                .getTables(null, sFrmSchema, null, new String[]{"TABLE"});

            objfrmSchema.setName(sFrmSchema);
            objToSchema.setName(sToSchema); /// to chnage chnaged 

            objDBts.objFrmSchema = objfrmSchema;
            objDBts.objToSchema = objToSchema;
            objDBts.conn1 = objSrcloc._conn;

            while (objSelrs.next()) {
                
                objDBts.getTabDetails(sFrmSchema, sToSchema, objSelrs.getString(3));

                lSumBJCLogger.WriteLog("table = "   + objSelrs.getString(3) + " 2 " 
                                                    + objSelrs.getString(2)
                                                    + objSelrs.getSQLXML("TABLE_TYPE")
                                        );
            }
            lSumBJCLogger.setSYSTEM_LOG_OUT(true);
            objDBts.prntTableswithIssues();
            lSumBJCLogger.WriteLog(" DROP TABS " + objDBts.strDropStatRec(""));
            lSumBJCLogger.setSYSTEM_LOG_OUT(false);

        } catch (SQLException ex) {

            lSumBJCLogger.WriteErrorStack("GetShcmea ", ex);

        } catch (Exception ex) {
            lSumBJCLogger.WriteErrorStack("GetShcmea ", ex);
        } catch (Throwable ex) {
            Logger.getLogger(testiDrive_1.class.getName()).log(Level.SEVERE, null, ex);
        } finally {

        }

    }

    @Override
    void setDbConnection() throws Exception {

        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    void _MergePlacerOrder(LinkedHashMap HL7Msg) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    void sqlDbConnection() throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void _updateFillerInfo(LinkedHashMap HL7Msg) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public String Getvalue(String sBoltColProp) {
        String[] tokens = sBoltColProp.split(":");

        if (tokens.length > 1) {
            return tokens[1].toString();
        } else {
            return "";
        }
    }

    public void run_list_params( String qry, List<String> lstMergeCyprParams )
    {
        Map<String, Object> parameter = new HashMap<String, Object>();
        String sTmp = "";
        lSumBJCLogger.WriteOut( " //---------- Starting : " + " \n \n "  + 
                qry + "\n\n");
        if (qry !="" || !lstMergeCyprParams.isEmpty())        
        {
            for (int i=0; i<lstMergeCyprParams.size(); i++)
            {
                sTmp = lstMergeCyprParams.get(i);
                String[] parts1 = sTmp.split(":", 2);
                parameter.put(parts1[0].toUpperCase() + "" , parts1[1].replaceAll("'", "\""));
                
            
            }    
            try ( Transaction tx = session.beginTransaction() )
           {
               
                   tx.run(qry
                           , parameter);
                    tx.success();
                   
                    tx.close();
           }
          catch (Error e ){
           System.out.println(e.getMessage() + "ccf");
          
          }
         
        }   
        lSumBJCLogger.WriteOut( " //---------- Done : " + " \n \n ");
    
    }            
            
    
    public void  run_me_tran(String qry, String Para)
    {
        Map<String, Object> parameter = new HashMap<String, Object>();

//        HashMap parameters = new HashMap<>();
        lSumBJCLogger.WriteOut( " //---------- Starting : " + " \n \n "  + 
                qry + "\n\n"
                        + Para.replaceAll(",", ", \n"));
        
        if (Para != "" || qry !="") 
        {
            String[] parts = Para.split(",",2);
          //  System.out.println("parts : " + String.join("",parts)); //good
            
            for (int i=0 ; i <= parts.length -1 ; i ++)
            {   String[] parts1 = parts[i].split(":", 2);
                System.out.println("parts1=" + parts[i]);
                 parameter.put(parts1[0].toUpperCase() + "" , parts1[1].replaceAll("'", "\""));
            }    
            //Para = String.join("", parts);
           System.out.println( qry + "\n" + "fddddd=" + parameter.toString());
           //session.run(qry, parameters  );
          try ( Transaction tx = session.beginTransaction() )
           {
               
                   tx.run(qry
                           , parameter);
                    tx.success();
                   
                    tx.close();
           }
          catch (Error e ){
           System.out.println(e.getMessage() + "ccf");
          
          }
         
        }
        
        lSumBJCLogger.WriteOut( " //---------- Done : " + " \n \n ");
        
        
        
    }
    
    public void run_me(String sBoltCrte) {
        String sBoltCrte_1 = "";
        if (sBoltCrte.trim() != "") {
           // sBoltCrte =  sBoltCrte.substring(0, sBoltCrte.length() - 1) + "\n";
            run_merge(sBoltCrte);

        }
    }

    public void run_merge(String sBoltCrte) {

        //lSumBJCLogger.setSYSTEM_LOG_OUT(true);
        lSumBJCLogger.WriteLog("ORIGINAL: " + sBoltCrte);
        lSumBJCLogger.WriteOut( " //---------- Starting : " + " \n \n " + sBoltCrte.replaceAll(",", ", \n"));
        
        if (sBoltCrte != "") {
            session.run(sBoltCrte);
        }
        
        lSumBJCLogger.WriteOut( " //---------- Done : " + " \n \n ");
        
    }

    public String update_mergeCmd(String sBoldPkCondition, String sBoltMrg, String sTabName) {
        String[] sBoltMrgs = sBoltMrg.split(";");
        String sTmp = "";
        sTabName = sTabName + " {";
        int ipos;
        for (int i = 0; i <= sBoltMrgs.length - 1; i++) {
            ipos = 0;
            ipos = sBoltMrgs[i].indexOf(sTabName);
            if (ipos > 0) {
                sTmp = sBoltMrgs[i].substring(0, ipos + sTabName.length());
                // now inject the PK condition 
                sBoltMrgs[i] = sTmp + sBoldPkCondition + "," + sBoltMrgs[i].substring(ipos + sTabName.length());

                /*                System.out.println(
                 "*****original \n" + sBoltMrg + "\n" + 
                 " update_mergeCmd -   " +sBoltMrgs[i] + " \n sBoldPkCondition " + sBoldPkCondition  );
                 */
            }

        }
        return String.join("", sBoltMrgs);
    }

    public void neoGraphBoltStat(String sStartTime, int DES_DB_BATCH_SIZE, String stableName, itable it) throws SQLException {
        int rowCount = 0;
        java.sql.ResultSetMetaData rsMD;
        String sTime = "";
        String sBoltCrte = "";
        String sBoltMrg = "";
        String sBoltMrgall = "";
        String sBoltColProp = "";
        
        String sBoltPKColProp = "";
        String strMergeCypr="", straparams = "";
        String sMergeCyprParams = "";
        List<String> lstMergeCyprParams = new ArrayList<String>();
        
        int ibatch = 1;

        //if (lPropertyReader.getProperty("DES.DB.BATCH.SIZE") !=null)
        //    DES_DB_BATCH_SIZE = Integer.parseInt(lPropertyReader.getProperty("DES.DB.BATCH.SIZE") );
        /*   create (juliusCaesar:Play {title:'Julius Caesar', age:100, price:200.12})
         */
        rsMD = objSelrs.getMetaData();
        strMergeCypr = it.getMergeCypr("");
        strMergeCypr  = "Merge (p:" + stableName + " {";
        for (int i = 1; i <= rsMD.getColumnCount(); i++) {
             strMergeCypr = strMergeCypr + rsMD.getColumnName(i).toUpperCase() 
                     + ": {" + rsMD.getColumnName(i).trim().toUpperCase() + "},";
        }    
         strMergeCypr =   strMergeCypr.substring(0, strMergeCypr.length() - 1) + "})"; 
       
         
    //     session.run( "CREATE (a:Person {name: {name}, title: {title}})",
     //   parameters( "name", "Arthur", "title", "King" ) );

        //keep the PKS as list 

        
// add this to a string for batching 
        while (objSelrs.next()) {
            rowCount++;
            sTime = "Start" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
            sBoltCrte = sBoltCrte + " merge (" + stableName + rowCount + ":" + stableName + "{";
            sBoltPKColProp = "";
            sMergeCyprParams = "";
            lstMergeCyprParams.clear();
            for (int i = 1; i <= rsMD.getColumnCount(); i++) {
                if (comfun.isTiny_Big_Small_Int(rsMD.getColumnType(i))) {
                   /* if (objSelrs.getInt(i)==null)
                        sBoltColProp = rsMD.getColumnName(i) + ":0" ;
                                else    
                     */      
                  sBoltColProp = rsMD.getColumnName(i) + ":null" +objSelrs.getInt(i);

                } 
                else if (comfun.isNumeric_Decimal_Float(rsMD.getColumnType(i))) {
                  /*  if (objSelrs.getLong(i)=null)
                        sBoltColProp = rsMD.getColumnName(i) + ":null" ;
                    else 
                  */        
                        sBoltColProp = rsMD.getColumnName(i) + ":" + objSelrs.getLong(i);
                } 
                else if (rsMD.getColumnType(i) == java.sql.Types.VARCHAR
                        || rsMD.getColumnType(i) == java.sql.Types.CHAR) {
                    if (objSelrs.getString(i) != null) {
                        sBoltColProp = rsMD.getColumnName(i) + ":'"
                                + objSelrs.getString(i).replaceAll("\\x00", "")
                                .replaceAll("'", "") + "'";
                    } else {
                        sBoltColProp = rsMD.getColumnName(i) + ":'null'";
                        
                    }

                } else if (comfun.isAnyDateType(rsMD.getColumnType(i))) {
                    if (objSelrs.getString(i) ==null)
                        sBoltColProp = rsMD.getColumnName(i) + ":'" + "null"+"'";
                    else 
                         sBoltColProp = rsMD.getColumnName(i) + ":'" + objSelrs.getString(i) + "'";
                } else if (rsMD.getColumnType(i) == java.sql.Types.BLOB
                        || rsMD.getColumnType(i) == java.sql.Types.LONGVARBINARY) {
                } else if (rsMD.getColumnTypeName(i).toUpperCase().trim().endsWith("UUID")) {
                    if (objSelrs.getString(i)==null)
                        sBoltColProp = rsMD.getColumnName(i) + ":'" + "null" + "'";
                    else 
                        sBoltColProp = rsMD.getColumnName(i) + ":'" + objSelrs.getString(i) + "'";
                } else {
                    if (objSelrs.getString(i)==null)
                    sBoltColProp = rsMD.getColumnName(i) + ":'" + "null"+ "'";
                    else 
                        sBoltColProp = rsMD.getColumnName(i) + ":'" + objSelrs.getString(i) + "'";
                }
            // shoudl be improved     move to another function 

                if (sBoltColProp.trim() != "") {
                    if (it.isprrimaryKey(rsMD.getColumnName(i))) // store in a varibable 
                    {
                        sBoltPKColProp = sBoltPKColProp + sBoltColProp + ",";
                    }
                    for (fkTable fk : it.fktables.gettables()) {

                        if (fk.FkColumn.field.getName().equalsIgnoreCase(rsMD.getColumnName(i))) {
               //             System.out.println("creating FK links " +fk.FkColumn.field.getName()  + " rsmdn " + rsMD.getColumnName(i) );
                            // need to add the Primary of the child table                         
                            sBoltMrg = sBoltMrg 
                                    + " Merge (fk" + fk.FkColumn.CON_TABLE.toLowerCase() 
                                    + fk.PKColumn.CON_TABLE.toLowerCase()
                                    + rowCount + i + ":" + stableName + " {" + sBoltColProp + "})"
                                    + " Merge (pk" + fk.PKColumn.CON_TABLE.toLowerCase() +rowCount + i + ":" 
                                    + fk.PKColumn.CON_TABLE.toLowerCase()
                                    + " {" + fk.PKColumn.field.getName().toLowerCase() + ":" + Getvalue(sBoltColProp) + "})"
                                    + " MERGE (fk" +fk.FkColumn.CON_TABLE.toLowerCase() 
                                    + fk.PKColumn.CON_TABLE.toLowerCase()
                                    +  rowCount +i + ")-[:" + fk.PKColumn.CON_NAME.toLowerCase() //no need to give unique name
                                    + "]-(pk" + fk.PKColumn.CON_TABLE.toLowerCase() +rowCount + i + ");";

                        }

                    }// end For look for FKTables
                    //  System.out.println(" sBoltCrte -   " +sBoltCrte + " 'sBoltColProp " + sBoltColProp  );

                    //System.out.println("sBoltColProp NUll" + " TABLE : " + it.getName() + "  +++++ " + sBoltCrte.substring(0, sBoltCrte.length() - 1) );
                    sBoltCrte = sBoltCrte + sBoltColProp + ",";
                    
                    sMergeCyprParams= sMergeCyprParams + "" 
                            + sBoltColProp.substring(0,sBoltColProp.indexOf(":"))
                            + ""
                            +  sBoltColProp.substring(sBoltColProp.indexOf(":"))
                            + ",";
                    
                    lstMergeCyprParams.add(sBoltColProp);
                    System.out.println("sMergeCyprParams" + sMergeCyprParams);
                 //   System.out.println("sBoltCrte " + sBoltCrte);
                }
                
                sBoltColProp = "";
            }// for loop to loop thru columns 
            //if (sBoltCrte.substring(sBoltCrte.length()-1)==",")
            sBoltCrte = sBoltCrte.trim();
            sBoltCrte = sBoltCrte.substring(0, sBoltCrte.length() - 1);
            sBoltCrte =  sBoltCrte + "})";
            
            /// convert thi into an array 
            sMergeCyprParams = sMergeCyprParams.substring(0, sMergeCyprParams.length() -1 );
            sMergeCyprParams = "" + sMergeCyprParams + "";
            //System.out.println(strMergeCypr + "::" + sMergeCyprParams);

            // add the FK mapping w/ PK 
            if (sBoltPKColProp != "") {
                sBoltPKColProp = sBoltPKColProp.substring(0, sBoltPKColProp.length() - 1);
                //if (sBoltMrg !="")
                sBoltMrgall = sBoltMrgall+update_mergeCmd(sBoltPKColProp, sBoltMrg, stableName);
            }
            
            

            System.out.println("creating FK links for  " + rowCount + " : " + sBoltMrg);
            sBoltMrg= "";

            sTime = sTime + ": adding to batch =" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
            // add to batch 

            if ((int) rowCount % DES_DB_BATCH_SIZE == 0) {
                //run_me(sBoltCrte);
//                run_me_tran(strMergeCypr, sMergeCyprParams);
                run_list_params(strMergeCypr,lstMergeCyprParams );
             //   run_merge(sBoltMrgall);
                
                sBoltMrg = "";
                sBoltCrte = "";
                sBoltMrgall= "";
                sTime = sTime + " : batch commit =" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());

                lSumBJCLogger.WriteLog(sStartTime + "\n"
                        + String.join("", Collections.nCopies(30, "-")) + '\n' + sTime
                );
                ibatch++;

            }

        }// while 

        run_me(sBoltCrte);
        run_merge(sBoltMrgall);
        sBoltMrg = "";
        sBoltMrgall= "";
        objSelrs.close();
        //_insStatement.close();

        sStartTime = sStartTime
                + String.join("", Collections.nCopies(30, "-"))
                + ":End" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());

        lSumBJCLogger.WriteLog("Done  " + getName() + sStartTime);

    }

   //--------------------------------------------------------------------------------------------------------
    // ltestiDrive.Write2DB("", "", "tms.mime_type_lu", "tms_tmp.mime_type_lu", "tms", "tms_tmp");String strfromTabName, String strToTabName, String strFrmSchema, String strtoSchema
    public void Write2DB(int limitSize) {
        String strsql = "";
        String strFrmSch = objfrmSchema.getName();
        String strInsSql = "";
        String sStartTime = "";
        String WhereClause = "";

        int SRC_DB_FETCH_SIZE = 2000;
        int DES_DB_BATCH_SIZE = 2000;

        try {
            if (lPropertyReader.getProperty("SRC.DB.FETCH.SIZE") != null) {
                SRC_DB_FETCH_SIZE = Integer.parseInt(lPropertyReader.getProperty("SRC.DB.FETCH.SIZE"));
            }

            if (lPropertyReader.getProperty("DES.DB.BATCH.SIZE") != null) {
                DES_DB_BATCH_SIZE = Integer.parseInt(lPropertyReader.getProperty("DES.DB.BATCH.SIZE"));
            }
            DES_DB_BATCH_SIZE=1;
            for (itable itab : objToSchema.gettables()) {
                System.out.println("Processing " + itab.getName());
             // verify if  table is not created in destination 
                //if ( objDBts.isDbtable(   objToSchema.getName().toLowerCase() , itab.getName().toLowerCase()                         )  )
                {
                    if (itab.hasBCLOB) // has clob ior large fields then 
                    {
                        SRC_DB_FETCH_SIZE = Integer.parseInt(lPropertyReader.getProperty("SRC.DB.FETCH.SIZE.LARGE.OBJ")); //SRC.DB.FETCH.SIZE.LARGE.OBJ=20
                        DES_DB_BATCH_SIZE = SRC_DB_FETCH_SIZE;

                    }
                    // if it has clob/ blob then lets chnage the fetch size 
                    sStartTime = "Begin" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
                    strsql = "Select " + itab.getName() + ".* from " + strFrmSch + "." + itab.getName() + " " + itab.getName();//+ " limit 10";
                    // recursively get the whereclause 

                    if (limitSize > 0) {
                        WhereClause = objToSchema.getRecuriveFKs(itab, objfrmSchema.getName());// get this from from Schema Clause 
                        WhereClause = WhereClause + "  LIMIT " + limitSize;
                    }

                    _selStatement = objSrcloc._conn.prepareStatement(strsql + WhereClause);
                    objSelrs = _selStatement.executeQuery();
                    _selStatement.setFetchSize(SRC_DB_FETCH_SIZE);// chnage this if PK index cardinality

                    neoGraphBoltStat(sStartTime, DES_DB_BATCH_SIZE, itab.getName().toLowerCase(), itab); // call this proc to map the values ot derstinationj tagle 

                }// dont process the table if the table is no created in destination 
            }// end for 

        } catch (Error e) {
            lSumBJCLogger.WriteErrorStack(strsql, e);
        } catch (SQLException e) {

            Exception E = null;
            while (e.getNextException() != null) {
                E = e.getNextException();
                setErrDesc("SQL Error !! in tquery.open() Err Code" + "\n" + E.getMessage());// + lSumBJCLogger.GetErrFromStack(E);
                System.out.println("\nError:" + getiErrDesc());
                lSumBJCLogger.WriteErrorStack(strsql, E);
            }
            setErrDesc("SQL Error !! in tquery.open() Err Code" + e.getErrorCode() + "\n" + e.getMessage());
            System.out.println("\nError:" + getiErrDesc());
            e = null;
            lSumBJCLogger.WriteErrorStack(strsql, e);

        } finally {
            _selStatement = null;
            _insStatement = null;
        }

    }

    public static void main(String[] args) {

        neo4cp lneo4cp = new neo4cp("", "");

       lneo4cp.getTabBySchema("tms", "tms_tmp");
        //lneo4cp.getTabBySchema("qtg", "qtg_tmp");

     lneo4cp.Write2DB(0);
        
        String statement = " Merge (p:MIME_TYPE_LU {MIME_TYPE_LU_ID: $MIME_TYPE_LU_ID,TYPE_CODE: $TYPE_CODE,DESC_TEXT: $DESC_TEXT,CRTE_USER_CODE: $CRTE_USER_CODE,CRTE_TIMESTMP: $CRTE_TIMESTMP,UPDT_USER_CODE: $UPDT_USER_CODE,UPDT_TIMESTMP: $UPDT_TIMESTMP,VERSION: $VERSION,ACTV_REC_IND: $ACTV_REC_IND}"
                + ")";
       // Transaction tx = lneo4cp.session.beginTransaction() 
       //      ;   
Map<String, Object> paramter = new HashMap<String, Object>();
paramter.put("MIME_TYPE_LU_ID","9B65DB389A4C4D8091D3D26B7300A14E");
paramter.put("TYPE_CODE","JPEG"); 
paramter.put("DESC_TEXT","IMAGE/JPEG"); 
paramter.put("CRTE_USER_CODE","SYSTEM"); 
paramter.put("CRTE_TIMESTMP","2014-08-19 21,37,44.397908"); 
paramter.put("UPDT_USER_CODE","SYSTEM"); 
paramter.put("UPDT_TIMESTMP","2014-08-19 21,37,44.397908"); 
paramter.put("VERSION",1); 
paramter.put("ACTV_REC_IND","T");
System.out.println("paramter= " + paramter.toString());
lneo4cp.session.run(statement,paramter);

        lneo4cp.session.run("MErge(dude{name:'George'})"

        
        );

     lneo4cp.session.run(statement, Values.parameters("UPDT_USER_CODE","SYSTEM", "TYPE_CODE","JPEG", "CRTE_USER_CODE","SYSTEM", "VERSION",1, "UPDT_TIMESTMP","2014-08-19 21:37:44.397908", "ACTV_REC_IND","t", "MIME_TYPE_LU_ID","9b65db389a4c4d8091d3d26b7300a14e", "DESC_TEXT","image/jpeg", "CRTE_TIMESTMP","2014-08-19 21:37:44.397908")
     )
     ;   
  Map<String, Object> props = new HashMap<>();
props.put( "name", "Andres" );
props.put( "position", "Developer" );

Map<String, Object> params = new HashMap<>();
params.put( "props", paramter );
String query = "CREATE (p:man $props)";
lneo4cp.session.run( query, params );
     
     
lneo4cp.session.close();
        
        
        lneo4cp = null;

    }

}
