/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
This Copies the struct of DB to another DB 
postgres to postgres is good 
Db2 to postgres underway 

 */
package dbutils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import static dbutils.idrive.lSumBJCLogger;
import static dbutils.idrive.lPropertyReader;
import static dbutils.idrive.lSumBJCLogger;

/**
 *
 * @author TGAJ2
 */
public class iCopy extends idrive {

    protected iDataloc objSrcloc;
    //public _selResultSet

    protected iDataloc objDesloc;
    public String SRC_DB_URL;
    public String DES_DB_URL;
    
    public String SRC_DB_USER;
    public String SRC_DB_PASSWORD;
    


    public String DES_DB_USER;
    public String DES_DB_PASSWORD;
    
    
    public String SRCdbDriver;
    public String DESdbDriver;

    public ischema objfrmSchema;// = new ischema();
    public ischema objToSchema;// = new ischema();

    public Dbtables objDBts;

    ResultSet objSelrs = null;
    PreparedStatement _selStatement = null;

    ResultSet objIns = null;
    PreparedStatement _insStatement = null;

    //public _selResultSet insert resultset 
    public iCopy(String sSrcUrl, String sDesUrl) {

        
        
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
        objSrcloc = new iDataloc(SRC_DB_URL, SRCdbDriver, SRC_DB_USER, SRC_DB_PASSWORD );
        _connSrc = objSrcloc._conn;

        //Initaite Des Db location object 
        objDesloc = new iDataloc(DES_DB_URL, DESdbDriver,DES_DB_USER, DES_DB_PASSWORD);
        _connDest = objDesloc._conn;

        //  initiate the schema Objects 
        objfrmSchema = new ischema();
        objToSchema = new ischema();

    // initiate the dbtables object 
        objDBts = new Dbtables();

    }

    protected void finalize() throws Throwable {
        try {
            objSrcloc.finalize();
            objDesloc.finalize();
            objfrmSchema.finalize();
            objToSchema.finalize();

        } catch (Exception e) {
            lSumBJCLogger.WriteErrorStack("Finalize Exception", e);

        } catch (Error e) {
            lSumBJCLogger.WriteErrorStack("Finalize Error ", e);

        } finally {
            objIns = null;
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
                lSumBJCLogger.WriteLog("table = " + objSelrs.getString(3) + " 2 " + objSelrs.getString(2)
                        + objSelrs.getSQLXML("TABLE_TYPE")
                );
            }
            
            objDBts.prntTableswithIssues();
            String d = objDBts.strDropStatRec("");
            lSumBJCLogger.WriteLog(" DROP TABS " + d);

            lSumBJCLogger.setSYSTEM_LOG_OUT(false);

        } catch (SQLException ex) {

            lSumBJCLogger.WriteErrorStack("GetShcmea ", ex);

        } catch (Exception ex) {
            //Logger.getLogger(testiDrive_1.class.getName()).log(Level.SEVERE, null, ex);
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

    public void tabFldTovalues(String sStartTime, int DES_DB_BATCH_SIZE) throws SQLException{
        int rowCount=0;
        ResultSetMetaData rsMD;
        String sTime= "";
        
        int ibatch = 1;
        
        
        
                
        //if (lPropertyReader.getProperty("DES.DB.BATCH.SIZE") !=null)
        //    DES_DB_BATCH_SIZE = Integer.parseInt(lPropertyReader.getProperty("DES.DB.BATCH.SIZE") );
                
        
        
        rsMD = objSelrs.getMetaData();
        
        while (objSelrs.next()) 
        {
            rowCount++;
            sTime = "Start" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
            
            for (int i = 1; i <= rsMD.getColumnCount(); i++) {
                if (rsMD.getColumnType(i) == java.sql.Types.TINYINT
                        || rsMD.getColumnType(i) == java.sql.Types.BIGINT
                        || rsMD.getColumnType(i) == java.sql.Types.INTEGER
                        || rsMD.getColumnType(i) == java.sql.Types.SMALLINT) 
                {
                    _insStatement.setInt(i, objSelrs.getInt(i));
                } 
                else if (rsMD.getColumnType(i) == java.sql.Types.NUMERIC
                        || rsMD.getColumnType(i) == java.sql.Types.DECIMAL
                        || rsMD.getColumnType(i) == java.sql.Types.FLOAT) 
                {
                    _insStatement.setLong(i, objSelrs.getLong(i));
                } 
                else if (rsMD.getColumnType(i) == java.sql.Types.VARCHAR
                        || rsMD.getColumnType(i) == java.sql.Types.CHAR) 
                {
                    if (objSelrs.getString(i) != null) 
                    {
                        _insStatement.setString(i, objSelrs.getString(i).replaceAll("\\x00", ""));
                    } else 
                    {
                        _insStatement.setString(i, objSelrs.getString(i));
                    }

                    _insStatement.setString(i, objSelrs.getString(i));
                } 
                else if (rsMD.getColumnType(i) == java.sql.Types.DATE
                        || rsMD.getColumnType(i) == java.sql.Types.TIME
                        || rsMD.getColumnType(i) == java.sql.Types.TIMESTAMP
                        || rsMD.getColumnType(i) == java.sql.Types.TIMESTAMP_WITH_TIMEZONE
                        || rsMD.getColumnType(i) == java.sql.Types.TIME_WITH_TIMEZONE) 
                {
                    _insStatement.setObject(i, objSelrs.getObject(i));
                } else if (rsMD.getColumnType(i) == java.sql.Types.BLOB
                        || rsMD.getColumnType(i) == java.sql.Types.LONGVARBINARY) 
                {
                    _insStatement.setBytes(i, objSelrs.getBytes(i));
                } else if (rsMD.getColumnTypeName(i).toUpperCase().trim().endsWith("UUID")) 
                {
                    _insStatement.setObject(i, objSelrs.getObject(i));
                } else 
                {
                    _insStatement.setObject(i, objSelrs.getObject(i));
                }
            }
            sTime = sTime + ": adding to batch =" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
            // add to batch 
            _insStatement.addBatch();
            if ((int) rowCount % DES_DB_BATCH_SIZE == 0)
            {
                _insStatement.executeBatch();
                objDesloc._conn.commit();
                _insStatement.clearBatch();
                sTime = sTime + " : batch commit =" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
                
                lSumBJCLogger.WriteLog( sStartTime + "\n"
                                        + String.join("", Collections.nCopies(30, "-")) + '\n' + sTime 
                                        ) ;
                ibatch++;
            
            }    
            
        }// while 
        _insStatement.executeBatch();
        objDesloc._conn.commit();
        _insStatement.clearBatch();
        objSelrs.close();
        _insStatement.close();
        
         sStartTime = sStartTime
                    + String.join("", Collections.nCopies(30, "-"))
                    + ":End" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());

            lSumBJCLogger.WriteLog("Done  " + getName() + sStartTime);
        
    
    }
    
    
   //--------------------------------------------------------------------------------------------------------
    // ltestiDrive.Write2DB("", "", "tms.mime_type_lu", "tms_tmp.mime_type_lu", "tms", "tms_tmp");String strfromTabName, String strToTabName, String strFrmSchema, String strtoSchema
public void Write2DB(int limitSize ) {
     String strsql = "";
     String strFrmSch = objfrmSchema.getName();
     String strInsSql = "";
     String sStartTime = "";
     String WhereClause = "";

    int SRC_DB_FETCH_SIZE  = 2000;
    int DES_DB_BATCH_SIZE = 2000;

     try {
         if (lPropertyReader.getProperty("SRC.DB.FETCH.SIZE") !=null)
             SRC_DB_FETCH_SIZE = Integer.parseInt(lPropertyReader.getProperty("SRC.DB.FETCH.SIZE") );
         
        if (lPropertyReader.getProperty("DES.DB.BATCH.SIZE") !=null)
            DES_DB_BATCH_SIZE = Integer.parseInt(lPropertyReader.getProperty("DES.DB.BATCH.SIZE") );

         for (itable itab : objToSchema.gettables()) 
         {
             // verify if  table is not created in destination 
             if ( objDBts.isDbtable(   objToSchema.getName().toLowerCase() , itab.getName().toLowerCase()                         )  )
             {   
                if (itab.hasBCLOB) // has clob ior large fields then 
                {    
                    SRC_DB_FETCH_SIZE =  Integer.parseInt(lPropertyReader.getProperty("SRC.DB.FETCH.SIZE.LARGE.OBJ") ); //SRC.DB.FETCH.SIZE.LARGE.OBJ=20
                    DES_DB_BATCH_SIZE =  SRC_DB_FETCH_SIZE;

                }
                // if it has clob/ blob then lets chnage the fetch size 
                 sStartTime = "Begin" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
                 strsql = "Select " + itab.getName()  + ".* from " + strFrmSch + "." + itab.getName() + " " + itab.getName()  ;//+ " limit 10";
                 // recursively get the whereclause 

                 if (limitSize > 0){
                    WhereClause = objToSchema.getRecuriveFKs(itab, objfrmSchema.getName());// get this from from Schema Clause 
                    WhereClause = WhereClause + "  LIMIT " + limitSize  ;
                 }

                 System.out.println ( "table = " +itab.getName() + "\n " +  strsql + " WhereClauseaa = " + WhereClause);

                 _selStatement = objSrcloc._conn.prepareStatement(strsql + WhereClause ); 
                 objSelrs  = _selStatement.executeQuery();
                 _selStatement.setFetchSize(SRC_DB_FETCH_SIZE);// chnage this if PK index cardinality

                 strInsSql = itab.getInsStatCommaSep(objToSchema.getName());
                 _insStatement =objDesloc._conn.prepareStatement(strInsSql);

                 tabFldTovalues(sStartTime, DES_DB_BATCH_SIZE); // call this proc to map the values ot derstinationj tagle 
                 _insStatement.close();// DO I NEED THIS

             }// dont process the table if the table is no created in destination 
             //else itab.= false 
         }// end for 

     } catch (Error e) {
         lSumBJCLogger.WriteErrorStack(strsql, e);
     } catch (SQLException e) {

          Exception E = null;
         while (e.getNextException() != null) {
             E = e.getNextException();

             setErrDesc("SQL Error !! in tquery.open() Err Code" + "\n" + E.getMessage());// + lSumBJCLogger.GetErrFromStack(E);

             System.out.println("\nError:" + getiErrDesc() );
             
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

        iCopy licopy = new iCopy("", "");
        
        //licopy.getTabBySchema("tms", "tms_tmp");
        licopy.getTabBySchema("qtg", "qtg_tmp");
        
    //    licopy.Write2DB(0);
        licopy= null;
        
        
        
        
        
    }
    
    

}
