/*
 * Tao change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
straung postgres 
cmd C:\Program Files\PostgreSQL\9.3\bin
pg_ctl start -D c:\data2

 */
package dbutils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.postgresql.*;
import org.postgresql.copy.CopyManager;
import java.sql.JDBCType;

/**
 *
 * @author tgaj2
 */
public class testiDrive_1 extends idrive {

    // @@Override
    public CopyManager cpManager;
    public byte c[];

    Connection conn1;
    Connection connDest;

    DatabaseMetaData dmdDestdb;
    ResultSet dataTypeinfoDestdb;

    itable srctable;
    itable desttable;

    ischema objfrmSchema;
    ischema objToSchema;
    // List<itable> tabList ;//= ArrayList<String>();

    /* postrgres type mapping */
    private void copywhat(String tab) {

        ByteArrayInputStream input = new ByteArrayInputStream(c);

        try {

            cpManager.copyIn("COPY " + tab + " FROM STDIN  WITH DELIMITER '|' NULL AS 'NULL' ", input);
            connDest.commit();

        } catch (SQLException ex) {
            lSumBJCLogger.WriteErrorStack("Copywhat ", ex);
        } catch (IOException ex) {
            lSumBJCLogger.WriteErrorStack(" Tab :", ex);
        }

    }

    void _MergePlacerOrder(LinkedHashMap HL7Msg) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    protected void _updateFillerInfo(LinkedHashMap HL7Msg) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    void sqlDbConnection() throws Exception {

    }

    @Override
    void setDbConnection() throws Exception {
        try {
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://stgd520a/coredb?user=tgaj2&password=8UcREt3p&ssl=false";
            url = "jdbc:postgresql://localhost/clindb?user=asd&password=asd&ssl=false";

            _connDest = java.sql.DriverManager.getConnection(url);
            _connDest.setAutoCommit(false);
            ;
        } catch (ClassNotFoundException e) {
            System.out.println("  error." + e.getMessage());
            _iErrDesc = "  error." + e.getMessage();

        } catch (SQLException e) {
            Exception E = null;
            while (e.getNextException() != null) {
                E = e.getNextException();

                _iErrDesc = _iErrDesc + "\n" + "SQL Error !! " + "\n" + E.getMessage();
                setErrDesc(_iErrDesc);
                //System.out.println("\nError:" + _ErrDesc);
            }
            _iErrDesc = "SQL Error !! in tquery.open() Err Code" + e.getErrorCode() + "\n" + e.getMessage();
            setErrDesc(_iErrDesc);
            E = null;
            throw new UnsupportedOperationException(_iErrDesc); //To change body of generated methods, choose Tools | Templates.

        }

    }

    void PrintTabInfo(String sSchema, String sTable, DatabaseMetaData t) {

        ResultSet pkrs;
        try {
            pkrs = t.getPrimaryKeys(null, sSchema, sTable);

            while (pkrs.next()) {
                String PK = pkrs.getString("COLUMN_NAME");
                System.out.println("getPrimaryKeys(): columnName=" + PK);
                //getFK(Schemas.getString("TABLE_SCHEM"), resultSet.getString("TABLE_NAME"));
            }// while pk rs 
            pkrs.close();

            pkrs = t.getImportedKeys(null, sSchema, sTable);
            while (pkrs.next()) {

                String PK = pkrs.getString("FKTABLE_NAME");
                System.out.println("FK(): Tabname=" + PK + ": Colname " + pkrs.getString("FKCOLUMN_NAME"));

            }// while pk rs 

        } catch (SQLException ex) {
            Logger.getLogger(testiDrive.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /* 
     These could be copied to impelmenation side 
     
    void canCreateIdx(ResultSet srcTabPks, String sTableName) throws Throwable {
        int i = 0;
        tfield a;
        try {
            desttable.Indexes = new indexes();
            while (srcTabPks.next()) {
                i++;

                //   desttable.FieldByName(srcTabPks.getString("COLUMN_NAME"))
                if (desttable.Indexes.IndexExists(srcTabPks.getString("INDEX_NAME"))) {
                    a = desttable
                            .FieldByName(
                                    srcTabPks
                                    .getString("COLUMN_NAME")
                            );
                    //a.setName(sTableName);
                    desttable.Indexes
                            .getIndex(srcTabPks.getString("INDEX_NAME")
                            ).AddIndexField(a);
                } else {

                    iindex d = new iindex();
                    d.OwnerName = sTableName;
                    d.setName(srcTabPks.getString("INDEX_NAME")
                    );
                    d.setType(srcTabPks.getString("NON_UNIQUE")
                    );
                    d.AddIndexField(
                            desttable
                            .FieldByName(srcTabPks
                                    .getString("COLUMN_NAME")
                            )
                    );
                    desttable.Indexes
                            .AddIndex(
                                    d
                            );

                }
                //if (sIdxName !=null)
                {   //srctable.FieldByName(sIdxName).setPrimary(true);
                    //desttable.FieldByName(sIdxName).setPrimary(true);
                }
                // lSumBJCLogger.WriteLog(rsval.toString());
            }
        } catch (Exception ex) {
            lSumBJCLogger.WriteErrorStack("", ex);
        } catch (Error ex) {
            lSumBJCLogger.WriteErrorStack("", ex);
        } finally {
            lSumBJCLogger.WriteLog("");

        }
    }
*/
    void canCreatePrivs(ResultSet srcTabPks, String sTableName) throws Throwable {

        StringBuilder sPkname = new StringBuilder(" ");
        int i = 0;
        try {
            while (srcTabPks.next()) {
                i++;

                sPkname.append("GRANT ");
                sPkname.append(srcTabPks.getString("PRIVILEGE"));
                sPkname.append(" ON ");
                sPkname.append(desttable.OwnerName);
                sPkname.append(".");
                sPkname.append(sTableName + " TO " + srcTabPks.getString("GRANTEE") + ";\n");

                if (sPkname != null) {   //srctable.setGrants(sPkname.toString().rep);
                    desttable.setGrants(sPkname.toString());
                }

            }
        } catch (Exception ex) {
            lSumBJCLogger.WriteErrorStack("", ex);

        } catch (Error ex) {
            lSumBJCLogger.WriteErrorStack("", ex);
        } finally {
            lSumBJCLogger.WriteLog
        ("");

        }

    }

    void canCreatePks(ResultSet srcTabPks, String sTableName) throws Throwable {
        String sPkname = "";
        int i = 0;
        try {
            while (srcTabPks.next()) {
                i++;

                sPkname = srcTabPks.getString("COLUMN_NAME");
                if (sPkname != null) {
                    srctable.FieldByName(sPkname).setPrimary(true);
                    desttable.FieldByName(sPkname).setPrimary(true);
                }
                lSumBJCLogger.WriteLog(sPkname);
            }
        } catch (Exception ex) {
            lSumBJCLogger.WriteErrorStack("", ex);

        } catch (Error ex) {
            lSumBJCLogger.WriteErrorStack("", ex);
        } finally {
            lSumBJCLogger.WriteLog(desttable.GetDDL());
        }
    }

    public String GetJDBCTYPE(String type) {
        JDBCType[] JDBCType1 = JDBCType.values();
        String a = "NOTFOUND";
        System.out.println(" Finding " + ":  " + type);
        for (int i = 0; i <= JDBCType1.length - 1; i++) {
//        System.out.println(" Comparing " + JDBCType1[i].getName() + "  w/  " + type +"  " + JDBCType1[i].name() + " "+java.sql.Types.class.getName()
//       + " " 
//        
//        );
//        

            if (JDBCType1[i].getName().equalsIgnoreCase(type)) {
                a = JDBCType1[i].getName();
                System.out.println(JDBCType1[i] + "  Found  " + type);
                break;
            }
        }
        return a;
    }

    public void getTabBySchema(String sFrmSchema, String sToSchema) {
        ResultSet rstables;
        try {
            rstables = conn1.getMetaData()
                    .getTables(null, sFrmSchema, null, new String[]{"TABLE"});

            objfrmSchema = new ischema();
            objToSchema = new ischema();

            Dbtables dbts = new Dbtables();
            dbts.objFrmSchema = objfrmSchema;
            dbts.objToSchema = objToSchema;
            
            dbts.conn1 = conn1;
            
// dbts.tabList      = tabList;
            
            
            while (rstables.next()) {
                 
                // getTabDetails(sFrmSchema, sToSchema, rstables.getString(3));  
                dbts.getTabDetails(   sFrmSchema, sToSchema, rstables.getString(3)     );
                lSumBJCLogger.WriteLog("table = " + rstables.getString(3) + " 2 " + rstables.getString(2) + 
                        rstables.getSQLXML("TABLE_TYPE")
                );
            }

            lSumBJCLogger.setSYSTEM_LOG_OUT(true);
            dbts.prntTableswithIssues();

            String d = dbts.strDropStatRec("");


            lSumBJCLogger.WriteLog(" DROP TABS " + d);
            lSumBJCLogger.setSYSTEM_LOG_OUT(false);

            objfrmSchema.finalize();
            objToSchema.finalize();

        } catch (SQLException ex) {
            //Logger.getLogger(testiDrive_1.class.getName()).log(Level.SEVERE, null, ex);
            lSumBJCLogger.WriteErrorStack("GetShcmea ", ex);

        } catch (Exception ex) {
            //Logger.getLogger(testiDrive_1.class.getName()).log(Level.SEVERE, null, ex);
            lSumBJCLogger.WriteErrorStack("GetShcmea ", ex);
        } catch (Throwable ex) {
            Logger.getLogger(testiDrive_1.class.getName()).log(Level.SEVERE, null, ex);
        } finally {

        }

    }

    public void GetDBmeta(String strFrmSchema, String strtoSchema) throws SQLException {
        String url = "jdbc:postgresql://localhost/clindb?user=asd&password=asd&ssl=false&relaxAutoCommit=true";
        url = "jdbc:postgresql://stgd542a/qtgdb?user=tgaj2&password=8UcREt3p&ssl=false&relaxAutoCommit=true";
        String db2drvr = "com.ibm.db2.jcc.DB2Driver";
        String db2url = "jdbc:db2://prdd551a:50000/dpdwhs01";
        String db2user = "tgaj2";
        String db2pass = "password";

        try {
//                     Class.forName("org.postgresql.Driver");
            Class.forName(db2drvr); //*****
            lSumBJCLogger.WriteLog("Source conn1");
//                     conn1 = java.sql.DriverManager.getConnection(url);
            // db2url=db2url+""

            conn1 = java.sql.DriverManager.getConnection(db2url, db2user, db2pass);
            conn1.setAutoCommit(false);
            lSumBJCLogger.setSYSTEM_LOG_OUT(true);
            // get the datatyoes 

            System.out.println("*****************************************************************************************************************************");

            int i = 0;
            ResultSet asd = conn1.getMetaData().getTypeInfo();

            ResultSetMetaData ss;
            ss = asd.getMetaData();
            for (int j = 1; j <= ss.getColumnCount(); j++) {
                System.out.println("#" + j + ": " + ss.getColumnName(j) + ss.getColumnLabel(j)
                        + ss.getColumnType(j));
            }
            while (asd.next()) {
                System.out.println("# DB    " + i + ": " + asd.getString("TYPE_NAME") + ": JAVA TYPE :" + asd.getString("DATA_TYPE"));

                i++;
            }

            // end GEt datatype 
            getDb2TabBySchema(strFrmSchema, strtoSchema);
            conn1.close();
            conn1 = null;
            System.out.println("*****************************************************************************************************************************");
        } catch (Error e) {
            lSumBJCLogger.WriteErrorStack("Error in ", e);
        } catch (ClassNotFoundException ex) {
            lSumBJCLogger.WriteErrorStack("Error in ", ex);
        }
    }

    public void getDb2TabBySchema(String sFrmSchema, String sToSchema) {
        ResultSet rstables;

        try {

            //try to get the schema 
            DatabaseMetaData dbmd = conn1.getMetaData();
            // rstables = ((com.ibm.db2.jcc.DB2DatabaseMetaData) dbmd).getSchemas();
            //----------------------------------------------------------------------------
            // junk
            rstables = dbmd.getSchemas();
            while (rstables.next()) {

                lSumBJCLogger.WriteLog("Schenma = " + rstables.getString(1) + " 2 " + rstables.getString(2)
                );
            }

            rstables = ((com.ibm.db2.jcc.DB2DatabaseMetaData) dbmd).getCatalogs();
            while (rstables.next()) {

                lSumBJCLogger.WriteLog("Cat = " + rstables.getString(1)
                );
            }

            //----------------------------------------------------------------------------
            // junk
            rstables = ((com.ibm.db2.jcc.DB2DatabaseMetaData) dbmd).getTables(null, sFrmSchema, null, new String[]{"TABLE"}
            );
            objfrmSchema = new ischema(); objfrmSchema.setName(sFrmSchema);
            objToSchema = new ischema(); objToSchema.setName(sFrmSchema);
            //   tabList = new ArrayList<itable>();

            Dbtables dbts = new Dbtables();
            dbts.objFrmSchema = objfrmSchema;
            dbts.objToSchema = objToSchema;
            dbts.conn1 = conn1;
         // dbts.tabList      = tabList;

            ResultSetMetaData ss;
            ss = rstables.getMetaData();
            for (int j = 1; j <= ss.getColumnCount(); j++) {
                System.out.println("#" + j + ": " + ss.getColumnName(j) + ", " + ss.getColumnLabel(j) + ", "
                        + +ss.getColumnType(j));
            }

            while (rstables.next()) {

                // getTabDetails(sFrmSchema, sToSchema, rstables.getString(3));  
                dbts.getTabDetails(sFrmSchema, sToSchema, rstables.getString(3));
                lSumBJCLogger.setSYSTEM_LOG_OUT(true);
                lSumBJCLogger.WriteLog("table = " + rstables.getString(3) + " 2 " + rstables.getString(2) + " TABLE_SCHEM  " + rstables.getString("TABLE_SCHEM")
                        + "  "  + rstables.getString("TABLE_TYPE")
                
                );
                
                lSumBJCLogger.setSYSTEM_LOG_OUT(false);        

            }
            lSumBJCLogger.setSYSTEM_LOG_OUT(true);
            dbts.prntTableswithIssues();
            lSumBJCLogger.setSYSTEM_LOG_OUT(false);

            objfrmSchema.finalize();
            objToSchema.finalize();

        } catch (SQLException ex) {
            //Logger.getLogger(testiDrive_1.class.getName()).log(Level.SEVERE, null, ex);
            lSumBJCLogger.WriteErrorStack("GetShcmea ", ex);

        } catch (Exception ex) {
            //Logger.getLogger(testiDrive_1.class.getName()).log(Level.SEVERE, null, ex);
            lSumBJCLogger.WriteErrorStack("GetShcmea ", ex);
        } catch (Throwable ex) {
            Logger.getLogger(testiDrive_1.class.getName()).log(Level.SEVERE, null, ex);
        } finally {

        }

    }

    public void GetDBPostgresmeta(String strFrmSchema, String strtoSchema) throws SQLException {
        String url = "jdbc:postgresql://localhost/coredb?user=asd&password=asd&ssl=false&relaxAutoCommit=true";
        //url = "jdbc:postgresql://stgd542a/qtgdb?user=tgaj2&password=8UcREt3p&ssl=false&relaxAutoCommit=true";
        try {
            Class.forName("org.postgresql.Driver");
            lSumBJCLogger.WriteLog("Source conn1");
            conn1 = java.sql.DriverManager.getConnection(url);

            conn1.setAutoCommit(false);

            getTabBySchema(strFrmSchema, strtoSchema);

            conn1.close();
            conn1 = null;
        } catch (Error e) {
            lSumBJCLogger.WriteErrorStack("Error in ", e);
        } catch (ClassNotFoundException ex) {
            lSumBJCLogger.WriteErrorStack("Exception  ", ex);
        }
    }

    public void Write2DB(String strsql, String strSRVname, String strfromTabName, String strToTabName, String strFrmSchema,
            String strtoSchema) {
        /*
         --GEO--C-- 3/12 thgis will be staring dump from one db to another Db. 
         * Thinsg to do 
         *  1) ready from source and estination info from XML files. decrypt password 
         *  2) 
         */
        ResultSet objRS = null;
        PreparedStatement _Statement = null;
        //Statement _Statement = null;
        ResultSetMetaData rsMD;
// -- Rowdata 
        String srowData = "";
        int rowCount = 0;
        Date dtstar = new Date();
        String snewline = "\n";

        PreparedStatement _insStatement = null;
        String strInsSql = "";
        String sCrap = "";
        String straparams = "";
        String sStartTime = "";
        String _sDbg = "";
        String sTime = "";
        int ibatch = 1;
        byte[] browBites;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] readbytes;
        String sDesdbDatatype;

        String url = "jdbc:postgresql://localhost/clindb?user=asd&password=asd&ssl=false&relaxAutoCommit=true";
        url = "jdbc:postgresql://stgd542a/qtgdb?user=tgaj2&password=8UcREt3p&ssl=false&relaxAutoCommit=true";
        String db2drvr = "com.ibm.db2.jcc.DB2Driver";
        String db2url = "jdbc:db2://ug01.unigroupinc.com:5032/DBP";
        String db2user = "tgaj2";
        String db2pass = "[password";

        sStartTime = "Begin" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
        url = "jdbc:postgresql://localhost/coredb?user=asd&password=asd&ssl=false&relaxAutoCommit=true?searchpath=tms";
        //conn = Tobjects.tconnection.getInstance();

        try {
            Class.forName("org.postgresql.Driver");
            //   Class.forName(db2drvr);
            lSumBJCLogger.WriteLog("Source conn1");
            conn1 = java.sql.DriverManager.getConnection(url);
            // db2url=db2url+""
//                   //  conn1 = java.sql.DriverManager.getConnection(db2url,db2user,db2pass );
            conn1.setAutoCommit(false);

            strsql = "Select * from " + strfromTabName + " limit 10";
            //  strsql = "Select * from "+ strfromTabName + " WITH UR ";

            _Statement = conn1.prepareStatement(strsql);
            _Statement.setFetchSize(1000);

            objRS = _Statement.executeQuery();
            // objRS = (DB2ResultSet)_Statement.executeQuery();
/*  Destination 
                         
             */

            Class.forName("org.postgresql.Driver");

            url = "jdbc:postgresql://stgd542a/qtgdb?user=tgaj2&password=8UcREt3p&ssl=false&relaxAutoCommit=true";

            url = "jdbc:postgresql://stgd520a/coredb?user=tgaj2&password=8UcREt3p&ssl=false&relaxAutoCommit=true";
            url = "jdbc:postgresql://localhost/clindb?user=asd&password=asd&ssl=false&relaxAutoCommit=true?searchpath=tms";//-- set search path

            connDest = java.sql.DriverManager.getConnection(url);
            connDest.setAutoCommit(false);

            cpManager = ((PGConnection) connDest).getCopyAPI();
            // get medata of dest db     
            dmdDestdb = connDest.getMetaData();
            //getdatatypes(t.getTypeInfo());
// Do we need to create a table in destination ? 
//       Build create table strruct   

            dataTypeinfoDestdb = dmdDestdb.getTypeInfo();
            // Get metadata 
            rsMD = objRS.getMetaData();

//            getTabBySchema(strFrmSchema, strtoSchema);

            System.out.println("---------------------------------------------------------------");

            strInsSql = "Insert into " + strToTabName + " (";
            straparams = " Values(";
            for (int i = 1; i <= rsMD.getColumnCount(); i++) {
                strInsSql = strInsSql + rsMD.getColumnName(i) + ",";
                straparams = straparams + "?,";
                ;

                // shoudl Check if there are BLOBS; if there are the only fetch onoy 1 row
            }

            strInsSql = strInsSql.substring(0, strInsSql.length() - 1);
            strInsSql = strInsSql + ")" + straparams.substring(0, straparams.length() - 1) + ")";
            lSumBJCLogger.WriteLog(strInsSql);
            //System.out.println( "Row # " + rowCount + " I : " + i +" " +rsMD.getColumnName(i)+ " "+ rsMD.getColumnTypeName(i) + " " + objRS.getString(i) );

            _insStatement = connDest.prepareStatement(strInsSql);

            //strInsSql="insert into COUNT_EVENT (PATIENT_ID,MODULE_ID, CARE_EVENT_COUNTER, EVENT_TYPE_SEQ_NUM,INCORRECT_ACTIONS, EVENT_TYPE, COUNT_NAME , INCORRECT_ACTIONS_COMMENTS, INCORRECT_NOTIFIED, INCORRECT_NOTIFIED_COMMENTS, COMMENTS, INCORRECT_XRAY_TAKEN_TF )values (?,?, ?, ?,?, ?, ?, ?, ?, ?, ?,?)";
            while (objRS.next()) {
                //objRS.
                rowCount = rowCount + 1;
                sCrap = "";
                sTime = "Start" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
                for (int i = 1; i <= rsMD.getColumnCount(); i++) {
                    //    sCrap= sCrap + ""+ objRS.getString(i);
//                             System.out.println(rsMD.getColumnName(i) + " i= " + i + " " + rsMD.getColumnType(i));

                    /*
                     Db2 bytes not working 
                             
                     browBites  = ((byte[]) objRS.getBytes(i));
                     if (objRS.getBytes(i)== null ) {
                     _insStatement.setNull(i, rsMD.getColumnType(i));
                     browBites  ="NULL".getBytes();      
                                   
                     }
                     else*/ 
                    if (rsMD.getColumnType(i) == java.sql.Types.TINYINT
                            || rsMD.getColumnType(i) == java.sql.Types.BIGINT
                            || rsMD.getColumnType(i) == java.sql.Types.INTEGER
                            || rsMD.getColumnType(i) == java.sql.Types.SMALLINT) {
                        _insStatement.setInt(i, objRS.getInt(i));
                    } else if (rsMD.getColumnType(i) == java.sql.Types.NUMERIC
                            || rsMD.getColumnType(i) == java.sql.Types.DECIMAL
                            || rsMD.getColumnType(i) == java.sql.Types.FLOAT) {
                        _insStatement.setLong(i, objRS.getLong(i));
                    } else if (rsMD.getColumnType(i) == java.sql.Types.VARCHAR
                            || rsMD.getColumnType(i) == java.sql.Types.CHAR) {
                                if ( objRS.getString(i) != null ) {
                                            _insStatement.setString(i, objRS.getString(i).replaceAll("\\x00", "")); 
                                        }
                                        else {
                                          _insStatement.setString(i, objRS.getString(i));
                                        }
                        
                        _insStatement.setString(i, objRS.getString(i));
                    } else if (rsMD.getColumnType(i) == java.sql.Types.DATE
                            || rsMD.getColumnType(i) == java.sql.Types.TIME
                            || rsMD.getColumnType(i) == java.sql.Types.TIMESTAMP
                            || rsMD.getColumnType(i) == java.sql.Types.TIMESTAMP_WITH_TIMEZONE
                            || rsMD.getColumnType(i) == java.sql.Types.TIME_WITH_TIMEZONE) {
                        _insStatement.setObject(i, objRS.getObject(i));
                    } else if (rsMD.getColumnType(i) == java.sql.Types.BLOB
                            || rsMD.getColumnType(i) == java.sql.Types.LONGVARBINARY) {
                        _insStatement.setBytes(i, objRS.getBytes(i));
                    } else if (rsMD.getColumnTypeName(i).toUpperCase().trim().endsWith("UUID")) {
                        _insStatement.setObject(i, objRS.getObject(i));
                    } else {
                        _insStatement.setObject(i, objRS.getObject(i));
                    }

                            //if (! (browBites == null))
                    //    outputStream.write(browBites);
                    //if (i < rsMD.getColumnCount())                             outputStream.write("|".getBytes());
                    _sDbg = _sDbg + '\n' + "Row # " + rowCount + " I : " + i + " " + rsMD.getColumnName(i) + " " + rsMD.getColumnTypeName(i) + " " + objRS.getString(i);

                }
                //   outputStream.write("\n".getBytes());
                //   System.out.println(outputStream);
                sTime = sTime + ": end =" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
                // System.out.println(String.join("", Collections.nCopies(30, "-"))+'\n'+ _sDbg
                // + String.join("", Collections.nCopies(30, "-")));

                _sDbg = "";
                sTime = sTime + ": adding to batch =" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
                _insStatement.addBatch();

                // System.out.println(String.join("", Collections.nCopies(30, "-"))+'\n'+ sTime
                // + String.join("", Collections.nCopies(30, "-")));
                // Check if # rows is 1000 then write . 
                if ((int) rowCount % 2000 == 0) {

                    readbytes = null;
                    c = null;
                    browBites = null;

                    readbytes = outputStream.toByteArray();
                    c = readbytes;

//                            copywhat(strToTabName);
                    outputStream = null;
                    outputStream = new ByteArrayOutputStream();

                    sTime = "batch# " + ibatch + " :StartTime =" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());

                    _insStatement.executeBatch();
                    connDest.commit();
                    _insStatement.clearBatch();
                    //_insStatement.clearBatch();
//                              lSumBJCLogger.WriteOut("Committed rows " + rowCount 
                    //                                     + " ["+ new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date()) + "]\t" +  " ["+ new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(dtstar) + "]\t");
                    //_output.write(srowData);
                    sTime = sTime + " : batch commit =" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());
                    lSumBJCLogger.WriteLog(
                            sStartTime + "\n"
                            + String.join("", Collections.nCopies(30, "-")) + '\n' + sTime
                    );
                    ibatch++;

                }

            }
            // for 

            _insStatement.executeBatch();
            connDest.commit();

            //   if (_output != null)  _output.close();
            objRS.close();
            _Statement.close();
            conn1.setAutoCommit(true);
            conn1.close();

            _insStatement.close();

            connDest.close();
            //Statement _Statement = null;

            sStartTime = sStartTime
                    + String.join("", Collections.nCopies(30, "-"))
                    + ":End" + new java.text.SimpleDateFormat("HH:mm:ss:SSS").format(new Date());

            lSumBJCLogger.WriteLog("Done in " + sStartTime);
        } catch (SQLException e) {
            //e = new  SQLException();

            Exception E = null;
            while (e.getNextException() != null) {
                E = e.getNextException();

                setErrDesc("SQL Error !! in tquery.open() Err Code" + "\n" + E.getMessage());// + lSumBJCLogger.GetErrFromStack(E);

                System.out.println("\nError:" + getiErrDesc() + sCrap);
            }

            setErrDesc("SQL Error !! in tquery.open() Err Code" + e.getErrorCode() + "\n" + e.getMessage());
            System.out.println("\nError:" + getiErrDesc());

            e = null;

        } // end Catch
        catch (Error e) {
            _iErrDesc = "Error !! in tquery.open()  Err Code" + e.getCause() + "\n" + e.getMessage();
            System.out.println("\nError:" + _iErrDesc);
            e.printStackTrace();

            //lSumBJCLogger.WriteErrorStack("getReportDetails " ,e ) ;
            //lSumBJCLogger.WriteLog("");
            e = null;

        } // end Catch 
        catch (Exception e) {
            _iErrDesc = "Exception !! in tquery.open() Err Code ddd"
                    + "\n"
                    + e.getStackTrace().toString()
                    + e.getMessage();
            System.out.println("\nError:"
                    + _iErrDesc
            );
            e = null;
        } finally {
            objRS = null;
            _Statement = null;
            conn1 = null;
            _insStatement = null;
            connDest = null;
        }
    }
    //    _output = null;

    public testiDrive_1() {

        super();

    }

    protected synchronized void finalize() throws java.lang.Throwable {

        try {
            lSumBJCLogger.WriteLog(objToSchema.gettable("dock1").getName());

            objToSchema.finalize();
            objfrmSchema.finalize();

            //dbts = null;
            //     tabList.clear();
            //     tabList = null;
            //desttable.finalize();
            //srctable.finalize();
          
            for (int i = 0; i < this.getClass().getFields().length - 1; i++) {
                System.out.println(this.getClass().getFields()[i].getType().getSimpleName() + " i=" + i);
            }

        } catch (Exception e) {
//        	////System.out.println("Exception " + e.getMessage() + " :" + e.getCause() );
            throw new RuntimeException("unexpected invocation exception: " + e.getMessage());
        } catch (Error e) {
//        	////System.out.println("Exception " + e.getMessage() + " :" + e.getCause() );
            throw new Error("unexpected Error occured : " + e.getMessage());
        } finally {

            super.finalize();
        }

    }

    public static void main(String[] args) {
        //       muhl7ParseRad lmuhl7ParseRad =  new muhl7ParseRad("C:\\Users\\gaj3236\\Stuff\\downloads\\IR\\NR\\mercury.na55asyng_out422.hl7", "na55asyng_out");
        testiDrive_1 ltestiDrive = new testiDrive_1();//("C:\\Users\\gaj3236\\Stuff\\downloads\\IR\\NR\\mercury.na51syngo_in422.hl7", "na55asyng_in");
        
        int i = 1;
        ResultSetMetaData ss;
        System.out.println(ltestiDrive._iErrDesc);
        try {
//            DatabaseMetaData t;
//            ltestiDrive.setDbConnection();
//            t = ltestiDrive._connDest.getMetaData();
//            //getdatatypes(t.getTypeInfo());
//
//            ResultSet asd = t.getTypeInfo();
//            ss = asd.getMetaData();
//            for (int j = 1; j <= ss.getColumnCount(); j++) {
//                System.out.println("#" + j + ": " + ss.getColumnName(j) + ss.getColumnLabel(j)
//                        + ss.getColumnType(j));
//            }
//            while (asd.next()) {
//                System.out.println("#" + i + ": " + asd.getString("TYPE_NAME") + " JAVA TYPE "
//                        + asd.getString("DATA_TYPE"));
//
//                i++;
//            }

            /*
          
             ResultSet Schemas = t.getSchemas();
          
             while (Schemas.next())  {
             System.out.println("Printing schema \"TABLE\" " + Schemas.getString("TABLE_SCHEM"));
             String SS = String.join("", Collections.nCopies(30, "-"));
             ResultSet resultSet = t.getTables(null, Schemas.getString("TABLE_SCHEM"), null, new String[]{"TABLE"});
             System.out.println("Printing TABLE_TYPE \"TABLE\" ");
             System.out.println(SS);

             while(resultSet.next())
             {
             //Print
             System.out.println(resultSet.getString("TABLE_NAME"));
             ltestiDrive.PrintTabInfo(Schemas.getString("TABLE_SCHEM"),resultSet.getString("TABLE_NAME"), t  );

             }
             // get PK 

             System.out.println(SS +  Schemas.getString("TABLE_SCHEM").toString() + SS);
             } 
             */
            //ltestiDrive.Write2DB("", "", "tms.LTAB", "TMS.LTAB_NEW");
            //  ltestiDrive.Write2DB("", "", "tms.test1", "TMS.test2", "tms", "tmsnew");
//            ltestiDrive.GetDBmeta("UNIPROD", "UNIPROD_TEST");
            try {
                //
//     ltestiDrive.Write2DB("", "", "UNIPROD.MASTERAGENT", "appdba.masteragent_1");
      //    ltestiDrive.Write2DB("", "", "tms.mime_type_lu", "tms_tmp.mime_type_lu", "tms", "tms_tmp");
                        

//ltestiDrive.GetDBPostgresmeta("qtg", "qtg_tmp");
//                ltestiDrive.GetDBPostgresmeta("tms", "tms_tmp");
ltestiDrive.Write2DB("", "", "tms.mime_type_lu", "tms_tmp.mime_type_lu", "tms", "tms_tmp");

                
                ltestiDrive.finalize();
            } catch (Throwable ex) {
                lSumBJCLogger.WriteError("Error " + ex.getStackTrace());
            }
        } /*   catch (SQLException e) 
         {
         System.out.println(e.fillInStackTrace().toString());

         } 
         */ catch (Exception e) {
            System.out.println(e.fillInStackTrace().toString());

        }

        ltestiDrive = null;
    }//end of main

} // testIDrive
