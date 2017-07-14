/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.sql.Connection;
import java.sql.SQLException;

import static dbutils.idrive.lSumBJCLogger;

/**
 *
 * @author TGAJ2
 */
public class iDataloc extends tfield {
    public Connection _conn;
    public String surl = "";// JDBC connection URL 

    
    
    
    
    public iDataloc(String url, String class4Name, String db2user, String db2pass) {
        try {
            Class.forName(class4Name);
            
            _conn =java.sql.DriverManager.getConnection(url, db2user, db2pass);
            _conn.setAutoCommit(false);
            lSumBJCLogger.WriteLog(" Created connection  " + url);

        } catch (SQLException e) {
            Exception E = null;
            while (e.getNextException() != null) {
                E = e.getNextException();
                lSumBJCLogger.WriteErrorStack("Error ", E);
            }

            lSumBJCLogger.WriteErrorStack("Error ", e);

        } 
        
        catch (ClassNotFoundException E) {
             lSumBJCLogger.WriteErrorStack(url, E);


        } 
        
        finally 
        {

        }

    }

    protected void finalize() throws Throwable {
        tfield l;
        String indx1;
        try {
            if (!(_conn == null)) 
            {
                if (!_conn.isClosed()) {
                    _conn.close();
                }
            }
        } catch (Exception e) {
            lSumBJCLogger.WriteErrorStack("Finalize Exception", e);

        } catch (Error e) {
            lSumBJCLogger.WriteErrorStack("Finalize Error ", e);

        } finally {
            _conn = null;
            super.finalize();
        }
    }

}
