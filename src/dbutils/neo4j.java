/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.jdbc.bolt.BoltDriver;

import org.neo4j.driver.v1.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.jdbc.*;
import org.neo4j.jdbc.bolt.BoltConnection;
import static dbutils.idrive.lSumBJCLogger;
import static org.neo4j.driver.v1.Values.parameters;

/**
 *
 * @author TGAJ2
 */
public class neo4j {

    public static void main(String[] args) {

        //licopy.getTabBySchema("tms", "tms_tmp");
        // licopy.getTabBySchema("qtg", "qtg_tmp");
        // after this write to new4j database 
        iCopy licopy;
        java.sql.ResultSet objSelrs = null;
        org.neo4j.driver.v1.Driver driver = null;
        Session session = null;
        String sLinks = "";
        String sChld = "";
        try {
            Class.forName("org.neo4j.jdbc.Driver");
            licopy = new iCopy("", "");
            licopy.getTabBySchema("tms", "tms_tmp");

            
            //bolt://127.0.0.1:7687
            driver = GraphDatabase.driver("bolt://127.0.0.1:7687", AuthTokens.basic("neo4j", "mypassword"));

            session = driver.session();
            session.run("CREATE (a:Person {name: {name}, title: {title}})",
                    parameters("name", "Arthur", "title", "King"));

            //new Driver().connect("jdbc:neo4j:bolt://127.0.0.1:7474");
            lSumBJCLogger.setSYSTEM_LOG_OUT(true);
            String neoTabCreate = "create (";
         // java.sql.Statement crtDDL = _conn.createStatement();

            for (itable it : licopy.objToSchema.gettables()) {
                neoTabCreate = "create (" + it.getName() + ":Table{name:'" + it.getName() + "'}),";
              //getTabCols(it);

                neoTabCreate = neoTabCreate + "\n\t "
                        + it.getfields().stream()
                        .map(fld -> "(" + fld.getName() + ":Column{name:'" + fld.getName() + "'})," + "\t"
                                + "(" + it.getName() + ")-[:has]->(" + fld.getName() + " ),"
                        )
                        .collect(Collectors.joining("\n\t"));

               // Foreign Key 
               // end Foreihn Key 
                neoTabCreate = neoTabCreate.substring(0, neoTabCreate.length() - 1) + "\n";

                session.run(neoTabCreate);
                
                

                for (fkTable fk : it.fktables.gettables()) {
                    sLinks = "MERGE (fk:Table {name:'" + fk.FkColumn.CON_TABLE + "'}) MERGE (pk:Table {name:'" + fk.PKColumn.CON_TABLE + "'})"
                            + " MERGE (fk)-[:ForeignKey]-(pk)";
                   ;session.run(sLinks);
                  
                    
                }
                
                lSumBJCLogger.WriteLog(" --- \n" + neoTabCreate + "\n ");
                neoTabCreate = "";
                sLinks= "";
            }// for 

            session.close();
            driver.close();

        } catch (ClassNotFoundException ex) {
            Logger.getLogger(neo4j.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception e) {
            Exception E = null;

            System.out.println(" error " + e.getMessage());

            //lSumBJCLogger.WriteErrorStack("Error ", e);
            //lSumBJCLogger.WriteLog(" Created connection  " );
        } finally {
            licopy = null;

        }

    }

}
