/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.*;
import org.neo4j.jdbc.bolt.BoltConnection;
//import org.neo4j.jdbc.bolt.
import org.neo4j.jdbc.DatabaseMetaData;

/**
 *
 * @author TGAJ2
 */
public class neo4jMetaDb {
    
    public static void main(String[] args) {
        
        
        //org.neo4j.driver.v1.Driver conn = null;
       
        String databaseVersion; 
        //BoltConnection conn;
        org.neo4j.driver.v1.Driver objDesloc = null;
        try {
            objDesloc =// (BoltConnection) java.sql.DriverManager.getConnection("jdbc:neo4j:http://127.0.0.1:7474", "neo4j", "mypassword");
                   GraphDatabase.driver("bolt://127.0.0.1:7687", AuthTokens.basic("neo4j", "mypassword"));
				
        Session session = objDesloc.session();
        //StatementResult rs = session.run("CALL dbms.components() yield name,versions WITH * WHERE name=\"Neo4j Kernel\" RETURN versions[0] AS version");
        StatementResult rs = session.run("CALL db.labels() yield label return label order by label");
		if (rs != null && rs.hasNext()) {
			Record record = rs.next();
			if (record.containsKey("version")) {
				databaseVersion = record.get("version").asString();
                                System.out.println(databaseVersion + " -- databaseVersion ");
			}
		}
                
                
        } catch (Error  ex) {
            java.util.logging.Logger.getLogger(neo4jMetaDb.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            java.util.logging.Logger.getLogger(neo4jMetaDb.class.getName()).log(Level.SEVERE, null, ex);
        }        
        
    }
    
    
}
