/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author TGAJ2
 */
public class fkTable extends tfield {
    public contraintcolumn FkColumn;
    public contraintcolumn PKColumn;
    public boolean hasIssues ;
    public String  shasIssues ;
    public boolean hasDups ;
    
public fkTable()
{
    super(); 
}      

protected  void AddFKField(tfield tt, String tabName, String  sSchema, String FK_NAME)   {
   FkColumn = new contraintcolumn(tt);
   FkColumn.CON_TYPE="FK";
   FkColumn.CON_SCHEMA = sSchema.toUpperCase();
   FkColumn.CON_TABLE = tabName.toUpperCase();
   FkColumn.CON_NAME = FK_NAME.toUpperCase();
   this.setName(tabName.toUpperCase());
   
    FkColumn.field = tt;   
    
    hasIssues= false;
    hasDups=false ;
    
}

protected  void AddPKField(tfield tt, String tabName, String  sSchema, String PK_NAME)   {
   PKColumn = new contraintcolumn(tt);
   PKColumn.CON_TYPE="PK";
   PKColumn.CON_SCHEMA = sSchema.toUpperCase();
   PKColumn.CON_TABLE = tabName.toUpperCase();
   PKColumn.field = tt;        
   PKColumn.CON_NAME = PK_NAME.toUpperCase();
    
}

// this generated the index ddl 
public String GetDDL(String s2Schema){
    String sidxCrte= "Alter table ";
    String scols= "";

    sidxCrte = sidxCrte + s2Schema+ "."
            + FkColumn.CON_TABLE + "\t "
            + "ADD CONSTRAINT " + FkColumn.CON_NAME
            + "\t" + FkColumn.CON_TYPE.replace("FK", "FOREIGN KEY(")
            + " " + FkColumn.field.getName() + ") "
            + " References "
            //''+ PKColumn.CON_SCHEMA + "."
// we are creating the table also onto the same schema             
            + s2Schema+ "."
            + PKColumn.CON_TABLE 
            +"(" 
            + PKColumn.field.getName() + ");";
//System.out.println(FkColumn.toString() + "  skdkdk");
    
    if (hasIssues)
      sidxCrte = "/* ----"+shasIssues 
              + "*/\n"  
              + "\n  --- " + sidxCrte;
    
    return sidxCrte;
    }


public String GetConstraint_name()
{
    String Con_name = "";
    if (FkColumn.CON_NAME =="")
        Con_name = FkColumn.CON_TABLE + "_ref_" + PKColumn.CON_TABLE ;
    else 
        Con_name = FkColumn.CON_NAME;    

    return Con_name;
}

protected void finalize() throws Throwable {
    
    try {
       
       this.FkColumn= null;
       this.PKColumn= null;
       super.finalize(); 
       
    }
       catch (Exception e ){
       	}
       catch (Error e ){
       	}

    finally { 
    	super.finalize();     }
    }


}
