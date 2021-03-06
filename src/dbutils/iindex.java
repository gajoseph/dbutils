/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import static dbutils.idrive.lSumBJCLogger;

/**
 *
 * @author TGAJ2
 */
public class iindex extends tfield{
    public  String sidxDDL; 
    public  String OwnerName; // schemaname
    public String CARDINALITY; // # of rows for an index if IDXTYPE is table filetr 
    public String PAGES ;
    public String FILTER_CONDITION ;
    public String IDXTYPE ; /*
    short => index type:
tableIndexStatistic - this identifies table statistics that are returned in conjuction with a table's index descriptions
tableIndexClustered - this is a clustered index
tableIndexHashed - this is a hashed index
tableIndexOther - this is some other style of index
    */
    
    private List<idxFld> idxfields;
    
    // now declare a collection of idxdet
    
    
    
    //   HashMap<String,tfield>fields; //=new HashMap<String,tfield>(); 
    //and index can have mutiple fields
   
protected  void AddIndexField(tfield tFld, String ORDINAL_POSITION, String ASC_OR_DESC)    {
     idxFld idxFld1 = new idxFld(tFld);
            
    idxFld1.ORDINAL_POSITION = ORDINAL_POSITION;
    idxFld1.setASC_OR_DESC( ASC_OR_DESC);
    
        this.idxfields.add(idxFld1);
    /*
        this.AddField(tFld.getName() 
            , tFld.getType() 
            , tFld.getValue()
            , 1
            , dbtype.isnull.valueOf( tFld.getNullable().toString()
            ).ordinal()
            , tFld.getLength()
    );
   */     
}

  public idxFld getIndexField( String fldName ){
    
        Optional<idxFld>  t= idxfields.stream()
                        .filter(u -> u.indxField.getName().equalsIgnoreCase(fldName))
                            .findFirst();
        if (t.isPresent())    
            return t.get();
        else return                   
            new idxFld(new tfield());
    

    }



protected  void DelIndexField(idxFld tFld)    
{
            idxfields.remove(tFld);
            
}
//-------------------------------------------------------------------------

public iindex(){
    super();
    idxfields = new ArrayList<idxFld>();
}    
// this generated the index ddl 
public String GetDDL(String sSchema){
    String sidxCrte= "create";
    String scols= "";
     boolean isvalid = true ;
        
      if (this.getType().equalsIgnoreCase("t"))
          sidxCrte= sidxCrte + "  INDEX " ;
      else 
         sidxCrte= sidxCrte + " UNIQUE INDEX " ;
/*
      for (int i=0; i< this.idxfields.size(); i++)
        try { 
            scols = scols  + this.Fields(i).getName() + ", ";
            } 
         catch (Throwable ex) {
                    Logger.getLogger(testiDrive_1.class.getName()).log(Level.SEVERE, null, ex);
                }
      */
      
      for(idxFld next  : idxfields){
          if (next.indxField.getName().trim().equalsIgnoreCase(""))
              isvalid=false;
            scols = scols  + next.indxField.getName() 
                    + "\t" +  next.ASC_OR_DESC // addd the asc or desc 
                    + ", ";
        }
      
       scols = this.getName()
                    + " on " + sSchema + "."+ this.OwnerName 
                    + " ("+ scols.substring(0, scols.length()-2)
                    + ")";
        sidxCrte = sidxCrte + scols + ";";
// check if the index fields isempty       
    if (( scols.equalsIgnoreCase(", ")) || (isvalid== false) )
    { //2017-06-02  custom index which can't be converted 
        sidxCrte = "/*============== Cannot create index >>"+ this.OwnerName + "==============   \n" 
                    + sidxCrte 
                    + "==================================================================================*/\n";
    }
    // add the     public String CARDINALITY;    public String PAGES ; public String FILTER_CONDITION ;
    sidxCrte = "-- CARDINALITY:" 
            +  this.CARDINALITY
             + ": PAGES: " + this.PAGES
             + ": FILTER_CONDITION:" + this.FILTER_CONDITION
              + " ---"
              +  "\n"  + sidxCrte;
      sidxDDL = sidxCrte;
      return sidxCrte;
    }



 protected void finalize() throws Throwable {
     
    comfun.rmidxFldFromArrylist(idxfields);    

 }
 
 
}
