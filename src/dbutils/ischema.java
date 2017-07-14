/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import static dbutils.idrive.lSumBJCLogger;

/**
 *
 * @author TGAJ2
 */
public class ischema  extends tfield{
    
// Storing in lust b/c hash table won't store in the order it was added .
    private List<itable> tabList;// = new ArrayList<String>();
             
    private int tabcount=1;
    // Store the CReate statement DDL
    private String _CreateTabStatement= "";
    

//    HashMap<String,iindex> indexes=new HashMap<String,iindex>(); 

    //----------------------------------------------------------------------------
    public ischema() {
        this.tabcount   = 0;
        tabList         = new ArrayList<itable>();
    }

    public String getCreateTabStatement() {
        return _CreateTabStatement;
    }

    
    public void setCreateTabStatement(String _CreateTabStatement) {
        this._CreateTabStatement = _CreateTabStatement;
    }
    
    public boolean isTableAdded(String TabName){
    return    false; 
    }
public itable TableByName( String TabName ){
  Optional<itable>  t = tabList.stream()
                          .filter(u -> u.getName().equalsIgnoreCase(TabName)
                                  )
                                  .findFirst();
  if (t.isPresent())    
      return t.get();
  else return                   
      new itable();
}
    
    public itable AddTable(String TabName, String TabType, itable itab) {
        //tabList.add( itab);
        return AddTable(TabName, TabType ,itab , 1 );
//        this.AddField(TabName , TabType ,String value , int indx, int Nullable, int Length )
  }
     
  //----------------------------------------------------------------------------  
    
   private itable AddTable(String Tabname , String Tabtype ,itable itab , int indx )
    { 	
    	String indx1    = ""+indx;
        itable  ltable  =itab;
//Code starts 
        ltable.setName(Tabname.toUpperCase());
        ltable.setType(Tabtype);
        
        ltable.ID       = this.tabcount;
//        _tabByName.put(Tabname.toUpperCase(),ltable);
        tabList.add(itab);
        tabcount++;
        return ltable;
    }

public List<itable> gettables(){
        return this.tabList;
}
     
    
    public itable gettable( String tabName ){
    
       Optional<itable>  t= tabList.stream()
                        .filter(u -> u.getName().equalsIgnoreCase(tabName))
                            .findFirst();
        if (t.isPresent())    return t.get();
        else return                   new itable();
       
        

    }
   public String joinCon(itable it , String s2Sch){
       String a = "";
   if  (it.sJoincondition.indexOf("LEFT JOIN") > 0 )
       a = "(" + it.sJoincondition + ")";
    else 
       a = it.sJoincondition;
       //a = s2Sch + "." + it.getName();
   
       System.out.println( "ddddddd  + "+ a  + " has join cond " + it.sJoincondition );
       return a ;
   } 
          
// get JOIN CLuae when there is limit pull    
public  String getRecuriveFKs(itable itab, String s2Sch )
    {
        String strJoin= "";
        String CON_TABLE = ""; // check if there is referecne to the same table of 2 columns 
        String strchldJOinCond= "";
        if (itab.fktables != null)
        {
            for (fkTable fktab : itab.fktables.gettables() )
            {System.out.println( itab.getName()  + " << Starting " +  this.gettable(fktab.PKColumn.CON_TABLE).getName()  + "---------" ); 
                
               if (fktab.hasDups)          
                    strJoin= strJoin + " LEFT JOIN " 
                            +
                            joinCon(this.gettable(fktab.PKColumn.CON_TABLE) , s2Sch)  
                            + "  /*Condition */  " 
                            + fktab.PKColumn.CON_TABLE  +"_" + fktab.FkColumn.field.getName() 
                    + " ON "  + fktab.FkColumn.CON_TABLE+ "."    
                    + fktab.FkColumn.field.getName()  + " =  " 
                            + fktab.PKColumn.CON_TABLE +"_"+ fktab.FkColumn.field.getName()+ "." 
                            + fktab.PKColumn.field.getName();

                else 
                    strJoin= strJoin 
                            + " LEFT JOIN " +
                            joinCon(this.gettable(fktab.PKColumn.CON_TABLE), s2Sch )  
                            
                            + " /*condition*/ " + fktab.PKColumn.CON_TABLE
                    + " ON "  + fktab.FkColumn.CON_TABLE+ "."    
                        + fktab.FkColumn.field.getName()  + " =  " + fktab.PKColumn.CON_TABLE + "." + fktab.PKColumn.field.getName();
                // recursilvel call the function 
                System.out.println("Recurisvelu calling " +  this.gettable(fktab.PKColumn.CON_TABLE).getName()  + "-----"  );
//           
     strJoin = strJoin + " " 
                        //+ getRecuriveFKs (   this.gettable(fktab.PKColumn.CON_TABLE) ) 
                        + "";
  
            }            
        }
        else
        {
            //itab.sJoincondition =s2Sch + "." + itab.getName();
          //  System.out.println("Recurisvelu calling but missede *****" +  itab.getName() );
        
        }
            
            
            
        if (strJoin.trim() !="")    
            itab.sJoincondition = s2Sch + "." + itab.getName() + " "  + "  "+strJoin + "   "  + "  /* "
                    + itab.getName()+ " */";   
        else 
            itab.sJoincondition = s2Sch + "." + itab.getName() ;
    return strJoin;
    }    
    
    
    
    
    
            
    
    protected void finalize() throws Throwable {
    	//lets list
//        comfun.listTabDetails(this.tabList);
        
       comfun.hasTabRemovelst(this.tabList);
       this.tabList.clear();
       this.tabList = null;
        super.finalize();     
    }
     protected void print() throws Throwable {
    	//comfun.print(this._tabByName, tabcount);
        
    }
    
}
