/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbutils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author TGAJ2
 */
public class dbtype {



public List<jdbcType> db2types = new ArrayList<jdbcType>();
    

public enum isnull{                
NULL(1),
NOT_NULL(2);

 private final int isnullCode;

    isnull(int levelCode) {
        this.isnullCode = levelCode;


        }
private static final Map<String, isnull> lookup = new HashMap<String, isnull>();    

static {
        for (isnull d : isnull.values()) {
            lookup.put(Integer.toString(d.isnullCode), d);
        }
    }

    
}
    
    
    
public enum db{                
POSTGRES(2003),
DB2(-5);

 private final int dbCode;

    db(int levelCode) {
        this.dbCode = levelCode;


        }
private static final Map<String, db> lookup = new HashMap<String, db>();    

static {
        for (db d : db.values()) {
            lookup.put(Integer.toString(d.dbCode), d);
        }
    }

    

 public static db get(String abbreviation) {
        return lookup.get(abbreviation);
    }
} 

    
    
    
interface JDBC {
 
}    
    
    
    
    
public enum lJDBC implements JDBC{                
ARRAY(2003),
BIGINT(-5),
BINARY(-2),
BIT(-7),
BLOB(2004),
BOOLEAN(16),
CHAR(1),
CLOB(2005),
DATALINK(70),
DATE(91),
DECIMAL(3),
DISTINCT(2001),
DOUBLE(8),
FLOAT(6),
INTEGER(4),
JAVA_OBJECT(2000),
LONGNVARCHAR(-16),
LONGVARBINARY(-4),
LONGVARCHAR(-1),
NCHAR(-15),
NCLOB(2011),
NULL(0),
NUMERIC(2),
NVARCHAR(-9),
OTHER(1111),
REAL(7),
REF(2006),
ROWID(-8),
SMALLINT(5),
SQLXML(2009),
STRUCT(2002),
TIME(92),
TIMESTAMP(93),
TINYINT(-6),
VARBINARY(-3),
VARCHAR(12)
;

 private final int lJDBCCode;

    lJDBC(int levelCode) {
        this.lJDBCCode = levelCode;


        }
private static final Map<String, lJDBC> lookup = new HashMap<String, lJDBC>();    

static {
        for (lJDBC d : lJDBC.values()) {
            lookup.put(Integer.toString(d.lJDBCCode), d);
        }
    }

    

 public static lJDBC get(String abbreviation) {
        return lookup.get(abbreviation);
    }
} 

/*
postgres JDBC mapping 
*/


public enum PosJDBC implements JDBC{                
ARRAY(2003),
BIGINT(-5),
BYTEA(-2),
BIT(-7),
BLOB(2004),
BOOLEAN(16),
CHARACTER(1),
CLOB(2005),
DATALINK(70),
DATE(91),
DECIMAL(3),
DISTINCT(2001),
DOUBLE_PRECISION(8),
FLOAT(6),
INTEGER(4),
JAVA_OBJECT(2000),
TEXT(-16) ,//LONGNVARCHAR(-16),
BYTE_A(-4), // --LONGVARBINARY(-4),
LONGVARCHAR(-1),
NCHAR(-15),
NCLOB(2011),
NULL(0),
NUMERIC(2),
NVARCHAR(-9),
OTHER(1111),
REAL(7),
REF(2006),
ROWID(-8),
SMALLINT(5),
XML(2009),
STRUCT(2002),
TIME(92),
TIMESTAMP(93),
TINYINT(-6),
VARBINARY(-3),
VARCHAR(12)
;

 private final int PosJDBCCode;

    PosJDBC(int levelCode) {
        this.PosJDBCCode = levelCode;


}
private static final Map<String, PosJDBC> lookup = new HashMap<String, PosJDBC>();    

static 
    {
        for (PosJDBC d : PosJDBC.values()) 
        {
            lookup.put(Integer.toString(d.PosJDBCCode), d);
        }
    }

    

 public static PosJDBC get(String abbreviation) {
        return lookup.get(abbreviation);
    }
} 

/// DB2 Mapping 

 
public void loadDb2TYpes() {
        
    db2types.add(new jdbcType("LONG VARGRAPHIC"	,	-1	) );
    db2types.add(new jdbcType(	"CHAR"          ,	1	) );
    db2types.add(new jdbcType(	"GRAPHIC"	,	1	) );
    db2types.add(new jdbcType(	"DECIMAL"	,	3	) );
    db2types.add(new jdbcType(	"INTEGER"	,	4	) );
    db2types.add(new jdbcType(	"SMALLINT"	,	5	) );
    db2types.add(new jdbcType(	"REAL"          ,	7	) );
    db2types.add(new jdbcType(	"DOUBLE"	,	8	) );
    db2types.add(new jdbcType(	"VARCHAR"	,	12	) );
    db2types.add(new jdbcType(	"VARGRAPHIC"	,	12	) );
    db2types.add(new jdbcType(	"BOOLEAN"	,	16	) );
    db2types.add(new jdbcType(	"DATE"          ,	91	) );
    db2types.add(new jdbcType(	"TIME"          ,	92	) );
    db2types.add(new jdbcType(	"TIMESTAMP"	,	93	) );
    db2types.add(new jdbcType(	"DECFLOAT"	,	1111	) );
    db2types.add(new jdbcType(	"XML"           ,	1111	) );
    db2types.add(new jdbcType(	"DISTINCT"	,	2001	) );
    db2types.add(new jdbcType(	"ROW"           ,	2002	) );
    db2types.add(new jdbcType(	"ARRAY"         ,	2003	) );
    db2types.add(new jdbcType(	"BLOB"          ,	2004	) );
    db2types.add(new jdbcType(	"CLOB"          ,	2005	) );
    db2types.add(new jdbcType(	"DBCLOB"	,	2005	) );

    db2types.add(new jdbcType(	"BIGINT"                        ,	-5	) );
    db2types.add(new jdbcType(	"LONG VARCHAR FOR BIT DATA"	,	2004	) );
    db2types.add(new jdbcType(	"VARCHAR () FOR BIT DATA"	,	2004	) );
    db2types.add(new jdbcType(	"CHAR () FOR BIT DATA"          ,	2004	) );
    db2types.add(new jdbcType(	"LONG VARCHAR"                  ,	-1	) );
    db2types.add(new jdbcType(	"LONG VARGRAPHIC"               ,	-1	) );

        
        
        
        
}

    
 




    
}

