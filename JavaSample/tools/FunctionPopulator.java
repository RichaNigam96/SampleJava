package com.amazon.dw.grasshopper.tools;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import com.amazon.dw.grasshopper.functions.sql.*;
import com.amazon.dw.grasshopper.metadata.MetaDataConfiguration;
import com.amazon.dw.grasshopper.model.DBType;
import com.amazon.dw.grasshopper.model.Function;

public class FunctionPopulator {

    private static List<Function> functions = new LinkedList<Function>();
    
    public static void main(String [] args){
        run();
        }
    
    public static void run(){
        addFunctions();
        
        MetaDataConfiguration config = MetaDataConfiguration.getInstance();
        writeToFile(config.getFunctionFilename(), functions);
    }
    
    private static void writeToFile(String filename, List<? extends Serializable> items)
    {
        try {
            FileOutputStream fileOut = new FileOutputStream(filename);
            ObjectOutputStream outfile = new ObjectOutputStream(new BufferedOutputStream(fileOut));
           
            outfile.writeObject(items);
            
            outfile.close();
            fileOut.close();
        } catch(FileNotFoundException e) {
            e.printStackTrace();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void addFunctions(){
        functions.add(new Function(-1, "NONE", DBType.SQL, "#X#", false));
        functions.add(new SQLNaryAndFunction(0, DBType.SQL));
        functions.add(new SQLNaryOrFunction(1, DBType.SQL));
        functions.add(new Function(2, "NOT", DBType.SQL, "(NOT #X#)", true));
        functions.add(new Function(3, "EQUALS", DBType.SQL, "(#X# = #Y#)", true));
        functions.add(new Function(4, "NOT EQUALS", DBType.SQL, "(#X# <> #Y#)", true));
        functions.add(new Function(5, "LESS THAN", DBType.SQL, "(#X# < #Y#)", true));
        functions.add(new Function(6, "LESS THAN OR EQUAL", DBType.SQL, "(#X# <= #Y#)", true));
        functions.add(new Function(7, "GREATER THAN", DBType.SQL, "(#X# > #Y#)", true));
        functions.add(new Function(8, "GREATER THAN OR EQUAL", DBType.SQL, "(#X# >= #Y#)", true));
        functions.add(new Function(9, "BETWEEN", DBType.SQL, "(#X# BETWEEN #Y# AND #Z#)", true));
        functions.add(new Function(10, "LIKE", DBType.SQL, "(#X# LIKE #Y#)", true));
        functions.add(new Function(11, "PLUS", DBType.SQL, "(#X# + #Y#)", false));
        functions.add(new Function(12, "MINUS", DBType.SQL, "(#X# - #Y#)", false));
        functions.add(new Function(13, "MULTIPLY", DBType.SQL, "(#X# * #Y#)", false));
        functions.add(new Function(14, "DIVIDE", DBType.SQL, "(#X# / #Y#)", false));
        functions.add(new Function(15, "MODULUS", DBType.SQL, "MOD(#X#, #Y#)", false));
        functions.add(new Function(16, "ABS", DBType.SQL, "ABS(#X#)", false));
        functions.add(new Function(17, "CEIL", DBType.SQL, "CEIL(#X#)", false));
        functions.add(new Function(18, "POWER", DBType.SQL, "POWER(#X#, #Y#)", false));
        functions.add(new Function(19, "FLOOR", DBType.SQL, "FLOOR(#X#)", false));
        functions.add(new Function(20, "ROUND", DBType.SQL, "ROUND(#X#)", false));
        functions.add(new Function(21, "TRUNC", DBType.SQL, "TRUNC(#X#)", false));
        functions.add(new Function(22, "LOWER", DBType.SQL, "LOWER(#X#)", false));
        functions.add(new Function(23, "UPPER", DBType.SQL, "UPPER(#X#)", false));
        functions.add(new Function(24, "LENGTH", DBType.SQL, "LENGTH(#X#)", false));
        functions.add(new Function(25, "CONCAT", DBType.SQL, "CONCAT(#X#,#Y#)", false));
        functions.add(new Function(26, "TO_CHAR", DBType.SQL, "TO_CHAR(#X#, #Y#)", false));
        functions.add(new Function(27, "TO_DATE", DBType.SQL, "TO_DATE(#X#, #Y#)", false));
        functions.add(new Function(28, "IS_NULL", DBType.SQL, "(#X# IS NULL)", true));
        //functions.add(new SQLCaseFunction(29));
        functions.add(new SQLINFunction(30, DBType.SQL));
        functions.add(new SQLRunDateFunction(31, DBType.SQL));
        functions.add(new SQLRankFunction(32, DBType.SQL));
        functions.add(new Function(33, "TRUNC_DATE", DBType.SQL, "TRUNC(#X#, #Y#)", false));
        functions.add(new Function(34, "ADD_MONTHS", DBType.SQL, "ADD_MONTHS(#X#, #Y#)", false));       
        functions.add(new Function(35, "NVL", DBType.SQL, "NVL(#X#, #Y#)", false));
        functions.add(new Function(36, "TRIM", DBType.SQL, "TRIM(#X#)", false));
        functions.add(new Function(37, "DATEPART", DBType.SQL, "DATEPART(#X#, #Y#)", false));
        functions.add(new Function(38, "DATENAME", DBType.SQL, "DATENAME(#X#, #Y#)", false));
        functions.add(new Function(39, "DATEADD", DBType.SQL, "DATEADD(#X#, #Y#, #Z#)", false));
        functions.add(new Function(40, "DATEDIFF", DBType.SQL, "DATEDIFF(#X#, #Y#, #Z#)", false));
        functions.add(new Function(41, "STRING", DBType.SQL, "STRING(#X#)", false));
        functions.add(new Function(42, "CONCAT", DBType.SQL, "CONCAT(#X#, #Y#)", false));
        functions.add(new Function(43, "SIGN", DBType.SQL, "SIGN(#X#)", false));
        //functions.add(new SQLDecodeFunction(44));
        //functions.add(new SQLCoalesceFunction(45));
        //functions.add(new SQLLikeBeginsWithFunction(46));
        functions.add(new Function(47, "CONCATENATION", DBType.SQL, "(#X# || #Y#)", false));
        
    }
}
