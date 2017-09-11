package com.amazon.dw.grasshopper.tools;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;


import amazon.odin.httpquery.OdinMaterialRetriever;
import amazon.odin.httpquery.model.MaterialPair;
import amazon.platform.config.AppConfig;

import com.amazon.dw.grasshopper.metadata.InMemoryRepository;
import com.amazon.dw.grasshopper.model.Table;
import com.amazon.dw.grasshopper.model.Attribute;

public class AddPartitionKeysToMetaData {
    private static final String SPECIAL_ATTR_FILENAME = "metadata-content/specialAttributes.txt";
    private static PrintStream specialOut;
    private static InMemoryRepository repo = new InMemoryRepository();
    
    
    private static String connectionString = "";
    
    public static void main(String[] args)
    {
        if(args == null || args.length < 2) {
            throw new RuntimeException("Must pass bimdata server connection string as an argument and the remove all flag");
        }       
        connectionString = args[0];
        boolean removeAllFlag = Boolean.parseBoolean(args[1]);

        if(removeAllFlag){
           repo = removeAllPartitionKeysFromMetadata(repo);
        }
        
        try {

            Properties connProperties = genProperties();
            
            for(String tName: repo.getAllTableNames()){
                addTablePartKeys(tName, connProperties);
            }
            
            specialOut = new PrintStream(new File(SPECIAL_ATTR_FILENAME));
        } catch (FileNotFoundException e) {
            specialOut = System.out;
            e.printStackTrace();
        }
        
        System.out.println("** START **");
        repo.save();
        System.out.println("** DONE **");
               
        specialOut.close();
    }
    
    private static Properties genProperties(){
        Properties connProperties = new Properties();
        String username = "";
        String password = "";
        
        if(!AppConfig.isInitialized()) {
            String cwd = null;
            try {
                cwd = new File(".").getCanonicalPath();
            }
            catch(Exception e) { }
            
            String[] args = new String[] {"--root="+cwd, "--domain=test", "--realm=USAmazon"};
            AppConfig.initialize("APPNAME", null, args);
        }
        
        try {
            OdinMaterialRetriever retriever = new OdinMaterialRetriever();
            MaterialPair certificateMaterial = retriever.retrievePair("com.amazon.grasshopper.bimkey", 1);
            byte[] privateArr = certificateMaterial.getPrivateMaterial().getMaterialData();
            username = new String(privateArr);
            
            certificateMaterial = retriever.retrievePair("com.amazon.grasshopper.bimkey", 2);
            privateArr = certificateMaterial.getPrivateMaterial().getMaterialData();
            password = new String(privateArr);
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        
        connProperties.put("user", username);
        connProperties.put("password", password);
        return connProperties;
    }
    
    private static void addTablePartKeys(String tableName, Properties connProperties) {
        
        Table table = repo.getTable(tableName);

        if(table == null)
            return;
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
        Connection conn = null;
        
        try
        {
            conn = DriverManager.getConnection(connectionString, connProperties);
            stmt = conn.prepareStatement("SELECT ID FROM MD_TABLES where TABLE_NAME=?");
            stmt.setString(1, table.getPhysicalName().toUpperCase());
            rs = stmt.executeQuery();
            if(rs.next()) {
                
                int tableId = rs.getInt("ID");
                ResultSet subResults = null;
                
                try {
                    stmt = conn.prepareStatement("SELECT C.NAME FROM MD_TABLE_PARTITION_KEYS P, MD_COLUMNS C WHERE P.COLUMN_ID = C.ID AND C.TABLE_ID = P.TABLE_ID AND P.TABLE_ID =?");
                    stmt.setInt(1, tableId);
                    subResults = stmt.executeQuery();
                    while(subResults.next()) {
                         //for each partition key for this table, edit the metadata to include it
                        String colName = subResults.getString("NAME");                      
                        //Find appropriate attribute to update first
                        for(Attribute a: table.getAttributes()){
                            if(a.getColumnInstance(tableName).getColumnName().equals(colName))
                                table.addExistingAttributeAsPartitionKey(a);
                        }

                    }
                    
                } catch(SQLException e) {
                    e.printStackTrace();
                } finally {
                    if(subResults != null) try { subResults.close(); } catch(SQLException e) { e.printStackTrace(); } finally { subResults = null; }
                    if(stmt != null) try { stmt.close(); } catch(SQLException e) { e.printStackTrace(); } finally { stmt = null; }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally{                    
            if(stmt != null) try { stmt.close(); } catch(SQLException e) { e.printStackTrace(); } finally { stmt = null; }
            if(rs != null) try { rs.close(); } catch(SQLException e) { e.printStackTrace(); } finally { rs = null; }
            if(conn != null) try{conn.close();} catch(SQLException e) {e.printStackTrace();} finally{ conn =null;}
        }
    }
        /*
         * Sets the field IsPartitionKey for every column in each table in the metadata to false.
         */
        private static InMemoryRepository removeAllPartitionKeysFromMetadata(InMemoryRepository repo){
            for(Table t: repo.getAllTables()){
                t.clearAllPartitionKeys();
            }
            
            return repo;
        }           

}
