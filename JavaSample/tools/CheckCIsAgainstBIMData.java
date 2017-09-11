package com.amazon.dw.grasshopper.tools;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.amazon.dw.grasshopper.metadata.InMemoryRepository;
import com.amazon.dw.grasshopper.model.ColumnInstance;
import com.amazon.dw.grasshopper.model.Table;

import amazon.platform.config.AppConfig;
import amazon.odin.httpquery.OdinMaterialRetriever;
import amazon.odin.httpquery.model.MaterialPair;

public class CheckCIsAgainstBIMData {
    
    public static void main(String args[]) {
        
        if(args == null || args.length < 1) {
            throw new RuntimeException("Must pass bimdata server connection string as an argument");
        }
        
        CheckCIsAgainstBIMData checker = new CheckCIsAgainstBIMData();
        checker.check(args[0]);
    }
    
    
    private void check(String connectionString) {
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
            
            System.out.println(username + " " + password);
        }
        catch(RuntimeException ex) {
            ex.printStackTrace();
        }
        
        connProperties.put("user", username);
        connProperties.put("password", password);
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
        Connection conn = null;
        
        InMemoryRepository repo = new InMemoryRepository();
        
        try
        {
            conn = DriverManager.getConnection(connectionString, connProperties);
            
            Collection<Table> tables = repo.getAllTables();
            boolean addComma = false;
            StringBuilder inTables = new StringBuilder("(");
            for(Table table : tables) {
                if(addComma) {
                    inTables.append(", ");
                } else {
                    addComma = true;
                }
                inTables.append("'" + table.getPhysicalName().toUpperCase() + "'");
            }
            inTables.append(")");
            
            Map<String, Integer> tableMap = new HashMap<String, Integer>();
            try {
                stmt = conn.prepareStatement("SELECT ID, TABLE_NAME FROM MD_TABLES WHERE TABLE_NAME IN " + inTables.toString());
                rs = stmt.executeQuery();
                while(rs.next()) {
                    tableMap.put(rs.getString("TABLE_NAME"), rs.getInt("ID"));
                }
            } catch(SQLException e) {
                e.printStackTrace();
            } finally {
                if(rs != null) try { rs.close(); } catch(SQLException e) { e.printStackTrace(); } finally { rs = null; }
                if(stmt != null) try { stmt.close(); } catch(SQLException e) { e.printStackTrace(); } finally { stmt = null; }
            }
            
            
            for(Table table : tables) {
                ResultSet subResults = null;
                
                Set<String> columnNames = new HashSet<String>();
                for(ColumnInstance ci : table.getAllAttributesMappingCopy().values()) {
                    columnNames.add(ci.getColumnName().toUpperCase());
                }
                
                try {
                    stmt = conn.prepareStatement("SELECT ID, NAME, DATA_TYPE, DESCRIPTION FROM MD_COLUMNS WHERE TABLE_ID=?");
                    Integer id = tableMap.get(table.getPhysicalName().toUpperCase());
                    if(id != null) {
                        stmt.setInt(1, id);
                        subResults = stmt.executeQuery();
                        while(subResults.next()) {
                            String name = subResults.getString("NAME").toUpperCase();
                            if(columnNames.contains(name)) {
                                columnNames.remove(name);
                            }
                        }
                    } else {
                        System.out.println("Couldn't Find Table: " + table.getTableName() + " (" + table.getPhysicalName() + ")");
                        columnNames.clear();
                    }
                } catch(SQLException e) {
                    e.printStackTrace();
                } finally {
                    if(subResults != null) try { subResults.close(); } catch(SQLException e) { e.printStackTrace(); } finally { subResults = null; }
                    if(stmt != null) try { stmt.close(); } catch(SQLException e) { e.printStackTrace(); } finally { stmt = null; }
                }
                
                if(columnNames.size() > 0) {
                    for(String s : columnNames) {
                        System.out.println(table.getTableName() + " > " + s);
                    }
                }
            }
        } catch(SQLException e) { e.printStackTrace(); }
        finally {
            if(conn != null) try { conn.close(); } catch(SQLException e) { e.printStackTrace(); } finally { conn = null; }
        }
    }
}
