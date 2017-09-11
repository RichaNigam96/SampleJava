package com.amazon.dw.grasshopper.tools;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import com.amazon.dw.grasshopper.metadata.InMemoryRepository;
import com.amazon.dw.grasshopper.metadata.InMemoryRepositoryManager;
import com.amazon.dw.grasshopper.model.Area;
import com.amazon.dw.grasshopper.model.Attribute;
import com.amazon.dw.grasshopper.model.Category;
import com.amazon.dw.grasshopper.model.ColumnInstance;
import com.amazon.dw.grasshopper.model.Join;
import com.amazon.dw.grasshopper.model.JoinCardinality;
import com.amazon.dw.grasshopper.model.Table;

import java.util.Collections;

public class GHCLI {
    InMemoryRepository repos;
    public GHCLI(){
        InMemoryRepositoryManager manager = new InMemoryRepositoryManager(null, null);
        repos = (InMemoryRepository)manager.getStore("global");
    }
    /**
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("*****************************************************************");
        System.out.println("************* GrassHopper MetaData Query CLI Tool ***************");
        System.out.println("*****************************************************************");
        
                
        GHCLI cli = new GHCLI();
        try{
            if(args.length == 2){
                if(args[0].equalsIgnoreCase("-t"))
                {
                    cli.printAllColumnsOfTable(args[1]);
                    System.exit(0);
                }
                else if(args[0].equalsIgnoreCase("-a")){
                    cli.printAllInstancesOfAttribute(Long.parseLong(args[1]));
                    System.exit(0);
                }
                else if(args[0].equalsIgnoreCase("-c")){
                    cli.printAllAttributesWithColumn(args[1]);
                    System.exit(0);
                }
                else if(args[0].equalsIgnoreCase("-ta")){
                    cli.printAllReachableTables(args[1], Join.UNASSIGNED_JOIN_AREA_ID);
                    System.exit(0);
                }
                else if(args[0].equalsIgnoreCase("-aa")){
                    cli.printAllReachableAttributes(args[1], Join.UNASSIGNED_JOIN_AREA_ID);
                    System.exit(0);
                }
                else if(args[0].equalsIgnoreCase("-cat")){
                    cli.printAllAttributesInCategory(Long.parseLong(args[1]));
                    System.exit(0);
                }
                else if(args[0].equalsIgnoreCase("-area")){
                    cli.printAllCategoriesInArea(Long.parseLong(args[1]));
                    System.exit(0);
                }
           }
           else if(args.length == 3 && args[0].equalsIgnoreCase("-j")){
               if(args[0].equalsIgnoreCase("-ta")) {
                   cli.printAllReachableTables(args[1], Long.parseLong(args[2]));
                   System.exit(0);
               } else if(args[0].equalsIgnoreCase("-aa")) {
                   cli.printAllReachableAttributes(args[1], Long.parseLong(args[2]));
                   System.exit(0);
               } else if(args[0].equalsIgnoreCase("-j")) {
                   cli.printJoin(args[1], args[2], Join.UNASSIGNED_JOIN_AREA_ID);
                   System.exit(0);
               }
           } else if(args.length == 4 && args[0].equalsIgnoreCase("-j")) {
               cli.printJoin(args[1], args[2], Long.parseLong(args[3]));
               System.exit(0);
           }
        }
        catch(Exception ex){
            System.err.print(ex.toString());
            ex.printStackTrace();
        }
        
       System.out.println("Supported parameters:");
       System.out.println("-t <TableName>  : Prints all Attributes-ColumnInstances in the table");
       System.out.println("-a <AttributeID>  : Prints all the Tables-ColumnInstances that contain the attribute.");
       System.out.println("-c <ColumnName>   : Prints all the Attribute-Tables that have a column with that name.");
       System.out.println("-ta <TableName> : Prints all the Tables accessible from driving-table");
       System.out.println("-aa <TableName> : Prints all the Attributes accessible from driving-table");
       System.out.println("-j <TableName1> <TableName2> : Prints the Joined attributes between two tables.");
       System.out.println("-cat <CategoryId> : Prints the attributes in a category.");
       System.out.println("-area <areaId> : Prints the categories in an area.");
       System.exit(0);
    
           
    }
    
    
    public void printAllAttributesWithColumn(String columnName){
        Collection<Long> attrIds = repos.getAllAttributesId();
        System.out.println("\nPrinting all ColumnInstances with columnName = " + columnName);
        System.out.println("---------------------------------------------------------------------");
        for(Long l : attrIds){
            Attribute attr = repos.getAttribute(l.longValue());
            Map<String, ColumnInstance> ciMap = attr.getColumnMappingCopy();
            for(Map.Entry<String,ColumnInstance> s : ciMap.entrySet()){
                
                if(s.getValue().getColumnName().equalsIgnoreCase(columnName)){
                    Table t = repos.getTable(s.getKey());
                    System.out.println("Attribute: " + attr.getName() + " (" + attr.getId() + ") \t\tTable: " + t.getTableName() + " (" + t.getPhysicalName() +")" );
                }
            }
        }
    }
    
    
    public void printAllColumnsOfTable(String tableName){
        ArrayList<Table> tables = new ArrayList<Table>();
        Table t = repos.getTable(tableName);
        if(t==null){
            for(Table t2 : repos.getAllTables()){
                if(t2.getPhysicalName().equalsIgnoreCase(tableName))
                    tables.add(t2);
            }
        }
        else
            tables.add(t);
        
        for(Table table : tables){
            System.out.println("Table: " + table.getTableName() + " (" + table.getPhysicalName() + ")" + "\t\tisFactTable:" + table.isFactTable());
            System.out.println("-------------------------------------------------------------------");
            for(Attribute attr : table.getAllAttributesCopy()){
                ColumnInstance ci= table.getColumnInstance(attr.getId());
                boolean isCIPK = false;
                Collection<Attribute> pks = table.getPartitionKeys();
                for(Attribute pkCandidate : pks){
                    if(pkCandidate.getId() == attr.getId())
                        isCIPK = true;
                }
                System.out.println("\tAttribute: " + attr.getName() + "("+attr.getId() +") \tColumn: " + ci.getColumnName() + "\tColumnType: " + ci.getColumnType() +"\tIsPartitionKey: " + isCIPK);
            }
        }
        
    }
    
    public void printAllInstancesOfAttribute(long attrId){
        Attribute attr = repos.getAttribute(attrId);
        if(attr==null){
            return;
        }
        
        System.out.println("Attribute: " + attr.getName() + "("+attr.getId() +")");
        Map<String, ColumnInstance> map = attr.getColumnMappingCopy();
        for(Table t :attr.getTables()){
            boolean isCIPK = false;
            Collection<Attribute> pks = t.getPartitionKeys();
            for(Attribute pkCandidate : pks){
                if(pkCandidate.getId() == attr.getId())
                    isCIPK = true;
            }
            ColumnInstance ci = map.get(t.getTableName());
            System.out.println("\tTable: " + t.getTableName() + " (" + t.getPhysicalName() + ") \tColumn: " + ci.getColumnName() + "\tIsPartitionKey: " + isCIPK);
        }
    }
    
    
    protected HashSet<Table> getListOfConnectedTables(Table drivingTable, long areaId){
        HashSet<Table> reachableTables = new HashSet<Table>();
        reachableTables.add(drivingTable);
        ArrayList<Table> thisIteration = new ArrayList<Table>();
        
        //calculating transitive closure
        while(true){
            for(Table t1 : reachableTables){
                for(Table t2 : repos.getAllTables()){
                    if(repos.getJoinCardinality(t1, t2 ,areaId)!= JoinCardinality.NOT_JOINABLE && reachableTables.contains(t2)==false)
                        thisIteration.add(t2);
                }
            }
            if(thisIteration.size() == 0)
                break;
            else{
                reachableTables.addAll(thisIteration);
                thisIteration.clear();
            }
        }
        
        return reachableTables;
        
    }
    
    public void printAllReachableTables(String drivingTable, long areaId){
        Table t = repos.getTable(drivingTable);
        if(t==null)
            return;
        HashSet<Table> reachableTables = getListOfConnectedTables(t, areaId);
        System.out.println("All Tables reachable from Driving-Table " + t.getTableName() + " (" + t.getPhysicalName() +")");
        for(Table t3: reachableTables){
            System.out.println("\tTable: " + t3.getTableName() + " (" + t3.getPhysicalName() +")");
        }
        
    }
    
    public void printAllReachableAttributes(String drivingTable, long areaId){
        Table t = repos.getTable(drivingTable);
        if(t==null)
            return;
        HashSet<Table> reachableTables = getListOfConnectedTables(t, areaId);
        System.out.println("All Tables reachable from Driving-Table " + t.getTableName() + " (" + t.getPhysicalName() +")");
        
        ArrayList<Attribute> reachableAttr = new ArrayList<Attribute>();
        HashSet<Attribute> attributes = new HashSet<Attribute>();
        for(Table t3: reachableTables){
            for(Attribute a2 : t3.getAllAttributesCopy()){
                if(attributes.contains(a2)==false){
                    reachableAttr.add(a2);
                    attributes.add(a2);
                }
                    
            }
        }
        
        Collections.sort(reachableAttr);
        System.out.println("Printing all attributes reachable from driving table" + t.getTableName() + " (" + t.getPhysicalName() +")");
        for(Attribute attr : reachableAttr){
            System.out.println("\tAttribute: " + attr.getName() + " (" + attr.getId() + ")");
        }
    }
    
    public void printJoin(String tb1, String tbl2, long areaId){
        Table t1 = repos.getTable(tb1);
        Table t2 = repos.getTable(tbl2);
        if(t1 != null && t2 != null){
            Collection<Attribute> shared = t1.getSharedAttributes(t2);
            JoinCardinality card = repos.getJoinCardinality(t1, t2, areaId);
            System.out.println();
            System.out.println("Join-Cardinality "+ t1.getTableName() + " (" + t1.getPhysicalName() +") - " +t2.getTableName() + " (" + t2.getPhysicalName() +") : " + card.getDescription());
            System.out.println("Shared Join Attributes:");
            for(Attribute attr : shared){
                if(attr.isJoinable())
                    System.out.println("\tAttribute: " + attr.getName() + " (" + attr.getId() + ")");
            }
        }
        else
        {
            System.out.println("Error printing join, check whether tables exist.");
        }
        
    }
    
    public void printAllAttributesInCategory(long catId){
        Category cat = repos.getCategory(catId);
        System.out.println("Printing all attributes for Category "+ cat.getName() + "("+cat.getId()+")");
        for(Attribute attr : cat.getAttributes()){
            System.out.println("\tAttribute: " + attr.getName() + " (" + attr.getId() + ")");
        }
    }
    
    public void printAllCategoriesInArea(long areaId){
        Area area = repos.getArea(areaId);
        
        System.out.println("Printing all categories for Area "+ area.getName() + "("+area.getId()+")");
        for(Category cat : area.getCategories()){
            System.out.println("\tCategory: " + cat.getName() + " (" + cat.getId() + ")");
        }
    }

}
