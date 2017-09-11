package com.amazon.dw.grasshopper.tools;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import amazon.odin.httpquery.OdinMaterialRetriever;
import amazon.odin.httpquery.model.MaterialPair;
import amazon.platform.config.AppConfig;

import com.amazon.dw.grasshopper.model.ColumnInstance;
import com.amazon.dw.grasshopper.model.ColumnType;
import com.amazon.dw.grasshopper.model.DBType;
import com.amazon.dw.grasshopper.AreaModel;
import com.amazon.dw.grasshopper.AttributeModel;
import com.amazon.dw.grasshopper.CategoryModel;
import com.amazon.dw.grasshopper.TableModel;

/**
 * NOTE: I'M SEARCHING FOR A WORKAROUND FOR NOW, BUT IN ORDER TO RUN THIS,
 *       YOU MUST MOVE "brazil-config/override/OdinLocal.cfg" TO
 *       "brazil-config/global/OdinLocal.cfg"
 */
public class DSSUIParser
{
    
    private static final String DSS_FILENAME = "dssui2.xml";
    private static final String OUTPUT_FILENAME = "metadata-content/dssOutput2.xml";
    private static final String SPECIAL_ATTR_FILENAME = "metadata-content/specialAttributes2.txt";
    private static final String ATTR_MAP_FILENAME = "metadata-content/dssAttributeMap2.xml";
    private static PrintStream specialOut;
    
    private static String connectionString = "";
    
    private List<Element> reportNodes;
    private List<Element> columnGroupNodes;
    private List<Element> columnNodes;
    private List<Element> tableNodes;
    private List<Element> columnAlternateNodes;
    private List<TableAttributeTuple> tableAttrTuples = new ArrayList<TableAttributeTuple>();
    private List<ColumnInstance> columnInstances = new ArrayList<ColumnInstance>();
    
    private Map<String, CategoryModel> categoryMap = new HashMap<String, CategoryModel>();
    private Map<String, AreaModel> areaMap = new HashMap<String, AreaModel>();
    private Map<String, String> columnAlternates = new HashMap<String, String>();
    private Map<String, AttributeModel> attributeMap = new HashMap<String, AttributeModel>();
    private Map<String, TableModel> tableMap = new HashMap<String, TableModel>();
    private Map<String, AttributeModel> bimAttributes = new HashMap<String, AttributeModel>();
    private Map<String, List<Integer>> tableNameToCINums = new HashMap<String, List<Integer>>();
    
    private Set<String> tablesInArea = new HashSet<String>();
    
    private int areaId = 1;
    private int catId = 1;
    private int attributeId = 1;
    private int ciCount = 0;
    
    public static void main(String[] args)
    {
        if(args == null || args.length < 1) {
            throw new RuntimeException("Must pass bimdata server connection string as an argument");
        }
        
        connectionString = args[0];
        
        try {
            specialOut = new PrintStream(new File(SPECIAL_ATTR_FILENAME));
        } catch (FileNotFoundException e) {
            specialOut = System.out;
            e.printStackTrace();
        }
        
        System.out.println("** START **");
        
        DSSUIParser parser = new DSSUIParser();
        
        System.out.println("...parsing");
        parser.parse(DSS_FILENAME);
        
        System.out.println("...enriching");
        parser.enrichTables();
        
        System.out.println("...writing output file");
        parser.print();
        
        System.out.println("...writing dss mapping file");
        parser.printAttributeMap();
        parser.finish();
        
        specialOut.close();
    }
    
    
    private void printAttributeMap() {
        try {
            PrintStream out = new PrintStream(new File(ATTR_MAP_FILENAME));
            out.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            out.println("<Data>");
            for(String key : attributeMap.keySet()) {
                out.printf("\t<Mapping dssid=\"%s\" attrid=\"%d\"/>%n",
                    key, attributeMap.get(key).getId());
            }
            out.println("</Data>");
            out.close();
        } catch (FileNotFoundException e) {
            System.out.println("Error printing out the attribute mapping");
            e.printStackTrace();
        }
    }


    private void finish() {
        System.out.println("** DONE **%n");
        System.out.printf("Areas: %d%nAttributes: %d%nCategories: %d%n" +
            "Column Instances: %d%nTables: %d%n%n", (areaId - 1),
            (attributeId - 1), (catId - 1), ciCount, tableMap.size());
    }
    
    
    @SuppressWarnings("unchecked")
    private void parse(String filename)
    {
        SAXBuilder builder = new SAXBuilder();
        File xmlFile = new File(filename);
        
        try {
            Document document = (Document) builder.build(xmlFile);
            Element rootNode = document.getRootElement();
            reportNodes = (List<Element>)((Element)rootNode.getChild("REPORTS")).getChildren("REPORT");
            columnGroupNodes = (List<Element>)((Element)rootNode.getChild("COLUMN_GROUPS")).getChildren("COLUMN_GROUP");
            columnNodes = (List<Element>)((Element)rootNode.getChild("COLUMNS")).getChildren("COLUMN");
            tableNodes = (List<Element>)((Element)rootNode.getChild("TABLES")).getChildren("TABLE");
            columnAlternateNodes = (List<Element>)((Element)rootNode.getChild("COLUMN_ALTERNATES")).getChildren("COLUMN_ALTERNATE");
            
            // Map all column aliases to a single attribute id
            for(Element columnAltNode : columnAlternateNodes) {
                String attrNodeId = columnAltNode.getAttributeValue("ID");
                String[] ciNodeIds = columnAltNode.getAttributeValue("COLUMNS").split(",");
                for(String ciNode : ciNodeIds) {
                    columnAlternates.put(ciNode, attrNodeId);
                }
            }
            
            // Create areas, and find categories for those areas
            for(Element reportNode : reportNodes) {
                AreaModel area = new AreaModel();
                area.setId(areaId++);
                area.setName(reportNode.getAttributeValue("DESC"));
                area.setDescription("");
                CategoryModel uncategorized = new CategoryModel();
                uncategorized.setId(catId++);
                uncategorized.setName("Uncategorized");
                categoryMap.put("uncategorized_" + area.getId(), uncategorized);
                
                for(Element module : (List<Element>)reportNode.getChildren("MODULE"))
                {
                    if("0".equals(module.getAttributeValue("TYPE")))
                    {
                        Set<Long> categoryIds = getCategoryIdsFromNodes(module.getAttributeValue("COLUMN_GROUPS"));
                        categoryIds.add(uncategorized.getId());
                        area.setCategories(new LinkedList<Long>(categoryIds));
                        break;
                    }
                }
                
                findMissingColumnInstances(uncategorized);
                tablesInArea.clear();
                areaMap.put(reportNode.getAttributeValue("ID"), area);
            }
            
            // Create all tables
            for(Element tableNode : tableNodes) {
                TableModel model = new TableModel();
                model.setName(tableNode.getAttributeValue("ID"));
                model.setPhysicalName(tableNode.getAttributeValue("NAME"));
                model.setDbType(DBType.SQL.getId());
                tableMap.put(model.getName(), model);
            }
            
        } catch(IOException e) {
            e.printStackTrace();
        } catch(JDOMException e) {
            e.printStackTrace();
        }
    }


    private void findMissingColumnInstances(CategoryModel category)
    {
        for(Element columnNode : columnNodes) {
            String columnId = columnNode.getAttributeValue("ID");
            String alternateName = columnAlternates.get(columnId);
            if(alternateName == null) {
                alternateName = columnId;
            }
            
            String tableName = columnNode.getAttributeValue("TABLE");
            
            if(tablesInArea.contains(tableName)) {
                // This table is in this area, create a new attribute and add it to uncategorized
                Set<Long> attributeIds = new HashSet<Long>();
                List<Long> attributes = category.getAttributes();
                if(attributes != null) {
                    attributeIds.addAll(attributes);
                }
                
                createAttribute(columnNode, columnId, attributeIds);
                category.setAttributes(new LinkedList<Long>(attributeIds));
            }
        }
    }


    private Set<Long> getCategoryIdsFromNodes(String groupNames)
    {
        Set<Long> catIds = new HashSet<Long>();
        
        if(groupNames == null) {
            return catIds;
        }
        
        String[] groupNodeIds = groupNames.split(",");
        for(String groupNodeId : groupNodeIds) {
            for(long catId : getCategoryIdsFromNode(groupNodeId)) {
                if(!catIds.contains(catId)) {
                    catIds.add(catId);
                }
            }
        }
        
        return catIds;
    }


    private Set<Long> getCategoryIdsFromNode(String groupNodeId)
    {
        Set<Long> catIds = new HashSet<Long>();
        
        for(Element columnGroupNode : columnGroupNodes) {
            if(columnGroupNode.getAttributeValue("ID").equals(groupNodeId)) {
                String columns = columnGroupNode.getAttributeValue("COLUMNS");
                
                // If a column group contains any columns, it should be a category
                if(columns != null && columns.length() > 0) {
                    CategoryModel category = categoryMap.get(columnGroupNode.getAttributeValue("ID"));
                    
                    if(category == null) {
                        category = new CategoryModel();
                        category.setId(catId++);
                        categoryMap.put(columnGroupNode.getAttributeValue("ID"), category);
                    }
                    
                    catIds.add(category.getId());
                    
                    category.setName(columnGroupNode.getAttributeValue("DESC"));
                    Set<Long> attributeIds = new HashSet<Long>();
                    List<Long> attributes = category.getAttributes();
                    if(attributes != null) {
                        attributeIds.addAll(attributes);
                    }
                    
                    Set<Long> attributesToAdd = getAttributeIdsFromNodes(columns);
                    attributeIds.addAll(attributesToAdd);
                    
                    category.setAttributes(new LinkedList<Long>(attributesToAdd));
                }
                
                String columnGroups = columnGroupNode.getAttributeValue("COLUMN_GROUPS");
                catIds.addAll(getCategoryIdsFromNodes(columnGroups));
                
                break;
            }
        }
        
        return catIds;
    }
    
    
    // Get all the attribute IDs given a list of <column>s
    private Set<Long> getAttributeIdsFromNodes(String columnNodeNames) {
        
        Set<Long> attributeIds = new HashSet<Long>();
        
        if(columnNodeNames == null) {
            return attributeIds;
        }
        
        String[] columnNodeIds = columnNodeNames.split(",");
        for(String columnNodeId : columnNodeIds) {
            for(long attrId : getAttributeIdsFromNode(columnNodeId)) {
                if(!attributeIds.contains(attrId)) {
                    attributeIds.add(attrId);
                }
            }
        }
        
        return attributeIds;
    }
    
    
    private Set<Long> getAttributeIdsFromNode(String columnNodeName) {
        
        Set<Long> attributeIds = new HashSet<Long>();
        
        for(Element columnNode : columnNodes) {
            if(columnNode.getAttributeValue("ID").equals(columnNodeName)) {
                createAttribute(columnNode, columnNodeName, attributeIds);
                break;
            }
        }
        
        return attributeIds;
    }
    
    
    private void createAttribute(Element columnNode, String columnNodeName,
            Set<Long> attributeIds) {
        
        String tableName = columnNode.getAttributeValue("TABLE");
        String expr = columnNode.getAttributeValue("EXPR");
        
        if(tableName == null) return;
        
        String columnName = "";
        boolean isSpecial = true;
        if(expr == null){
            expr = columnNode.getAttributeValue("EXPR_ORACLE");
            
        }
        if(expr.startsWith(tableName + "."))
        {
            columnName = expr.substring(tableName.length() + 1);
            if(columnName.matches("[0-9a-zA-Z_]+"))
            {
                isSpecial = false;
            }
        }
        
        columnName = columnName.toUpperCase();
        
        if(isSpecial)
        {
            // This is an aggregate or special type of column, print its info and move on
            specialOut.println("ATTRIBUTE: " + columnNode.getAttributeValue("ID") + " has SPECIAL COLUMN: " + expr);
            return;
        }
        
        String alternateName = columnAlternates.get(columnNodeName);
        AttributeModel model = null;
        boolean createCI = true;
        
        if(alternateName == null) {
            alternateName = columnNodeName;
            createCI = false;
        }
        
        model = attributeMap.get(alternateName);
        
        if(model == null) {
            model = new AttributeModel();
            model.setId(attributeId++);
            model.setName(columnNode.getAttributeValue("DESC"));
            model.setDescription("");
            model.setIsJoinable(false);
            model.setAvailability(DBType.SQL.getId());
            
            attributeMap.put(alternateName, model);
            
            createCI = true;
        }
        
        attributeIds.add(model.getId());
        
        if(createCI) {
            String columnTypeString = columnNode.getAttributeValue("TYPE").toLowerCase();
            ColumnType columnType = ColumnType.UNKNOWN;
            
            if(columnTypeString.equals("text")) {
                columnType = ColumnType.CHAR;
            } else if(columnTypeString.equals("currency") ||
                      columnTypeString.equals("curency")) {
                columnType = ColumnType.CURRENCY;
            } else if(columnTypeString.equals("date")) {
                columnType = ColumnType.DATE;
            } else {
                columnType = ColumnType.NUMERIC;
            }
            
            tablesInArea.add(tableName);
            ColumnInstance ci = new ColumnInstance(columnName, columnType);
            tableAttrTuples.add(new TableAttributeTuple(tableName, model));
            columnInstances.add(ci);
            
            List<Integer> tableCIList = tableNameToCINums.get(tableName);
            if(tableCIList == null) {
                tableCIList = new LinkedList<Integer>();
                tableNameToCINums.put(tableName, tableCIList);
            }
            tableCIList.add(ciCount++);
        }
    }


    private static class TableAttributeTuple {
        
        private String tableName;
        private AttributeModel attribute;
        
        public TableAttributeTuple(String tableName, AttributeModel attribute) {
            this.tableName = tableName;
            this.attribute = attribute;
        }
        
        public String getTableName() {
            return tableName;
        }
        
        public AttributeModel getAttribute() {
            return attribute;
        }
    }
    
    
    private void print() {
        File file = new File(OUTPUT_FILENAME);
        PrintStream out = null;
        
        try {
            out = new PrintStream(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        }
        
        out.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        out.println("<Data>");
        out.println("\t<Additions>");
        out.println("\t\t<Tables>");
        for(TableModel model : tableMap.values()) {
            out.printf("\t\t\t<Table name=\"%s\" physicalName=\"%s\" dbType=\"" +
                "%d\" isFactTable=\"%b\" />%n",
                StringEscapeUtils.escapeXml(model.getName()),
                StringEscapeUtils.escapeXml(model.getPhysicalName()),
                model.getDbType(), model.isFactTable());
        }
        out.println("\t\t</Tables>");
        
        out.println("\t\t<Attributes>");
        for(AttributeModel model : attributeMap.values()) {
            out.printf("\t\t\t<Attribute id=\"%d\" name=\"%s\" description=\"%s" +
                "\" isJoinable=\"%b\" availability=\"%d\" />%n",
                model.getId(), StringEscapeUtils.escapeXml(model.getName()),
                StringEscapeUtils.escapeXml(model.getDescription()),
                model.isIsJoinable(), model.getAvailability());
        }
        out.println("\t\t</Attributes>");
        
        out.println("\t\t<ColumnInstances>");
        int len = columnInstances.size();
        for(int i=0; i<len; i++) {
            ColumnInstance ci = columnInstances.get(i);
            TableAttributeTuple tuple = tableAttrTuples.get(i);
            out.printf("\t\t\t<ColumnInstance columnName=\"%s\" ColumnType=\"%d" +
                "\" AttrId=\"%d\" Table=\"%s\" />%n",
                StringEscapeUtils.escapeXml(ci.getColumnName()),
                ci.getColumnType().getId(), tuple.getAttribute().getId(),
                StringEscapeUtils.escapeXml(tuple.getTableName()));
        }
        out.println("\t\t</ColumnInstances>");
        
        out.println("\t\t<Areas>");
        for(AreaModel model : areaMap.values()) {
            out.printf("\t\t\t<Area id=\"%d\" name=\"%s\" description=\"%s\" />%n",
                model.getId(), StringEscapeUtils.escapeXml(model.getName()),
                StringEscapeUtils.escapeXml(model.getDescription()));
        }
        out.println("\t\t</Areas>");
        
        out.println("\t\t<Categories>");
        for(CategoryModel model : categoryMap.values()) {
            
            out.printf("\t\t\t<Category catId=\"%d\" name=\"%s\" />%n",
                model.getId(), StringEscapeUtils.escapeXml(model.getName()));
        }
        out.println("\t\t</Categories>");
        
        out.println("\t\t<AreaCats>");
        for(AreaModel model : areaMap.values()) {
            for(long id : model.getCategories()) {
                out.printf("\t\t\t<AreaCat areaId=\"%d\" catId=\"%d\" />%n", model.getId(), id);
            }
        }
        out.println("\t\t</AreaCats>");
        
        out.println("\t\t<CategoryAttrs>");
        for(CategoryModel model : categoryMap.values()) {
            for(long id : model.getAttributes()) {
                out.printf("\t\t\t<CategoryAttr catId=\"%d\" attrId=\"%d\" />%n", model.getId(), id);
            }
            
        }
        out.println("\t\t</CategoryAttrs>");
        
        
        

        out.println("\t</Additions>");
        out.println("</Data>");
        
        out.close();
    }
    
    
    private void enrichTables() {
        int startAttribute = attributeId;
        for(TableModel model : tableMap.values()) {
            enrich(model);
        }
        
        List<Long> attributeIds = new LinkedList<Long>();
        for(long i=startAttribute; i<attributeId; i++) {
            attributeIds.add(i);
        }
        
        CategoryModel bimCat = new CategoryModel();
        bimCat.setId(catId++);
        bimCat.setName("BIMetadata Additions");
        bimCat.setAttributes(attributeIds);
    }
    
    
    private void enrich(TableModel table) {
        
        if(table == null) return;
        
        Map<String, AttributeModel> attrMap = new HashMap<String, AttributeModel>();
        
        List<Integer> ciNums = tableNameToCINums.get(table.getName());
        if(ciNums == null) {
            ciNums = new LinkedList<Integer>();
            tableNameToCINums.put(table.getName(), ciNums);
        }
        
        for(int i : tableNameToCINums.get(table.getName())) {
            attrMap.put(columnInstances.get(i).getColumnName(), tableAttrTuples.get(i).getAttribute());
        }
        
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
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
        Connection conn = null;
        
        try
        {
            conn = DriverManager.getConnection(connectionString, connProperties);
            stmt = conn.prepareStatement("SELECT ID, TABLE_NAME, TABLE_TYPE_ID FROM MD_TABLES where TABLE_NAME=?");
            stmt.setString(1, table.getPhysicalName().toUpperCase());
            rs = stmt.executeQuery();
            if(rs.next()) {
                table.setFactTable(rs.getInt("TABLE_TYPE_ID") == 2);
                
                int tableId = rs.getInt("ID");
                
                Set<Integer> queryKeys = new HashSet<Integer>();
                ResultSet subResults = null;
                
                try {
                    stmt = conn.prepareStatement("SELECT COLUMN_ID FROM MD_TABLE_PARTITION_KEYS WHERE TABLE_ID=?");
                    stmt.setInt(1, tableId);
                    subResults = stmt.executeQuery();
                    while(subResults.next()) {
                        queryKeys.add(subResults.getInt("COLUMN_ID"));
                    }
                } catch(SQLException e) {
                    e.printStackTrace();
                } finally {
                    if(subResults != null) try { subResults.close(); } catch(SQLException e) { e.printStackTrace(); } finally { subResults = null; }
                    if(stmt != null) try { stmt.close(); } catch(SQLException e) { e.printStackTrace(); } finally { stmt = null; }
                }
                
                try {
                    stmt = conn.prepareStatement("SELECT RECOMMENDED_QUERY_KEY FROM MD_TABLE_RECOM_QUERY_KEYS WHERE TABLE_ID=?");
                    stmt.setInt(1, tableId);
                    subResults = stmt.executeQuery();
                    while(subResults.next()) {
                        queryKeys.add(subResults.getInt("RECOMMENDED_QUERY_KEY"));
                    }
                } catch(SQLException e) {
                    e.printStackTrace();
                } finally {
                    if(subResults != null) try { subResults.close(); } catch(SQLException e) { e.printStackTrace(); } finally { subResults = null; }
                    if(stmt != null) try { stmt.close(); } catch(SQLException e) { e.printStackTrace(); } finally { stmt = null; }
                }
                
                try {
                    stmt = conn.prepareStatement("SELECT COLUMN_ID FROM MD_TABLE_PRIMARY_KEY_COLUMNS WHERE TABLE_ID=?");
                    stmt.setInt(1, tableId);
                    subResults = stmt.executeQuery();
                    while(subResults.next()) {
                        queryKeys.add(subResults.getInt("COLUMN_ID"));
                    }
                } catch(SQLException e) {
                    e.printStackTrace();
                } finally {
                    if(subResults != null) try { subResults.close(); } catch(SQLException e) { e.printStackTrace(); } finally { subResults = null; }
                    if(stmt != null) try { stmt.close(); } catch(SQLException e) { e.printStackTrace(); } finally { stmt = null; }
                }
                
                try {
                    stmt = conn.prepareStatement("SELECT ID, NAME, DATA_TYPE, DESCRIPTION FROM MD_COLUMNS WHERE TABLE_ID=?");
                    stmt.setInt(1, tableId);
                    subResults = stmt.executeQuery();
                    while(subResults.next()) {
    
                        int id = subResults.getInt("ID");
                        AttributeModel attr = attrMap.get(subResults.getString("NAME"));
                        
                        if(queryKeys.contains(id) || attr != null) {
                            
                            boolean hadAttribute = false;
                            String name = subResults.getString("NAME");
                            
                            for(int i : ciNums) {
                                AttributeModel a = tableAttrTuples.get(i).getAttribute();
                                if(columnInstances.get(i).getColumnName().equals(name)) {
                                    a.setDescription(subResults.getString("DESCRIPTION"));
                                    a.setIsJoinable(queryKeys.contains(id));
                                    hadAttribute = true;
                                }
                            }
                            
                            if(!hadAttribute) {
                                
                                if(bimAttributes.containsKey(name)) {
                                    attr = bimAttributes.get(name);
                                }
                                else {
                                    attr = new AttributeModel();
                                    attr.setId(attributeId++);
                                    attr.setName(name);
                                    attr.setDescription(subResults.getString("DESCRIPTION"));
                                    attr.setAvailability(DBType.SQL.getId());
                                    attr.setIsJoinable(true);
                                    
                                    bimAttributes.put(name, attr);
                                    attributeMap.put("attr" + attr.getId(), attr);
                                }
                            }
                            
                            ColumnType columnType;
                            String typeString = subResults.getString("DATA_TYPE");
                            
                            if(typeString.equals("FLOAT") || typeString.equals("NUMBER") || typeString.equals("LONG")) {
                                columnType = ColumnType.NUMERIC;
                            } else if(typeString.equals("CHAR") || typeString.equals("VARCHAR2")) {
                                columnType = ColumnType.CHAR;
                            } else if(typeString.startsWith("TIME") ||
                                typeString.startsWith("INTERVAL") || typeString.equals("DATE")) {
                                columnType = ColumnType.DATE;
                            } else {
                                columnType = ColumnType.UNKNOWN;
                            }
                            
                            ColumnInstance colInstance = new ColumnInstance(subResults.getString("NAME"), columnType);
                            TableAttributeTuple tuple = new TableAttributeTuple(table.getName(), attr);
                            tableAttrTuples.add(tuple);
                            columnInstances.add(colInstance);
                            ciNums.add(ciCount++);
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
        } finally {
            if(rs != null) try { rs.close(); } catch(SQLException e) { e.printStackTrace(); }
            if(stmt != null) try { stmt.close(); } catch(SQLException e) { e.printStackTrace(); }
            if(conn != null) try { conn.close(); } catch(SQLException e) { e.printStackTrace(); }
        }
    }
}
