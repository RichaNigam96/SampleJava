package com.amazon.dw.grasshopper.tools;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

import com.amazon.dw.grasshopper.metadata.InMemoryRepository;
import com.amazon.dw.grasshopper.metadata.MetaDataStorageType;
import com.amazon.dw.grasshopper.model.Join;
import com.amazon.dw.grasshopper.model.JoinCardinality;
import com.amazon.dw.grasshopper.model.Table;

/**
 * A driver that reads the DSS JOIN conditions from dssui.xml and creates GrassHopper representation of that info.
 * Insert cardinality data into InMemoryRepository, and saves to a temporary file.
 * @author srebrnik
 *
 */
public class DSSUIJoinsImport {
    
    private static final String DSS_FILENAME = "dssui.xml";
    private List<Element> tableJoins;
    private InMemoryRepository repos;
    
    public static void main(String [] args){
        try{
            DSSUIJoinsImport importer = new DSSUIJoinsImport();
            importer.importJoins();
            importer.performAndPrintChanges();
            importer.saveToFile();
        }
        catch(Exception ex){
            ex.printStackTrace();
        }
        
    }
    
    public DSSUIJoinsImport(){
        tableJoins = null;
        repos = new InMemoryRepository();
    }
    
    @SuppressWarnings("unchecked")
    public void importJoins(){
        SAXBuilder builder = new SAXBuilder();
        File xmlFile = new File(DSS_FILENAME);
        
        try {
            Document document = (Document) builder.build(xmlFile);
            Element rootNode = document.getRootElement();
            tableJoins = (List<Element>)((Element)rootNode.getChild("TABLE_JOINS")).getChildren("TABLE_JOIN");
        }
        catch(Exception ex){
            System.out.println(ex.getMessage());
            ex.printStackTrace();
        }
    }
    
    public void performAndPrintChanges(){
        if(tableJoins==null)
            System.out.println("No table-joins Located.");
        else{
             System.out.println("<JoinCardinalityRecords>");
             for(Element elem : tableJoins)
                 processElement(elem);
             System.out.println("</JoinCardinalityRecords>");
          }
    }
    
    public void saveToFile(){
        if(tableJoins==null)
            return;
        SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");
        String date = dateFormat.format(new Date());
        String filename = "metadata-content/repo-DSSUI-Join-Importer-" + date + ".xml";
        repos.saveTo(MetaDataStorageType.XML, filename);
        System.out.println("Repository Backed Up To: " + filename);
    }
    
    private void processElement(Element elem){
       /* a DSS line looks like: TABLES="adma,marketplaces" RELATIONSHIP="ManyToOne" */
        String tablesStr = elem.getAttribute("TABLES").getValue();
        String relationStr = elem.getAttribute("RELATIONSHIP").getValue();
        String[] joinExpressions = elem.getAttribute("EXPR").getValue().toLowerCase().split(" and ");
        
        String[] tables = tablesStr.split(",");
        if(tables.length != 2){
            return;
        }
        
        // verifies the tables actually exists.
        Table t1 = repos.getTable(tables[0].trim());
        Table t2 = repos.getTable(tables[1].trim());
        if(t1==null || t2==null)
        {
            StringBuilder builder = new StringBuilder();
            if(t1==null){
                builder.append("Table " + tables[0].trim() + " does not exist.");
            }
            if(t2==null){
                builder.append("Table " + tables[1].trim() + " does not exist.");
            }
            System.out.println("<!-- Error in: "+ tablesStr + "-" + builder.toString() + "-->");
            return;
        }
            
        // parse EXPR to understand whether it's an INNER/RIGHT_OUTER/LEFT_OUTER join.
        // handles the case where the tables join expression is other than the order in the "TABLES" attribute.
        boolean outerInRight = false;
        boolean outerInLeft = false;
        StringBuilder constantJoinExpressions = new  StringBuilder();
        for(String s : joinExpressions){
            String[] elements = s.split("=");
            if(elements.length == 2){
                if(elements[0].contains("(+)")){
                        if(elements[0].contains(tables[0]))
                            outerInLeft = true;
                        else
                            outerInRight = true;
                }
                if(elements[1].contains("(+)")){
                    if(elements[1].contains(tables[1]))
                        outerInRight = true;
                    else
                        outerInLeft = true;
                }
                
                // expressions in where clause which don't contain both tables. (i.e probably "TBL1.columnA = const_value" )
                if(elements[1].contains(tables[1])==false && elements[1].contains(tables[0])==false)
                {
                    if(constantJoinExpressions.length() > 0)
                        constantJoinExpressions.append(" AND " + s);
                    else
                        constantJoinExpressions.append(s);
                }
            }
        }
        
        // Determine what is the JOIN used in the DSS expression.
        String strJoinType = "INNER";
        if(outerInLeft && !outerInRight){
            if(relationStr.equals("ManyToOne"))
                strJoinType = "ILLOGICAL";
            else
                strJoinType = "LEFT OUTER JOIN";
        }
        else if(!outerInLeft && outerInRight){
            if(relationStr.equals("OneToMany"))
                strJoinType = "ILLOGICAL";
            else
                strJoinType = "RIGHT OUTER JOIN";
        }
        else if(outerInLeft && outerInRight)
            strJoinType = "FULL OUTER JOIN";
        
       
        //Using the DSS-JOIN and cardinality, determine the GH cardinality
        JoinCardinality card = JoinCardinality.NOT_JOINABLE;
        if(relationStr.equals("OneToOne")){
            if(strJoinType.equals("INNER")){
                card = JoinCardinality.ONE_TO_ONE;
               
            }
            else if(strJoinType.contains("LEFT")){
                card = JoinCardinality.ONE_TO_ZERO_OR_MORE;
                
            }
            else if(strJoinType.contains("RIGHT")){
                card = JoinCardinality.ONE_TO_ONE;
                
            }
        } 
        else if(relationStr.equals("ManyToOne")){
            if(strJoinType.equals("INNER")){
                card = JoinCardinality.ONE_TO_MANY;
                
            }
            
            else if(strJoinType.contains("LEFT")){
                System.out.println("ILLOGICAL!");
               
            }else if(strJoinType.contains("RIGHT")){
                card = JoinCardinality.ONE_TO_MANY;
            }
            
        }else if(relationStr.equals("OneToMany")){
            if(strJoinType.equals("INNER")){
               card = JoinCardinality.ONE_TO_MANY;
            }
            else if(strJoinType.contains("LEFT")){
                System.out.println("ILLOGICAL!");
               
            }else if(strJoinType.contains("RIGHT")){
                System.out.println("ILLOGICAL!");
            }
        }
        
        System.out.println("<JoinCardinalityRecord Tables=\""+tablesStr+"\" Cardinality=\"" + card.getDescription() + "\"/>");
        repos.addCardinality(t1, t2, Join.UNASSIGNED_JOIN_AREA_ID, card);
        
    }
}
