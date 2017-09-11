package com.amazon.dw.grasshopper.tools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.amazon.dw.grasshopper.metadata.InMemoryRepository;
import com.amazon.dw.grasshopper.model.Attribute;
import com.amazon.dw.grasshopper.model.Category;
import com.amazon.dw.grasshopper.model.ColumnType;
import com.amazon.dw.grasshopper.model.Table;

public class CheckMDCorrectness {

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        InMemoryRepository repos = new InMemoryRepository();
        
        List<String> tableNames = repos.getAllTableNames();
        for(String s : tableNames){
            Table t = repos.getTable(s);
            List<Attribute> attrs = t.getAllAttributesCopy();
            if(attrs.size() == 0){
                System.out.println("Issue with table: No attributes. : "+t.getTableName());
            }
            for(Attribute a: attrs){
                if(repos.getAttribute(a.getId())==null){
                    System.out.println("Issue with attribute: Not indexed. : "+a.getId() + "-" + a.getName());
                    
                }
                if(a.getListColumnInstances().size()==0)
                    System.out.println("Issue with attribute: No CI's : "+a.getId() + "-" + a.getName());
            }
            if(t.getAllAttributesCopy().size() != t.getAllAttributesMappingCopy().size())
                System.out.println("Issue with table: size(attributes) != size(CI's) : "+t.getTableName());
            else{
                ArrayList<Long> ids = new ArrayList<Long>();
                for(Attribute a2 : t.getAllAttributesCopy()){
                    if(t.getColumnInstance(a2.getId())==null)
                        System.out.println("Issue with table: Attribute doesn't appear in mapping : "+t.getTableName() + "," + a2.getName()+"-" + a2.getId());
                    ids.add(a2.getId());
                }
                ArrayList<Long> ids2 = new ArrayList<Long>(t.getAllAttributesMappingCopy().keySet());
                Collections.sort(ids);
                Collections.sort(ids2);
                for(int i=0;i<ids.size();i++){
                    if(ids.get(i).equals(ids2.get(i))==false)
                        System.out.println("Issue with table: attribute are different between mapping and list. : "+t.getTableName());
                    
                }
            }
                

        }
        
        for(Long longVal : repos.getAllAttributesId()){
            Attribute a = repos.getAttribute(longVal.longValue());
            if(a != null ){
                if(a.getListColumnInstances().size()==0)
                    System.out.println("Issue with attribute: No CI's : "+a.getId() + "-" + a.getName());
                ArrayList<Table> tables = new ArrayList<Table>(a.getTables());
                for(Table t2: tables){
                    if(repos.getTable(t2.getTableName())==null)
                        System.out.println("Issue with Table: Not indexed. : "+t2.getTableName());
                    
                }
                if(tables.size() != a.getListColumnInstances().size())
                    System.out.println("Issue with attribute: size(tables) != size(CI's) : "+a.getId() + "-" + a.getName());
                else{
                    ArrayList<String> tablenames1 = new ArrayList<String>();
                    for(int j=0;j<tables.size();j++){
                        tableNames.add(tables.get(j).getTableName());
                    }
                    ArrayList<String> tablenames2 = new ArrayList<String>(a.getColumnMappingCopy().keySet());
                    Collections.sort(tablenames1);
                    Collections.sort(tablenames2);
                    for(int j=0;j<tablenames1.size();j++){
                        if(tablenames1.get(j).equalsIgnoreCase(tablenames2.get(j))==false)
                            System.out.println("Issue with table: tables are different between mapping and list. : "+a.getName() +"-"+ a.getId());
                 
                    }
                }
            }
        }
        
        //check if attributes are accessible through categories:
        List<Long> categoriesID = repos.getAllCategoriesId();
        Set<Attribute> attributes = new HashSet<Attribute>(); 
            
        for(Long lCID : categoriesID){
            Category cat = repos.getCategory(lCID.longValue());
            attributes.addAll(cat.getAttributes());
        }
        for(Long longVal : repos.getAllAttributesId()){
            Attribute attr = repos.getAttribute(longVal.longValue());
            if(attributes.contains(attr)==false){
                System.out.println("Issue with attribute: does not appear in any category : "+attr.getName() +"-"+ attr.getId());
                
            }
        }
        
        //are we sure all attributes are joinable
        for(Long longVal : repos.getAllAttributesId()){
            Attribute attr = repos.getAttribute(longVal.longValue());
            if(attr.getColumnType() == ColumnType.NUMERIC && attr.isJoinable()==false && attr.getListColumnInstances().size()>0 && 
                    (attr.getName().toLowerCase().contains("code") || attr.getName().toLowerCase().contains("id") || attr.getName().toLowerCase().contains("group"))){
                System.out.println("Warning: attribute: "+attr.getName() +"-"+ attr.getId() + " marked as non-joinable.");
                
            }
        }
    }

}
