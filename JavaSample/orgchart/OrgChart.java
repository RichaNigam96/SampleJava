package com.amazon.dw.grasshopper.orgchart;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class OrgChart {
    
    // Mapping between a user and their direct manager
    private Map<String, String> directManagers = new HashMap<String, String>();
    
    
    /**
     * Gets the entire management chain for the given user, sorted with most
     * direct managers first and most distant managers last. The user is included
     * as the first element in the list.
     * 
     * @param user
     * @return the management chain for the given user
     */
    public List<String> getManagersForUser(String user) {
        List<String> managerList = new LinkedList<String>();
        if(user != null) {
            String mgr = user;
            
            do {
                managerList.add(mgr);
                mgr = directManagers.get(mgr);
            } while(mgr != null);
        }
        
        return managerList;
    }
    
    
    /**
     * Sets the direct manager for the given user to the given manager
     * 
     * @param user
     * @param mgr the user's direct manager
     * @throws NullPointerException if either parameter is null
     */
    public void setDirectManagerForUser(String user, String mgr) throws NullPointerException {
        Objects.requireNonNull(user);
        Objects.requireNonNull(mgr);
        
        directManagers.put(user, mgr);
    }
}
