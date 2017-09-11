package com.amazon.dw.grasshopper.userPreferences;

public class UserPreferencesConfig {
    private static volatile UserPreferencesConfig instance = null;
    private static Object lockObj = new Object();
    
    private int cacheSize;
    private int userPreferenceListSize;
    private float loadFactor;
    
    private UserPreferencesConfig(){
        if(instance == null){
            synchronized (lockObj) {
                if(instance == null)
                    instance = this;
            }
        }
    }
    
    public static UserPreferencesConfig getInstance(){
        return instance;
    }
    
    public int getCacheSize() {
        return cacheSize;
   }

   public float getLoadFactor() {
       return loadFactor;
   }
   
   public void setCacheSize(int cacheSize) {
       this.cacheSize = cacheSize;
   }

   public void setLoadFactor(float loadFactor) {
       this.loadFactor = loadFactor;
   }

public int getUserPreferenceListSize() {
    return userPreferenceListSize;
}

public void setUserPreferenceListSize(int userPreferenceListSize) {
    this.userPreferenceListSize = userPreferenceListSize;
}

}
