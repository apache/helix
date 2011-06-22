package com.linkedin.clustermanager.store;

public interface PropertyChangeListener<T>
{
    /**
     * Callback function when there a change in any property that starts with key
     * Its upto the implementation to handle the following different cases
     *  * key is a simple key and does not have any children. PropertyStore.get(key) must be used to retrieve the value
     *  * key is a prefix and has children. PropertyStore.getProperties(key) must be used to retrieve all the children
     * Its important to know that PropertyStore will not be able to provide the delta[old value,new value] or what child was added/deleted
     * The implementation must take care of the fact that there might be callback for every child thats added/deleted.
     * General way applications handle this is keep a local cache of keys and compare against the latest keys. 
     * @param key
     */
    void onPropertyChange(String key);
}
