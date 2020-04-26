package simpledb;

import java.util.*;

// Lock class designed to track information pertaining each page's lock
// Information to be used by the TransactionManager

public class Lock {
    
    public LockType lt;
    public int numReaders;
    public int numWriters;
    private Set<TransactionId> accessors;      // transactions using the lock
    private Map<TransactionId, LockType> waitingList;

    public Lock() {
        this.lt = LockType.READ;
        this.numReaders = 0;
        this.numWriters = 0;
        this.accessors = new HashSet<TransactionId>();
        this.waitingList = new HashMap<TransactionId, LockType>();
    }

    public enum LockType {
		READ, WRITE
    }
    
    public Map<TransactionId, LockType> getWaitingList() {
        return this.waitingList;
    }

    public Set<TransactionId> getAccessors() {
        return this.accessors;
    }

    public void print_waitlist() {
        for (TransactionId tid : this.waitingList.keySet()) {
            LockType x = waitingList.get(tid);
            if (x == Lock.LockType.WRITE) {
                System.out.println("TID <" + tid.getId() + "> waiting for XL");                
            } else {
                System.out.println("TID <" + tid.getId() + "> waiting for SL");
            }
        }
    }

    public void print_accessors() {
        for (TransactionId tid : this.accessors) {
            System.out.println("TID <" + tid.getId() + ">");                
        }
    }

}