package simpledb;

import java.util.*;

public class TransactionManager {
    
    private Map<TransactionId, Set<PageId>> transPages; //  quick access to a tid and the pages it has locks on
    private Map<PageId, Lock> pageLocks; // quick access to a page and its associated lock (each page can at most have one lock)
    private Map<TransactionId, Set<TransactionId>> graph; // graph used to detect dependency cycles to detect deadlocks

    public TransactionManager() {
        this.transPages = new HashMap<TransactionId, Set<PageId>>();
        this.pageLocks = new HashMap<PageId, Lock>();
        this.graph = new HashMap<TransactionId, Set<TransactionId>>();
    }

    /**
     * Outward facing function called in BufferPool, a transaction uses this to request a lock with its given permissions
     */
    public void grantLock(TransactionId tid, PageId pid, Permissions perm) 
        throws TransactionAbortedException 
    {
        if (perm == Permissions.READ_ONLY) {
            grantReadLock(tid, pid);
        }
        else grantWriteLock(tid, pid);
    }

    /**
     * Helper function that returns true if there is a cycle in the dependency graph
     *
     * Start with waiting_tid node, iterate over neighbors from set<tids> graph and
     * add node to visited_nodes. if any of neighbors are already visited, then there is a cycle
     *
     * Credit: https://www.geeksforgeeks.org/detect-cycle-in-a-graph/
     */
    private boolean isCyclic(TransactionId waiting_tid, Lock lock) {
        HashSet<TransactionId> visited_nodes = new HashSet<TransactionId>();
        Queue<TransactionId> queue = new LinkedList<TransactionId>();

        /* add tid to graph to see if it would be cyclic */
        this.graph.put(waiting_tid, lock.getAccessors());

        /* starting point */
        visited_nodes.add(waiting_tid);
        queue.add(waiting_tid);

        /* Take the head of queue, if neighbors are already visited, there is a cycle */
        do {
            TransactionId current_node = queue.remove();

            /* no deadlock if node_visited is not in graph */
            if (this.graph.containsKey(current_node) == false) continue;

            for (TransactionId neighbor : this.graph.get(current_node)) {
                /* No deadlock with itself */
                if (neighbor.equals(current_node)) continue;

                /* If not visited before, add */
                else if (visited_nodes.contains(neighbor) == false) {
                    visited_nodes.add(neighbor);
                    queue.add(neighbor);
                } 

                /* visited before, so cycle. */
                else {
                    this.graph.remove(waiting_tid);
                    return true;
                } 
            }
            
        } while (queue.isEmpty() == false);

        return false;
    }

    /**
     * Helper function to obtain/wait for a read lock
     */
    public void grantReadLock(TransactionId tid, PageId pid) throws TransactionAbortedException
    {
        Lock lock;
        Set<TransactionId> accessors;
        
        synchronized(this) {
            lock = pageLocks.containsKey(pid) ? pageLocks.get(pid) : new Lock();

            /* Insert <page, lock> if page <page_id> does not have a lock yet */
            if (!pageLocks.containsKey(pid)) {
                pageLocks.put(pid, lock);
            }

            /* Get transactions using the lock */
            accessors = lock.getAccessors();
            
            /* if transaction already uses lock, return */
            if (accessors.contains(tid)) return;

            /* if tid using the XL lock && there are accessors, check deadlock */
            if (lock.lt == Lock.LockType.WRITE && 
                !lock.getAccessors().isEmpty() && isCyclic(tid, lock)) {
                throw new TransactionAbortedException();
            }

        }

        /* Put transaction in waiting list for shared lock */
        Map<TransactionId, Lock.LockType> waitingList = lock.getWaitingList();
        waitingList.put(tid, Lock.LockType.READ);

        /* Wait until no XL locks */
        synchronized(this) {
            try {
                while (lock.numWriters > 0) this.wait();

                /* Create SL for tid */
                lock.lt = Lock.LockType.READ;
                lock.numReaders++;
                accessors.add(tid);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        /* <tid> is not waiting for lock anymore */
        waitingList.remove(tid);

        /* add <pid> to the affected pages of <tid> */
        synchronized(this) {
            if (transPages.containsKey(tid)) {
                transPages.get(tid).add(pid);
            }
            else {
                HashSet<PageId> pset = new HashSet<PageId>();
                pset.add(pid);
                transPages.put(tid, pset);
            }
        }
    }

    // /**
    //  * Helper function to add <page,lock> if <pid> does not have a lock associated with it yet
    //  * 
    //  * @param xl_flag - for SL vs XL policy
    //  *
    //  */
    // private Lock add_page_lock(TransactionId tid, PageId pid, boolean xl_flag) throws TransactionAbortedException {
    //     if (xl_flag) {
    //         synchronized(this) {
    //             /* Get or create lock */
    //             Lock lock = this.pageLocks.containsKey(pid) ? pageLocks.get(pid) : new Lock();
                
    //             /* Get transactions using the lock */
    //             Set<TransactionId> accessors = lock.getAccessors();

                
    //             if (accessors.contains(tid) && (lock.lt == Lock.LockType.WRITE)) {
    //                 return lock;
    //             }

    //             /* create lock on page if non-existent before */
    //             if (!this.pageLocks.containsKey(pid)) this.pageLocks.put(pid, lock);

    //             /* if there are holders, add to dependency graph */
    //             if (lock.getAccessors().isEmpty() == false) {
    //                 graph.put(tid, lock.getAccessors());

    //                 /* check for deadlock */
    //                 if (check_deadlock(tid)) {
    //                     this.graph.remove(tid);
    //                     throw new TransactionAbortedException();
    //                 }
    //             }

    //             return lock;
    //         }
    //     }
        
    //     else {
    //         synchronized(this) {
    //             /* Get or create lock */
    //             Lock lock = this.pageLocks.containsKey(pid) ? pageLocks.get(pid) : new Lock();
                
    //             /* Get transactions using the lock */
    //             Set<TransactionId> accessors = lock.getAccessors();

    //             /* Insert <page, lock> if page <page_id> does not have a lock yet */
    //             if (!pageLocks.containsKey(pid)) {
    //                 pageLocks.put(pid, lock);
    //             }
                
    //             /* if transaction already uses lock, return */
    //             if (accessors.contains(tid)) return lock;

    //             /* if tid using the XL lock, add to dependency graph */
    //             if (lock.lt == Lock.LockType.WRITE) {
    //                 if (lock.getAccessors().isEmpty() == false) {
    //                     this.graph.put(tid, lock.getAccessors());

    //                     /* if deadlock, throw TransactionAbortedException and remove from graph */
    //                     if (check_deadlock(tid)) {
    //                         this.graph.remove(tid);
    //                         throw new TransactionAbortedException();
    //                     }
    //                 }
    //             }

    //             return lock;
    //         }    
    //     }    
    // }

    /**
     * Helper function to obtain/wait for a write lock. Handles upgrading
     */
    public void grantWriteLock(TransactionId tid, PageId pid) throws TransactionAbortedException 
    {
        Lock lock;
        Set<TransactionId> accessors;
        
        /* Insert <page, lock> if page <page_id> does not have a lock yet */
        synchronized(this) {
            lock = pageLocks.containsKey(pid) ? pageLocks.get(pid) : new Lock();
            
            accessors = lock.getAccessors();
            if (accessors.contains(tid) && (lock.lt == Lock.LockType.WRITE)) {
                return;
            }

            /* create lock on page if non-existent before */
            if (!pageLocks.containsKey(pid)) pageLocks.put(pid, lock);

            /* if there are holders, check for deadlock */
            if (!lock.getAccessors().isEmpty() && isCyclic(tid, lock)) {
                throw new TransactionAbortedException();
            }
        }
        

        Map<TransactionId, Lock.LockType> waitingList = lock.getWaitingList();
        
        /* if <tid> not waiting on XL already, insert waiting for XL lock */
        if (waitingList.get(tid) != Lock.LockType.WRITE) {
            waitingList.put(tid, Lock.LockType.WRITE);
            
            /* wait until there are no XL nor SLs */
            synchronized(this) {
                try {

                    /* if <tid> already has SL remove SL lock when we are the last SL */
                    if (accessors.contains(tid)) {
                        
                        while (lock.numReaders > 1) this.wait();

                        /* remove SL lock */
                        if (accessors.contains(tid)) {
                            lock.numReaders -= 1;
                            accessors.remove(tid);
                        }
                    }

                    /* there are still XL / SL holders */
                    while (lock.numWriters > 0 || lock.numReaders > 0) this.wait();
            
                    /* finally able to acquire XL lock */
                    lock.lt = Lock.LockType.WRITE;
                    lock.numWriters++;
                    accessors.add(tid);
                    
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            waitingList.remove(tid);
        }
        
        /* add <pid> to the affected pages of <tid> */
        synchronized(this) {
            if (transPages.containsKey(tid)) {
                transPages.get(tid).add(pid);
            }
            else {
                HashSet<PageId> pset = new HashSet<PageId>();
                pset.add(pid);
                transPages.put(tid, pset);
            }
        }
    }
    /**
     * Unlock all locks associated with some transaction, given its tid.
     * After all locks have been released, remove the association between
     * the tid and all the pids in transPages of this TransactionManager
     */
    public void unlockAll(TransactionId tid) {
        synchronized(this) {
            /* if tid has any pids associated with it, then iterate through and unlock*/
            if (transPages.containsKey(tid)) {
                Set<PageId> pids = transPages.get(tid);
                for (PageId pid : pids) {
                    unlock(tid, pid, false);
                }
                /* remove all pids associated with tid */
                transPages.remove(tid);
            }
        }
    }

     /**
     * Given a tid, pid, release the lock tid has on pid. The individual parameter is 
     * a boolean used to determine the use case of this function
     * This is either used in unlockAll, in which case the associated page cannot be
     * removed from transPages until all pids have been iterated over (otherwise a
     * concurrency error is thrown from modifying a set that is being iterated over)
     * in which case individual = false, otherwise this is used in BufferPool's releasePage()
     * function to manually release a single page from a transaction so that page
     * can immediately be removed from transPages in which case individual = true.
     */
    public void unlock(TransactionId tid, PageId pid, Boolean individual) {
        synchronized(this) {
            if (pageLocks.containsKey(pid)) {
                Lock lock = pageLocks.get(pid);
                Set<TransactionId> accessors = lock.getAccessors();
                /* ensure tid actually owns the lock to the given pid */
                if (accessors.contains(tid)) {
                    /* update the counts */
                    if (lock.lt == Lock.LockType.READ) {
                        lock.numReaders--;
                    } 
                    else {
                        lock.numWriters--;
                    }
                    accessors.remove(tid);
                    /* if we're doing a single instance of an unlocking, then it's fine to remove
                    pid from the tid bucket in transPages. If we're doing all the locks of tid,
                    then this can't be touched until everything's been iterated over */
                    if (individual) {
                        transPages.get(tid).remove(pid);
                    }
                    notifyAll();
                }
            }
        }
    }

    /**
     * Accessing function to return the set of all pids associated with a given tid
     * Namely, return all pages that a given transaction holds the locks for
     */
    public Set<PageId> getTransactionPages(TransactionId tid) {
        return transPages.get(tid);
    }

    /**
     * Accessing function that checks if a given transaction owns a lock for a page
     * Used in bufferPool directly in the holdsLock() function
     */
    public Boolean holdsLock(TransactionId tid, PageId pid) {
        return transPages.containsKey(tid) ? transPages.get(tid).contains(pid) : false;
    }
}