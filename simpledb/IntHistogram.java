package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] buckets;
    private int num_buckets;
    private int width;            // width of buckets 
    private int total_values;
    private int distinct_values;
    private int min_value;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into n buckets, where n is @param buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = new int[buckets];
        this.num_buckets = buckets;
        this.total_values = 0;
        this.distinct_values = 0;
        this.min_value = min;

        /* width is used to map a value to bucket index */
        this.width = (int) Math.ceil( (double) (max - (min - 1)) / buckets);
    }

    /**
     * Find the bucket index of value v
     * 
     * @param v The integer value we are looking for in buckets[].
     *
     * @return bucket index of value v, -1 upon failure
     */
    private int findBucket(int v) {
        int bucket_index = (v - this.min_value) / this.width;
        if (bucket_index < 0) return -1;
        if (bucket_index > this.num_buckets) return num_buckets;
        return bucket_index;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int bucket_index = (v - this.min_value) / this.width;
        this.total_values++;
        if (this.buckets[bucket_index] == 0) this.distinct_values++;
        this.buckets[bucket_index]++;   // incr number of values in bucket[index]
    }


    /**
     * Helper function to determine EQUALITY/LIKE selectivity
     * @param v Value to find EQUALITY/LIKE for in buckets
     *
     * @return (double) selectivity
     */
    private double equals_selectivity(int v) {
        int index = findBucket(v);
        
        /* >= num_buckets because otherwise results in OutOfBounds error */
        if (index < 0 || index >= this.num_buckets) return 0.0;
        
        int height = this.buckets[index];  // number of values in bucket[index]

        /* Equality selectivity = (height / width) / ntups */
        return (double) ((double) height / this.width) / this.total_values;
    }

    /**
     * Helper function to determine fraction selectivity of bucket
     *
     * @param v Value to find
     * @param b_right Bucket to the right of bucket containing v
     *
     * @return bucket_part (to the right) [--- v {--b_part--}] [b-right]
     */
    private int calculate_bucket_part(int b_right, int v) {
        /* See above note on calculation */
        int value_b_right = b_right * this.width;
        int delta = v - this.min_value;

        return (value_b_right - delta) / this.width;
    }
    

    /**
     * Helper function to determine INEQUALITY (> or <) selectivity
     * 
     * @param v Value to find GREATER/LESS THAN for in buckets
     * @param op Operator (GREATER_THAN or LESS_THAN)
     * 
     * @return (double) selectivity
     */
    private double inequality_selectivity(Predicate.Op op, int v) {
        /**
         * Inequality selectivity:
         * - find b_index (using height, width)
         * - bucket[b_index] contains a fraction b_f (= height / ntups) 
             of all tuples
         * - assuming uniformity, b_part = (b_right - const) / width, where 
             b_part is the % of bucket b that is greater than v.

         * - hence b_f * b_part is the selecitity of bucket b
         * - then you have all buckets greater than b with all their selectivity
         */

        int index = findBucket(v);
        int n = this.num_buckets;
        double selectivity = 0.0;
        
        int b_right;      // bucket to the right
        int b_left;       // bucket to the left
        int b_part;       // flag to calculate b_part
        int height;

        /* Value is below min_value in buckets[],
         * so all bucket selectivity should be returned */
        if (index < 0) {
            b_right = 0;
            b_left = -1;
            b_part = 0;             // turn off flag
            height = 0;    
        } 
        /* Value is greater than max_value in buckets[],
         * so no bucket selectivity should be returned */
        else if (index >= n) {
            b_right = n;
            b_left = n - 1;
            b_part = 0;             // turn off flag
            height = 0;
        } 
        /* Value falls within buckets[] range,
         * so set flag to calculate the selectivity of buckets[index] */
        else {
            b_right = index + 1;    // bucket to the right
            b_left = index - 1;     // bucket to the left
            b_part = 1;             // flag to calculate b_part
            height = this.buckets[index];
        }

        /* Greater vs less than calculation */
        switch(op) {
            case GREATER_THAN:
            case GREATER_THAN_OR_EQ:
                /* Flag is set to calculate b_part */
                if (b_part == 1) {
                    b_part = calculate_bucket_part(b_right, v);

                    /* Turn b_part into selectivity of bucket[index] */
                    selectivity += (height * b_part) / this.total_values;
                }

                /* Most right bucket contains v, so b_part is total selectivity  */
                if (b_right >= n) return selectivity / this.total_values;

                /* Add selectivity from all the other buckets */
                for (int i = b_right; i < n; i++) {
                    selectivity += this.buckets[i];
                }

                /* Return selectivity */
                return selectivity / this.total_values;
            
            case LESS_THAN:
            case LESS_THAN_OR_EQ:
                /* Flag is set to calculate b_part */
                if (b_part == 1) {
                    b_part = 1 - calculate_bucket_part(b_right, v);

                    /* Turn b_part into selectivity of bucket[index] */
                    selectivity += (height * b_part) / this.total_values;   
                }

                /* Most left bucket contains v, so b_part is total selectivity  */
                if (b_right < 0 ) return selectivity / this.total_values;

                /* Add selectivity from all the other buckets */
                for (int i = 0; i <= b_left; i++) {
                    selectivity += this.buckets[i];
                }

                /* Return selectivity */
                return selectivity / this.total_values;
                
            default:
                return -1.0;
        }
        
    }
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        switch(op) {
            case EQUALS:
            case LIKE:
                return equals_selectivity(v);
            case LESS_THAN:
            case GREATER_THAN:
                return inequality_selectivity(op, v);
            case LESS_THAN_OR_EQ:
                return inequality_selectivity(op, v) + equals_selectivity(v);
            case GREATER_THAN_OR_EQ:
                return inequality_selectivity(op, v) + equals_selectivity(v);
            case NOT_EQUALS:
                return 1.0 - equals_selectivity(v);
            default:
                return -1.0;
        }

    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        return 1.0;
    }
    

    public int getDistinctTuples() {
        return this.distinct_values;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
