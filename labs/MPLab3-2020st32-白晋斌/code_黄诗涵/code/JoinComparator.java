import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class JoinComparator extends WritableComparator {
    public JoinComparator() {
        super(OrderBean.class,true);
    }

    public int compare(WritableComparable a,WritableComparable b)
    {
        return a.compareTo(b);
    }
}
