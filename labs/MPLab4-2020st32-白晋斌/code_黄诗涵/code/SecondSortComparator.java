import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondSortComparator extends WritableComparator{
    public SecondSortComparator() {
        super(PairBean.class,true);
    }
    public int compare(WritableComparable a, WritableComparable b)
    {
        return a.compareTo(b);
    }
}
