import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairBean implements WritableComparable<PairBean> {
    private Integer first=0;
    private Integer second=0;
    public PairBean(){}
    public PairBean(Integer f,Integer s)
    {
        first=f;
        second=s;
    }

    public void write(DataOutput out) throws IOException{
        out.writeInt(first);
        out.writeInt(second);
    }
    public void readFields(DataInput in) throws IOException{
        this.first = in.readInt();
        this.second = in.readInt();
    }
    public int compareTo(PairBean other) {
        if(first==other.first)
            return second > other.second? -1: 1 ;
        else
            return first < other.first? -1: 1 ;
    }
    @Override
    public String toString()
    {
        return first.toString()+'\t'+second.toString();
    }
}
