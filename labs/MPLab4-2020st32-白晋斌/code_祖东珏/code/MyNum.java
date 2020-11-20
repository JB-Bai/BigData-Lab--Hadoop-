package BigDataLab4;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyNum implements WritableComparable<MyNum> {
    private Integer first;
    private Integer second;

    MyNum() {
        super();
        first = second = 0;
    }

    MyNum(int key, int value) {
        super();
        first = key;
        second = value;
    }

    public Integer getFirst() {
        return this.first;
    }

    public Integer getSecond() {
        return this.second;
    }

    @Override
    public int compareTo(MyNum target) {
        if(this.first.equals(target.first))
            return -this.second.compareTo(target.second);
        else
            return this.first.compareTo(target.first);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.first);
        dataOutput.writeInt(this.second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first = dataInput.readInt();
        this.second = dataInput.readInt();
    }
}
