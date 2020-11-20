package BigDataLab3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

enum RecordType {
    Unknown, Product, Order
}

public class Record implements WritableComparable<Record> {
    public Long pid;
    public String pname;
    public Float price;
    public Long oid;
    public String odata;
    public Long oamount;
    RecordType type;

    public Record() {
        super();
        pid = 0L;
        price = 0F;
        oid = 0L;
        oamount = 0L;
        pname = " ";
        odata = " ";
        type = RecordType.Unknown;
    }

    public int compareTo(Record o) {
        int idcompare = this.pid.compareTo(o.pid);
        if(idcompare != 0)
            return idcompare;
        else {
            if(this.type == RecordType.Product && o.type == RecordType.Product)
                return this.pname.compareTo(o.pname);
            else if(this.type == RecordType.Order && o.type == RecordType.Order)
                return this.oid.compareTo(o.oid);
            else if(this.type == RecordType.Product && o.type == RecordType.Order)
                return 1;
            else
                return -1;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(pid);
        dataOutput.writeUTF(pname);
        dataOutput.writeFloat(price);
        dataOutput.writeLong(oid);
        dataOutput.writeUTF(odata);
        dataOutput.writeLong(oamount);
        dataOutput.writeUTF(type.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pid = dataInput.readLong();
        pname = dataInput.readUTF();
        price = dataInput.readFloat();
        oid = dataInput.readLong();
        odata = dataInput.readUTF();
        oamount = dataInput.readLong();
        type = RecordType.valueOf(dataInput.readUTF());
    }

    @Override
    public String toString() {
        return oid + " " + odata + " " + pid + " " + pname + " " + price + " " + oamount;
    }
}
