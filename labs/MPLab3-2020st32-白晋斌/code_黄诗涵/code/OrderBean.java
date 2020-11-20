import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private String oid="";
    private String odate="";
    private String pid="";
    private String pname="";
    private String price="";
    private String oamount="";
    public OrderBean()
    {

    }
    public String type()
    {
        if(oid.compareTo("")==0)
            return "product";
        else
            return "order";
    }
    public void set(String oid, String odate,String pid,String pname,String price,String oamount) {
        this.oid = oid;
        this.odate = odate;
        this.pid = pid;
        this.pname = pname;
        this.price = price;
        this.oamount = oamount;
    }
    public int compareOid(OrderBean other)
    {
        return oid.compareTo(other.oid);
    }


    public int compareTo(OrderBean other)
    {
        if(pid.compareTo(other.pid)!=0)
            return pid.compareTo(other.pid);
        else
        {
            if(pname.compareTo("")==0)//pname空
            {
                if(other.pname.compareTo("")==0)//都空
                    return compareOid(other);
                else return -pname.compareTo(other.pname);//不空的在前面
            }
            else//pname不空
            {
                if(other.pname.compareTo("")==0)
                    return -pname.compareTo(other.pname);
                else return price.compareTo(other.price);
            }
        }
    }
    public void write(DataOutput out) throws IOException
    {
        out.writeUTF(oid);
        out.writeUTF(odate);
        out.writeUTF(pid);
        out.writeUTF(pname);
        out.writeUTF(price);
        out.writeUTF(oamount);
    }
    public void readFields(DataInput in) throws IOException{
        this.oid = in.readUTF();
        this.odate = in.readUTF();
        this.pid = in.readUTF();
        this.pname = in.readUTF();
        this.price = in.readUTF();
        this.oamount=in.readUTF();
    }
    @Override
    public String toString() {
        String str="";
        str=str+((oid==null)?"":oid)+" ";
        str=str+((odate==null)?"":odate)+" ";
        str=str+((pid==null)?"":pid)+" ";
        str=str+((pname==null)?"":pname)+" ";
        str=str+((price==null)?"":price)+" ";
        str=str+((oamount==null)?"":oamount);
        return str;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public void setPrice(String price) {
        this.price = price;
    }
    public String getPname()
    {
        return pname;
    }

    public String getPrice() {
        return price;
    }
}
