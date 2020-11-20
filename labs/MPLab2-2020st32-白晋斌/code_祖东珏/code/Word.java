package BigDataLab2;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class Word implements WritableComparable<Word> {
    private String word;
    private float average;

    public Word() {
        super();
    }

    public Word(String word, float average) {
        super();
        this.word = word;
        this.average = average;
    }

    public String getKey() {
        return word;
    }

    public Float getAverage() {
        return average;
    }

    public int compareTo(Word o) {
        if(this.word.equals(o.word))
            return 0;
        else if(this.average < o.average)
            return -1;
        else
            return 1;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(average);
        dataOutput.writeUTF(word);
    }

    public void readFields(DataInput dataInput) throws IOException {
        average = dataInput.readFloat();
        word = dataInput.readUTF();
    }
}
