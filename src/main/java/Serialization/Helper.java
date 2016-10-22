package Serialization;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.Arrays;

/**
 * Created by me on 10/16/16.
 */
public class Helper {

    private static byte[] serialize(Writable writable) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        writable.write(dataOut);
        dataOut.close();
        return out.toByteArray();
    }

    private static String deserialize(Writable writable, byte[] bytes)
            throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream dataIn = new DataInputStream(in);
        writable.readFields(dataIn);
        dataIn.close();
        return "";
//         bytes;
    }

    public static void main(String[] args) throws IOException {
        IntWritable intWritable = new IntWritable(54);
        byte[] bytes = serialize(intWritable);
        System.out.println(Arrays.toString(bytes));

        IntWritable intWritable1 = new IntWritable();
        deserialize(intWritable1, bytes);
        System.out.println(intWritable1.get());
    }
}
