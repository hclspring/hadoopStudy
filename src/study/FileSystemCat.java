package study;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.IOUtils;


public class FileSystemCat {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String uriString = "hdfs://localhost/test1/kms-site.xml";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uriString), conf);
		FSDataInputStream in = null;
		try {
			byte[] buffer = new byte[1000];
			in = fs.open(new Path(uriString));
//			IOUtils.copyBytes(in, System.out, 10, false);
			in.read(0, buffer, 0, 10);
			System.out.println(buffer.length);
			String str = new String(buffer);
			System.out.println(str);
//			in.seek(0); // go back to the start of the file
//			IOUtils.copyBytes(in, System.out, 40, false);
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			IOUtils.closeStream(in);
		}
	}

}
