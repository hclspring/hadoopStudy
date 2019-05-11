package study;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FileCopyWithProgress {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String localSrc = "/home/spring/.ssh/id_rsa.pub";
		String dst = "hdfs://localhost/user/spring/id_rsa.pub.copy";
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		OutputStream out = fs.create(new Path(dst), new Progressable() {
			
			@Override
			public void progress() {
				// TODO Auto-generated method stub
				System.out.println(".");
			}
		});
		IOUtils.copyBytes(in, out, 4096, true);
	}

}
