package study;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;


public class Test {
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}

	public static void main(String[] args) throws MalformedURLException, IOException {
		// TODO Auto-generated method stub
		InputStream in = null;
		try {
			in = new URL("hdfs://localhost/test1/kms-site.xml").openStream();
			IOUtils.copyBytes(in, System.out, 4096, false);
		} finally {
			if(in != null) {
				IOUtils.closeStream(in);
			}
		}
	}

}
