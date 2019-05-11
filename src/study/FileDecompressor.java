package study;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import io.netty.util.internal.logging.Log4J2LoggerFactory;

public class FileDecompressor {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		
		Path inputPath = new Path(uri);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodec(inputPath);
		if (codec == null) {
			System.err.println("No codec found for " + uri);
			System.exit(1);
		}
		
		String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
		System.out.println(codec.getDefaultExtension());
		System.out.println(codec.toString());
		System.out.println("inputPath = " + inputPath.toString());
		System.out.println("outputPath = " + new Path(outputUri).toString());
		
		InputStream in = null;
		OutputStream out = null;
		try {
			in = codec.createInputStream(fs.open(inputPath));
			out = fs.create(new Path(outputUri));
			IOUtils.copyBytes(in, out, conf);
			System.out.println("Copy OK.");
		} finally {
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
			System.out.println("Close streams.");
		}
	}

}
