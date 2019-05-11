package study;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class ListStatus {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
//		String uri = "hdfs://localhost/user/spring/test1";
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path[] paths = new Path[args.length];
		for (int i = 0; i < paths.length; ++i) {
			paths[i] = new Path(args[i]);
		}
//		FileStatus[] status = fs.listStatus(paths);
		Path pathPattern = new Path("/t*");
		FileStatus[] status = fs.globStatus(pathPattern);
		Path[] listedPaths = FileUtil.stat2Paths(status);
		for (Path p: listedPaths) {
			System.out.println(p);
		}
	}

}
