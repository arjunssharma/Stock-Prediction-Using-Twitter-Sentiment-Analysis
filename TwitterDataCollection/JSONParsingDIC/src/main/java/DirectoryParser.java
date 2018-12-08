import com.google.gson.Gson;

import java.io.*;
import java.net.URI;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class DirectoryParser {

    public static final String HDFS_ROOT_URL="hdfs://localhost:9000";
    private static Configuration conf = new Configuration();


    public static void main(String arg[])throws IOException {
        Gson gson = new Gson();
//        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
//        String in = br.readLine();
//        TweetHolder tweets = new TweetHolder();
//        tweets = gson.fromJson(in, TweetHolder.class);
//        System.out.println(tweets.text);


//        String uri = HDFS_ROOT_URL+"/user/tweets";
//        FileSystem fs = FileSystem.get(URI.create(uri), conf);
//
//        File curDir = new File(".");
//        File[] filesList = curDir.listFiles();
//        for(File f : filesList){
//            if(f.isFile() && f.getName().startsWith("FlumeData")){
//                System.out.println(f.getName());
//                BufferedReader br = new BufferedReader(new FileReader(f));
//                int count = 0;
//                String in = br.readLine();
//                while(in != null) {
//                    try {
//                        TweetHolder tweets;
//                        tweets = gson.fromJson(in, TweetHolder.class);
//                        System.out.println(tweets.text);
//                    } catch (Exception ex) {
//                        count--;
//                    }
//                    count++;
//                    in = br.readLine();
//                }
//                System.out.println(count);
//
//            }
//        }

        int count = 1;

        while (true) {

            try {
                FileSystem fs = FileSystem.get(new Configuration());
                FileStatus[] status = fs.listStatus(new Path(arg[0] + "/user/tweets"));  // you need to pass in your hdfs path

                for (int i = 0; i < status.length; i++) {
                    try {
                        if(status[i].getPath().getName().contains("tmp")) {
                            continue;
                        }
                        System.out.println(status[i].getPath().getName());
                        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));


                        if (status[i].getPath().getName().startsWith("FlumeData")) {
                            String in = br.readLine();
                            while (in != null) {
                                try {
                                    TweetHolder tweets;
                                    tweets = gson.fromJson(in, TweetHolder.class);

                                    Configuration conf = new Configuration();
                                    conf.set("fs.default.name", arg[0] + "/user/tweets");

                                    FileSystem inFS = FileSystem.get(conf);
                                    FSDataOutputStream stream = inFS.create(new Path(arg[0] + "/input/" + System.currentTimeMillis()));
                                    stream.write(tweets.text.getBytes());
                                    System.out.println(tweets.text);
                                    stream.flush();
                                    stream.sync();
                                    stream.close();

                                } catch (Exception ex) {
                                }
                                count++;
                                in = br.readLine();
                                count = count % 50;
                                if (count == 0) {
                                    Thread.sleep(1000 * 30);
                                }
                            }
                            System.out.println(count);
                            try {
                                fs.delete(status[i].getPath(), true);
                            } catch (Exception exp) {
//                                exp.printStackTrace();
                                System.out.println("Data not read from  " + status[i].getPath());
                            }
                        }
                    } catch (Exception ex) {
//                        ex.printStackTrace();
                        System.out.println("Temp file from Flume");
                    }
                }
            } catch (Exception e) {
//                e.printStackTrace();
                System.out.println("No Data in this iteration.");
            }

        }
    }

}
