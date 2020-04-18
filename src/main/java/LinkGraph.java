
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;


public class LinkGraph extends Configured implements Tool {

    public static Path urlspath = new Path("./all/urls.txt");
    public static Map<String, String> urls = new HashMap<>();
    public static Map<String, String> inv_urls = new HashMap<>();
    public static int n_links = 564549;

    public static class LinkMapper extends Mapper<LongWritable, Text, IntWritable, Text> {


        public void setup(Mapper.Context context) throws IOException
        {
            n_links = 0;
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                FileStatus[] statuses = fs.globStatus(urlspath);
                for (FileStatus status : statuses)
                {
                    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status.getPath())) );
                    String line;
                    line=br.readLine();
                    while (line != null){
                        String[] args = line.split("\t");
                        String id = args[0];
                        String url = args[1];
                        urls.put(url, id);
                        inv_urls.put(id, url);
                        n_links += 1;
                        line=br.readLine();
                    }
                }
                //System.out.println(n_links);
            }
            catch (Exception ignored){}
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            int id = Integer.parseInt(line[0]);
            String compressed = line[1];
            LinkedList<String> links = extractLinks(compressed);
            StringBuilder sb = new StringBuilder();
            sb.append("L");
            for(String link : links){
                sb.append(link);
                sb.append("\t");
            }
            Text val = new Text(sb.toString());
            context.write(new IntWritable(id), val);
        }
    }

    public static LinkedList<String> extractLinks(String compressed) throws UnsupportedEncodingException, MalformedURLException{
        byte[] decoded = Base64.getDecoder().decode(compressed);
        Inflater inflater = new Inflater();
        inflater.setInput(decoded);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte outBuffer[] = new byte[1024];
        while (!inflater.finished()) {
            int bytesInflated;
            try {
                bytesInflated = inflater.inflate(outBuffer);
            } catch (DataFormatException e) {
                throw new RuntimeException(e.getMessage());
            }

            baos.write(outBuffer, 0, bytesInflated);
        }
        String page = baos.toString("UTF-8");

        Pattern link = Pattern.compile("<a[^>]+href=[\"']?([^\"'\\s>]+)[\"']?[^>]*>", Pattern.CASE_INSENSITIVE|Pattern.DOTALL);
        Matcher pageMatcher = link.matcher(page);
        LinkedList<String> links = new LinkedList<>();
        String found;
        HashSet<String> ls = new HashSet<>();
        while (pageMatcher.find()) {
            found = pageMatcher.group(1);
            if(found.equals("/")){
                continue;
            }
            if (found.charAt(0) == '/'){
                found = "http://lenta.ru" + found;
            }
            String host;
            try {
                URI uri = new URI(found);
                String domain = uri.getHost();
                host =  domain.startsWith("www.") ? domain.substring(4) : domain;
                if (host.equals("lenta.ru") && !ls.contains(found)) {
                    links.add(found);
                    ls.add(found);
                }
            } catch (URISyntaxException | NullPointerException ignored) {
            }

        }
        return links;
    }

    public static class UrlsMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] line = value.toString().split("\t");
            int id = Integer.parseInt(line[0]);
            StringBuilder sb = new StringBuilder();
            sb.append("U");
            sb.append(line[1]);
            context.write(new IntWritable(id), new Text(sb.toString()));
        }
    }

    public static class LinkReducer extends Reducer<IntWritable, Text, Text, Text>{
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String res = "";
            String val = "";
            StringBuilder lids = new StringBuilder();
            for(Text value : values){
                val = value.toString();
                char type = val.charAt(0);
                if (type == 'L'){
                    val = val.substring(1);
                    String[] ls = val.split("\t");
                    for (String l: ls){
                        String got = urls.get(l);
                        if (got == null){
                            continue;
                        }

                        lids.append(urls.get(l));
                        lids.append("\t");
                    }
                } else if(type == 'U'){
                    res = value.toString().substring(1);
                }
            }
            context.write(new Text("I"+ key.toString()), new Text(lids.toString()));
        }
    }
    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(LinkGraph.class);
        job.setJobName(LinkGraph.class.getCanonicalName());
        job.setInputFormatClass(TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(input), TextInputFormat.class, LinkMapper.class);
        //MultipleInputs.addInputPath(job, new Path(urls), TextInputFormat.class, UrlsMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        //job.setMapperClass(LinkMapper.class);
        job.setReducerClass(LinkReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], "./res/");
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        LinkGraph.urlspath = new Path(args[1]);
        int ret = ToolRunner.run(new LinkGraph(), args);
        System.exit(ret);
    }
}