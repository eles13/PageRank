import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;


public class PageRank extends Configured implements Tool {

    public static double p = 0.85;
    public static int iters = 0;
    public static HashSet<Integer> hanging = new HashSet<>();
    public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            StringBuilder sb = new StringBuilder();
            String[] line  = value.toString().split("\t");
            String k = line[0];
            if (k.charAt(0) == 'I'){
                double initr = 1.0/LinkGraph.n_links;
                sb.append(initr);
                sb.append("\t");
                for(String id:line){
                    if (id.equals(line[0])){
                        continue;
                    }
                    sb.append(id);
                    sb.append("\t");
                }
                context.write(new Text(k), new Text(sb.toString()));
            }else{
                double pr = Double.parseDouble(line[1]);
                int counturls = line.length - 2;

                if(counturls == 0){
                    hanging.add(Integer.parseInt(k));
                    context.write(new Text(k), new Text("R" + pr + "\t" + k));
                    return;
                }
                pr =  pr / counturls;
                sb.append("L");
                for (int i = 2; i < line.length; i++) {
                    sb.append(line[i]);
                    sb.append("\t");
                    context.write(new Text(line[i]), new Text("R" + pr + "\t" + k));
                }
                context.write(new Text(k), new Text(sb.toString()));
            }
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            if (key.toString().charAt(0) == 'I'){
                StringBuilder sb = new StringBuilder();
                for (Text value: values){
                    sb.append(value.toString());
                    sb.append("\t");
                }
                String k = key.toString().substring(1);
                context.write(new Text(k), new Text(sb.toString()));
            }else{
                StringBuilder sb = new StringBuilder();
                double prs = 0.0;
                String links = "";
                HashSet<String> from = new HashSet<>();
                for (Text value: values){
                    String rec = value.toString();
                    char type = rec.charAt(0);
                    if(type == 'L'){
                        links = rec.substring(1);
                    }else if(type == 'R'){
                        prs += Double.parseDouble(rec.substring(1).split("\t")[0]);
                        from.add(rec.substring(1).split("\t")[1]);
                    }
                }
                prs = p*prs + (1-p)/LinkGraph.n_links;
                if (links.length() == 0){
                    context.write(key, new Text(Double.toString(1.0/LinkGraph.n_links)));
                    return;
                }
                StringBuilder lksbldr = new StringBuilder();
                String[] lks = links.split("\t");
                for(String link : lks){
                    if (!from.contains(link) && !hanging.contains(Integer.parseInt(link))){
                        lksbldr.append(link);
                        lksbldr.append("\t");
                    }
                }
                sb.append(prs);
                sb.append("\t");
                sb.append(lksbldr);
                context.write(key, new Text(sb.toString()));
            }
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(PageRank.class);
        job.setJobName(PageRank.class.getCanonicalName());
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(PageRankMapper.class);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setReducerClass(PageRankReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration config = getConf();
        String inputStep, outputStep;
        String inputFormat  = "%s/it%02d/part-r-*";
        String outputFormat = "%s/it%02d/";
        String output = "./res/prsteps";
        ControlledJob[] steps = new ControlledJob[iters];
        steps[0] = new ControlledJob(config);
        outputStep = String.format(outputFormat, output, 1);
        steps[0].setJob(getJobConf("./res/part-r-00000", outputStep));
        for (int i = 1; i < iters; i++) {
            inputStep  = String.format(inputFormat,  output, i);
            outputStep = String.format(outputFormat, output, i + 1);
            steps[i] = new ControlledJob(config);
            steps[i].setJob(getJobConf(inputStep, outputStep));
        }

        JobControl control = new JobControl(PageRank.class.getCanonicalName());

        for (ControlledJob step: steps) {
            control.addJob(step);
        }
        for (int i = 1; i < iters; i++) {
            steps[i].addDependingJob(steps[i - 1]);
        }

        new Thread(control).start();
        while (!control.allFinished()) {}

        return control.getFailedJobList().isEmpty() ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        iters = Integer.parseInt(args[1]);
        int ret = ToolRunner.run(new PageRank(), args);
        System.exit(ret);
    }
}