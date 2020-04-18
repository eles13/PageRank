import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;


public class Hits extends Configured implements Tool {

    public static int iters = 0;
    static LinkedList<String> allurls = new LinkedList<>();

    public void geturls() throws IOException
    {
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] statuses = fs.globStatus(LinkGraph.urlspath);
            for (FileStatus status : statuses)
            {
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status.getPath())) );
                String line;
                line=br.readLine();
                while (line != null){
                    String[] args = line.split("\t");
                    String id = args[0];
                    allurls.add(id);
                    line=br.readLine();
                }
            }
        }
        catch (Exception ignored){}
    }

    public static class HitsAutMapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            StringBuilder sb = new StringBuilder();
            String[] line = value.toString().split("\t");
            if(line[0].charAt(0) == 'I'){
                sb.append("O");
                for(int i = 1; i < line.length; i++){
                    sb.append(line[i]);
                    sb.append("\t");
                    context.write(new Text("I"+ line[i]),new Text(line[0]));
                    context.write(new Text("N"), new Text("A1E-4"));
                }
                context.write(new Text(line[0]), new Text(sb.toString()));
                context.write(new  Text("N"), new Text("H" + (line.length-1)*1E-4));
            }
            else if (line[0].charAt(0) == 'N'){
                for(String url : allurls){
                    context.write(new Text(url), new Text("N" + line[1] + "\t" + line[2]));
                }
            }else{
                for(int i = 3; i < line.length; i++){
                    char type = line[i].charAt(0);
                    if (type == 'I'){
                        context.write(new Text(line[0]), new Text(line[i]));
                    }else if(type == 'O'){
                        String newkey = line[i].substring(1);
                        if(newkey.length() == 0){
                            continue;
                        }
                        Text newval = new Text("A" + line[2]);
                        context.write(new Text(newkey), new Text(newval));
                        context.write(new Text(line[0]), new Text(line[i]));
                        context.write(new  Text("N"), new Text("A" + line[2]));
                    }
                }
                context.write(new Text(line[0]), new Text("H" + line[2]));
                context.write(new Text("N"), new Text("H" + line[2]));
            }
        }
    }

    public static class HitsAutReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            StringBuilder sb = new StringBuilder();
            double in = 0;
            double out = 0;
            if (key.toString().charAt(0) == 'I') {
                for (Text value : values) {
                    char type = value.toString().charAt(0);
                    if (type == 'I'){
                        in+=1E-4;
                        sb.append(value.toString());
                        sb.append("\t");
                    }else if(type == 'O'){
                        String[] split = value.toString().substring(1).split("\t");
                        out+= split.length*1E-4;
                        for(String link : split){
                            sb.append("O");
                            sb.append(link);
                            sb.append("\t");
                        }
                    }
                }
                sb.insert(0,"\t");
                sb.insert(0,out);
                sb.insert(0,"\t");
                sb.insert(0,in);
                context.write(new Text(key.toString().substring(1)), new Text(sb.toString()));
            }else if (key.toString().charAt(0) == 'N'){
                double nh = 0.0;
                double val = 0.0;
                double na = 0.0;
                for (Text value : values){
                    char type = value.toString().charAt(0);
                    val = Double.parseDouble(value.toString().substring(1));
                    if (type == 'H'){
                        nh += val * val;
                    }else{
                        na +=val*val;
                    }
                }
                na = Math.sqrt(na);
                nh = Math.sqrt(nh);
                context.write(key, new Text(Double.toString(na) + "\t" + Double.toString(nh)));
            }else{
                String hub = "";
                double norm = 1.0;
                for(Text value: values){
                    char type = value.toString().charAt(0);
                    if(type == 'I' || type =='O'){
                        sb.append(value.toString());
                        sb.append("\t");
                    }else if (type == 'H'){
                        hub = value.toString().substring(1);
                    }else if (type == 'A'){
                        in+=Double.parseDouble(value.toString().substring(1));
                    }else if (type == 'N'){
                        String[] norms = value.toString().substring(1).split("\t");
                        norm = Double.parseDouble(norms[0]);
                    }
                }
                sb.insert(0,"\t");
                sb.insert(0,hub);
                sb.insert(0,"\t");
                sb.insert(0,in/norm);
                context.write(key, new Text(sb.toString()));
            }
        }
    }


    public static class HitsHubMapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] line = value.toString().split("\t");
            if(line[0].charAt(0) == 'N'){
                for(String url : allurls){
                    context.write(new Text(url), new Text("N" + line[1] + "\t" + line[2]));
                }
            }else {
                for (int i = 3; i < line.length; i++) {
                    char type = line[i].charAt(0);
                    if (type == 'I') {
                        context.write(new Text(line[0]), new Text(line[i]));
                        context.write(new Text(line[i].substring(1)), new Text("H" + line[1]));
                        context.write(new Text("N"), new Text("H" + line[1]));
                    } else if (type == 'O') {
                        context.write(new Text(line[0]), new Text(line[i]));
                    }
                }
                context.write(new Text(line[0]), new Text("A" + line[1]));
                context.write(new Text("N"), new Text("A" + line[1]));
            }
        }
    }

    public static class HitsHubReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            StringBuilder sb = new StringBuilder();
            if (key.toString().charAt(0) == 'N'){
                double nh = 0.0;
                double val = 0.0;
                double na = 0.0;
                for (Text value : values){
                    char type = value.toString().charAt(0);
                    val = Double.parseDouble(value.toString().substring(1));
                    if (type == 'H'){
                        nh += val * val;
                    }else{
                        na +=val*val;
                    }
                }
                na = Math.sqrt(na);
                nh = Math.sqrt(nh);
                context.write(key, new Text(Double.toString(na) + "\t" + Double.toString(nh)));
            }else {
                double out = 0;
                String aut = "";
                double norm = 1.0;
                for (Text value : values) {
                    char type = value.toString().charAt(0);
                    if (type == 'I' || type == 'O') {
                        sb.append(value.toString());
                        sb.append("\t");
                    } else if (type == 'H') {
                        out += Double.parseDouble(value.toString().substring(1));
                    } else if (type == 'A') {
                        aut = value.toString().substring(1);
                    }else if (type == 'N'){
                        String[] norms = value.toString().substring(1).split("\t");
                        norm = Double.parseDouble(norms[1]);
                    }
                }
                sb.insert(0, "\t");
                sb.insert(0, out/norm);
                sb.insert(0, "\t");
                sb.insert(0, aut);
                context.write(key, new Text(sb.toString()));
            }
        }
    }






    private Job getAutJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(Hits.class);
        job.setJobName(Hits.class.getCanonicalName());
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(HitsAutMapper.class);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setReducerClass(HitsAutReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    private Job getHubJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(Hits.class);
        job.setJobName(Hits.class.getCanonicalName());
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(HitsHubMapper.class);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setReducerClass(HitsHubReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        geturls();
        Configuration config = getConf();
        String inputStep, outputStep;
        String inputFormat  = "%s/it%02d/part-r-*";
        String outputFormat = "%s/it%02d/";
        String outputA = "./res/aut";
        String outputH = "./res/hub";
        ControlledJob[] steps = new ControlledJob[iters*2 + 1];
        steps[0] = new ControlledJob(config);
        outputStep = String.format(outputFormat, outputH, 0);
        steps[0].setJob(getAutJobConf("./res/part-r-00000", outputStep));
        for (int i = 1; i < iters*2+1; i+=2) {
            inputStep  = String.format(inputFormat,  outputH, i - 1);
            outputStep = String.format(outputFormat, outputA, i );
            steps[i] = new ControlledJob(config);
            steps[i].setJob(getAutJobConf(inputStep, outputStep));
            inputStep = String.format(inputFormat,  outputA, i );
            outputStep = String.format(outputFormat, outputH, i + 1);
            steps[i+1] = new ControlledJob(config);
            steps[i+1].setJob(getHubJobConf(inputStep, outputStep));
        }

        JobControl control = new JobControl(Hits.class.getCanonicalName());

        for (ControlledJob step: steps) {
            control.addJob(step);
        }
        for (int i = 1; i < 2*iters + 1; i++) {
            steps[i].addDependingJob(steps[i - 1]);
        }

        new Thread(control).start();
        while (!control.allFinished()) {}

        return control.getFailedJobList().isEmpty() ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        iters = Integer.parseInt(args[1]);
        int ret = ToolRunner.run(new Hits(), args);
        System.exit(ret);
    }
}