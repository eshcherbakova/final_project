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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class part_allqueries extends Configured implements Tool {
    public static Path urlspath = new Path("/user/elena.shcherbakova/url.data");
    public static Path qidspath = new Path("/user/elena.shcherbakova/queries.tsv");
    //public static Path urlspath = new Path("./url.data/url.data");
    //public static Path qidspath = new Path("./queries.tsv");

    public static class allqMapper extends Mapper<LongWritable, Text, Text, Text> {
        public static Map<String, String> url_id = new HashMap<>();
        public static Map<String, String> id_url = new HashMap<>();
        public static Map<String, String> t_qid = new HashMap<>();
        public static Map<String, String> qid_t = new HashMap<>();
        public static HashSet<String> hosts = new HashSet<>();


        public static void geturls()
        {
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                FileStatus[] statuses = fs.globStatus(urlspath);
                for (FileStatus status : statuses)
                {
                    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status.getPath()), StandardCharsets.UTF_8));
                    String line;
                    line=br.readLine();
                    while (line != null){
                        String[] args = line.split("\t");
                        String id = args[0];
                        String okurl = args[1].charAt(args[1].length()-1)=='/' ? args[1].substring(0, args[1].length()-1) : args[1];
                        okurl = okurl.startsWith("http://") ? okurl.substring(7) : okurl;
                        okurl = okurl.startsWith("www.") ? okurl.substring(4) : okurl;
                        //hosts.add(okurl.split("/")[0]);
                        url_id.put(okurl,id);
                        id_url.put(id, okurl);
                        line=br.readLine();
                    }
                }
            }
            catch (Exception ignored){}
        }

        public static void getqids()
        {
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                FileStatus[] statuses = fs.globStatus(qidspath);
                for (FileStatus status : statuses)
                {
                    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status.getPath()), StandardCharsets.UTF_8));
                    String line;
                    line=br.readLine();
                    while (line != null){
                        String[] args = line.split("\t");
                        String id = args[0];
                        t_qid.put(args[1],id);
                        qid_t.put(id, args[1]);
                        line=br.readLine();
                    }
                }
            }
            catch (Exception ignored){}
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            //geturls();
            getqids();
        }
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String[] ind = line[0].split("@");
            String nid = t_qid.getOrDefault(ind[0], "-1");
            if (nid.equals("-1")){
                return;
            }
            HashMap<String,Integer> cmap = new HashMap<>();
            HashMap<String,Integer> lmap = new HashMap<>();
            String[] rawlinks = line[1].split(",");
            List<String> rawclicks = Arrays.asList(line[2].split(","));
            List<String> llinks = new LinkedList<>();
            List<String> lclicks = new LinkedList<>();
            String[] rawts = line[3].split(",");
            List<Double> lts = new LinkedList<>();

            for (int i = 1; i < rawts.length ; i++){
                lts.add((double) ((Long.parseLong(rawts[i].substring(5)) - Long.parseLong(rawts[i-1].substring(5)))/1000));
            }
            lts.add(300.0);
            String[] rubrics_list = {"health_consultations", "spritze.deseasescmn", "app", "spritze.deseasesmsk", "spritze.tv-programm", "converter", "answer", "games", "today", "osmino", "spritze.horoscope-sign", "meta_video", "weather", "images", "newstext", "facts", "map", "spritze.metro", "afisha", "video", "calendar", "spritze.lady-treatment", "spritze.lang", "person", "recipes", "infocard.fact", "howtos", "spritze.realty-newb", "drugs", "music", "news", "spritze.dream-term", "torgs", "youla_web", "s1port_cup", "spritze.realty-base", "NONE", "NULL", "promo_amigo"};
            HashSet<String> rset = new HashSet<>(Arrays.asList(rubrics_list));
            for (String rawlink : rawlinks) {
                if(!rset.contains(rawlink) && !rawlink.startsWith("http://")){
                    continue;
                }
                String temp = rawlink.startsWith("http://") ? rawlink.substring(7) : rawlink;
                temp = temp.startsWith("www.") ? temp.substring(4) : temp;
                if (!rset.contains(temp)) {
                    llinks.add(temp);
                }
            }
            int ctr = 0;
            for(int i = 0; i < rawclicks.size(); i++){
                if(!rset.contains(rawclicks.get(i)) && !rawclicks.get(i).startsWith("http://")){
                    ctr++;
                    continue;
                }
                String temp = rawclicks.get(i).startsWith("http://") ? rawclicks.get(i).substring(7) : rawclicks.get(i);
                temp = temp.startsWith("www.") ? temp.substring(4) : temp;
                if(!rset.contains(temp)){
                    lclicks.add(temp);
                }else{
                    try {
                        lts.remove(i - ctr);
                        ctr++;
                    }
                    catch(Exception e){
                        if(i - ctr >= lts.size()){
                            lts.remove(lts.size() - 1);
                            ctr++;
                        }
                    }
                }
            }
            String[] links = new String[llinks.size()];
            for(int i = 0; i < llinks.size(); i++){
                links[i] = llinks.get(i);
                lmap.put(links[i],i);
            }
            String[] clicks = new String[lclicks.size()];
            for(int i = 0; i < lclicks.size(); i++){
                clicks[i] = lclicks.get(i);
                cmap.put(clicks[i],i);
            }
            Double[] ts = new Double[lts.size()];
            for(int i = 0; i < lts.size(); i++){
                ts[i] = lts.get(i);
            }
            StringBuilder sb = new StringBuilder();
            sb.append(links.length).append("\t");
            sb.append(clicks.length).append("\t");
            if(ts.length > 0) {
                double maxts = Collections.max(lts);
                double mints = Collections.min(lts);
                sb.append(maxts - mints + 300).append("\t");
            }else{
                sb.append(0).append("\t");
            }
            Integer[] cl = new Integer[10];
            Arrays.fill(cl,0);
            for (int i = 0; i < 10 && i< links.length; i++){
                if(cmap.containsKey(links[i])){
                    int pos  = cmap.get(links[i]);
                    if(pos < 10) {
                        cl[pos] += 1;
                    }
                }
            }
            for(int i = 0; i < 10; i++){
                sb.append(cl[i]).append("\t");
            }
            if (clicks.length > 0) {
                sb.append(lmap.get(clicks[0])).append("\t");
                sb.append(lmap.get(clicks[clicks.length - 1])).append("\t");
            }else {
                sb.append(0).append("\t");
                sb.append(0).append("\t");
            }
            double avgb = 0.0;
            if (ts.length > 1) {
                for (int i = 1; i < ts.length; i++) {
                    avgb += ts[i] - ts[i - 1];
                }
                sb.append(avgb / (ts.length - 1));
            }else{
                sb.append(0);
            }
            context.write(new Text(nid), new Text(sb.toString()));
        }

    }

    public static class allqReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            Double[] stats = new Double[9];
            Arrays.fill(stats,0.0);
            Double[] pos = new Double[10];
            Arrays.fill(pos, 0.0);
            for(Text value: values){
                String[] line = value.toString().split("\t");
                stats[0] += 1;
                stats[1] += Double.parseDouble(line[0]);
                stats[2] += Double.parseDouble(line[1]);
                stats[3] += Double.parseDouble(line[2]);
                for(int i = 0; i<10; i++){
                    pos[i] += Double.parseDouble(line[+3]);
                }
                stats[4] += Double.parseDouble(line[13]);
                stats[5] += Double.parseDouble(line[14]);
                stats[6] += Double.parseDouble(line[15]);
                if(Double.parseDouble(line[2]) == 0){
                    stats[7] += 1;
                }
                if(Double.parseDouble(line[2]) == 1){
                    stats[8] += 1;
                }
            }
            StringBuilder sb = new StringBuilder();
            double clickapp = (stats[0] - stats[7]);
            if(clickapp == 0){
                sb.append(stats[0]).append("\t");
                sb.append(stats[1]/stats[0]).append("\t");
                sb.append(stats[2]/stats[0]).append("\t");
                sb.append(0).append("\t");//
                sb.append(0).append("\t");
                for(int i = 0; i < 10; i++){
                    sb.append(0).append("\t");
                }
                sb.append(0).append("\t");
                sb.append(0).append("\t");
                sb.append(stats[6]/clickapp).append("\t");
                sb.append(stats[7]).append("\t");
                sb.append(stats[7]/stats[0]).append("\t");
                sb.append(stats[8]).append("\t");
                sb.append(stats[8]/stats[0]).append("\t");
                sb.append(0);
                System.out.print(key);
                System.out.println(new Text(sb.toString()));
                context.write(key, new Text(sb.toString()));
                return;
            }
            sb.append(stats[0]).append("\t");
            sb.append(stats[1]/stats[0]).append("\t");
            sb.append(stats[2]/stats[0]).append("\t");
            sb.append(stats[2]/clickapp).append("\t");//
            sb.append(stats[3]/clickapp).append("\t");
            for(int i = 0; i < 10; i++){
                sb.append(pos[i]/clickapp).append("\t");
            }
            sb.append(stats[4]/clickapp).append("\t");
            sb.append(stats[5]/clickapp).append("\t");
            sb.append(stats[6]/clickapp).append("\t");
            sb.append(stats[7]).append("\t");
            sb.append(stats[7]/stats[0]).append("\t");
            sb.append(stats[8]).append("\t");
            sb.append(stats[8]/stats[0]).append("\t");
            sb.append(stats[8]/clickapp);
            System.out.print(key);
            System.out.println(new Text(sb.toString()));
            context.write(key, new Text(sb.toString()));
        }
    }
    //appearances meanl meanc meant posctr meanfcpos meanlcpos meanbetw noclick oneclick
    private Job getJobConf(final String input, final String output) throws IOException {

        final Job job = Job.getInstance(getConf());
        job.setJarByClass(part_allqueries.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setJobName(part_allqueries.class.getCanonicalName());
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(output));
        FileInputFormat.addInputPath(job, new Path(input));
        job.setMapperClass(allqMapper.class);
        job.setReducerClass(allqReducer.class);
        job.setNumReduceTasks(1);
        return job;
    }

    @Override
    public int run(final String[] args) throws Exception {
        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        final Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(final String[] args) throws Exception {
        final int ret = ToolRunner.run(new part_allqueries(), args);
        System.exit(ret);
    }
}

