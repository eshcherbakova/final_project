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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;





public class part_all extends Configured implements Tool {
    public static Path urlspath = new Path("/user/elena.shcherbakova/url.data");
    public static Path qidspath = new Path("/user/elena.shcherbakova/queries.tsv");
    public static Path spellspath = new Path("/user/elena.shcherbakova/qspell.txt");
    //public static Path urlspath = new Path("./url.data/url.data");
    //public static Path qidspath = new Path("./queries.tsv");

    public static class allMapper extends Mapper<LongWritable, Text, Text, Text> {
        public static Map<String, String> url_id = new HashMap<>();
        public static Map<String, String> id_url = new HashMap<>();
        public static Map<String, String> t_qid = new HashMap<>();
        public static Map<String, String> nt_qid = new HashMap<>();
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
                        hosts.add(okurl.split("/")[0]);
                        //url_id.put(okurl,id);
                        //id_url.put(id, okurl);
                        line=br.readLine();
                    }
                }
            }
            catch (Exception ignored){}
        }

        public static Map<String, String> getqids(Path p)
        {
            Map<String, String> t_qid = new HashMap<>();
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                FileStatus[] statuses = fs.globStatus(p);
                for (FileStatus status : statuses)
                {
                    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status.getPath()), StandardCharsets.UTF_8));
                    String line;
                    line=br.readLine();
                    while (line != null){
                        String[] args = line.split("\t");
                        String id = args[0];
                        t_qid.put(args[1],id);
                        line=br.readLine();
                    }
                }
            }
            catch (Exception ignored){}
            return t_qid;
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            geturls();
            t_qid = getqids(qidspath);
            nt_qid = getqids(spellspath);
        }
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String[] ind = line[0].split("@");
            String nid = t_qid.getOrDefault(ind[0], "-1");
            if (nid.equals("-1")){
                nid = nt_qid.getOrDefault(ind[0], "-1");
            }
            HashMap<String,Integer> cmap = new HashMap<>();
            String[] rawlinks = line[1].split(",");
            List<String> rawclicks = Arrays.asList(line[2].split(","));
            List<String> llinks = new LinkedList<>();
            List<String> lclicks = new LinkedList<>();
            String[] rawts = line[3].split(",");
            List<Double> lts = new LinkedList<>();

            for (int i = 1; i < rawts.length ; i++){
                lts.add((double) ((Long.parseLong(rawts[i]) - Long.parseLong(rawts[i-1]))/1000));
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
            }
            String[] clicks = new String[lclicks.size()];
            for(int i = 0; i < lclicks.size(); i++){
                clicks[i] = lclicks.get(i);
            }
            Double[] ts = new Double[lts.size()];
            for(int i = 0; i < lts.size(); i++){
                ts[i] = lts.get(i);
            }
            for(int i = 0; i < links.length; i++){
                links[i] = links[i].split("/")[0];
            }
            for(int i = 0; i < clicks.length; i++){
                clicks[i] = clicks[i].split("/")[0];
                cmap.put(clicks[i],i);
            }

            for (int i = 0; i < links.length; i++){
                if (hosts.contains(links[i])) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(nid).append("\t");
                    sb.append(i + 1).append("\t");
                    sb.append(links.length - i);
                    if (cmap.containsKey(links[i])) {
                        sb.append(cmap.get(links[i]) + 1).append("\t");
                        sb.append(clicks.length - cmap.get(links[i])).append("\t");
                        int inx = 0;
                        if(cmap.get(links[i]) >= ts.length){
                            inx = ts.length - 1;
                        } else{
                            inx = cmap.get(links[i]);
                        }
                        try{
                            sb.append(ts[inx]).append("\t");
                            sb.append(inx != 0 ? ts[inx - 1] : 0).append("\t");
                            sb.append(inx != ts.length - 1 ? ts[inx + 1] : 0);
                        }catch (Exception e){
                            sb.append("300\t300\t300");
                        }
                    }
                    context.write(new Text(links[i]), new Text(sb.toString()));
                }
            }
        }

    }

    public static class allReducer extends Reducer<Text, Text, Text, Text> {

        private MultipleOutputs multipleOutputs;

        public void setup(Context context) {
            multipleOutputs = new MultipleOutputs(context);

        }
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            Map<String, Double[]> stats = new HashMap<>();
            Double[] cpositions = new Double[10];
            Double[] spositions = new Double[10];
            Double[] sifc = new Double[10];
            Arrays.fill(cpositions,0.0);
            Arrays.fill(spositions,0.0);
            Arrays.fill(sifc,0.0);
            for (Text value : values) {
                String[] items = value.toString().split("\t");
                Double[] temp = new Double[13];
                if(stats.containsKey(items[0])){
                    temp = stats.get(items[0]);
                } else{
                    Arrays.fill(temp,0.0);
                }
                if (items.length == 3){
                    temp[0] += 1;
                    temp[1] += Double.parseDouble(items[1]);
                    temp[2] += Double.parseDouble(items[2]);
                    if(Double.parseDouble(items[1]) == 1){
                        temp[7] += 1;
                    }
                    if(Double.parseDouble(items[2]) == 1){
                        temp[8] += 1;
                    }
                    if(Integer.parseInt(items[1]) - 1 < 10) {
                        spositions[Integer.parseInt(items[1]) - 1] += 1;
                    }
                }else{
                    temp[0] += 1;
                    temp[1] += Double.parseDouble(items[1]);
                    temp[2] += Double.parseDouble(items[2]);
                    temp[3] += 1;
                    temp[4] += Double.parseDouble(items[3]);
                    temp[5] += Double.parseDouble(items[4]);
                    try {
                        temp[6] += Double.parseDouble(items[5]);
                    } catch(Exception ignored){}
                    if(Double.parseDouble(items[1]) == 1){
                        temp[7] += 1;
                    }
                    if(Double.parseDouble(items[2]) == 1){
                        temp[8] += 1;
                    }
                    if(Double.parseDouble(items[3]) == 1){
                        temp[9] += 1;
                    }
                    if(Double.parseDouble(items[4]) == 1){
                        temp[10] += 1;
                    }
                    if(Integer.parseInt(items[1]) - 1 < 10) {
                        spositions[Integer.parseInt(items[1]) - 1] += 1;
                        sifc[Integer.parseInt(items[1]) - 1] += 1;
                    }
                    if(Integer.parseInt(items[3]) - 1 < 10) {
                        cpositions[Integer.parseInt(items[3]) - 1] += 1;
                    }
                    try {
                        temp[11] += Double.parseDouble(items[6]);
                    } catch(Exception ignored){}
                    try {
                        temp[12] += Double.parseDouble(items[7]);
                    } catch (Exception ignored){}
                }
                stats.put(items[0],temp);
            }
            Double[] gstats = new Double[13];
            Arrays.fill(gstats,0.0);
            for (String q: stats.keySet()){
                if(q.equals("-1")){
                    continue;
                }
                Double[] temp  = stats.get(q);
                for (int i = 0; i < gstats.length; i++){
                    gstats[i] += temp[i];
                }
                double shows = temp[0] + 0.00001;
                double clicks = temp[3] + 0.00001;
                StringBuilder sb = new StringBuilder();
                sb.append(temp[0]).append("\t");
                sb.append(temp[1]/shows).append("\t");
                sb.append(temp[2]/shows).append("\t");
                sb.append(temp[3]).append("\t");
                sb.append(temp[4]/clicks).append("\t");
                sb.append(temp[5]/clicks).append("\t");
                sb.append(Math.log1p(temp[6])/clicks).append("\t");
                sb.append(temp[7]/shows).append("\t");
                sb.append(temp[8]/shows).append("\t");
                sb.append(temp[9]/clicks).append("\t");
                sb.append(temp[10]/clicks).append("\t");
                sb.append(Math.log1p(temp[11])/ clicks).append("\t");
                sb.append(Math.log1p(temp[12])/clicks).append("\t");
                for (Double sposition : spositions) {
                    sb.append(sposition).append("\t");
                }
                for(int i = 0; i < cpositions.length; i++){
                    sb.append(cpositions[i]/(spositions[i] + 0.00001)).append("\t");
                }
                for (Double aDouble : sifc) {
                    sb.append(aDouble).append("\t");
                }
                multipleOutputs.write(new Text(q + "\t" + key.toString()), new Text(sb.toString()), "./QDF");
            }
            double shows = gstats[0] + 0.00001;
            double clicks = gstats[3] + 0.00001;
            StringBuilder sb = new StringBuilder();
            sb.append(gstats[0]).append("\t");
            sb.append(gstats[1]/shows).append("\t");
            sb.append(gstats[2]/shows).append("\t");
            sb.append(gstats[3]).append("\t");
            sb.append(gstats[4]/clicks).append("\t");
            sb.append(gstats[5]/clicks).append("\t");
            sb.append(gstats[6]/clicks).append("\t");
            sb.append(gstats[7]/shows).append("\t");
            sb.append(gstats[8]/shows).append("\t");
            sb.append(gstats[9]/clicks).append("\t");
            sb.append(gstats[10]/clicks).append("\t");
            sb.append(gstats[11]/clicks).append("\t");
            sb.append(gstats[12]/clicks).append("\t");
            multipleOutputs.write(key, new Text(sb.toString()), "./DF");
        }
    }
//shows showpos showposinv clicks clickpos clickposinv meantime firstshows lastshows firstclicks lastclicks cpos spos sifc tbef taft
    private Job getJobConf(final String input, final String output) throws IOException {

        final Job job = Job.getInstance(getConf());
        job.setJarByClass(part_all.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setJobName(part_all.class.getCanonicalName());
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(output));
        FileInputFormat.addInputPath(job, new Path(input));
        job.setMapperClass(allMapper.class);
        job.setReducerClass(allReducer.class);
        job.setNumReduceTasks(11);
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
        final int ret = ToolRunner.run(new part_all(), args);
        System.exit(ret);
    }
}

