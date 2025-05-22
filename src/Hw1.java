import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Hw1 {


    public static class Soru1Mapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            
            String line = value.toString();
            String[] tokens = line.split("\t");
            if (tokens.length < 7) {
                return;
            }

            String genre = tokens[6];
            int reviews;
            try {
                reviews = Integer.parseInt(tokens[3]);
            } catch (NumberFormatException e) {
                return; 
            }

            context.write(new Text(genre), new IntWritable(reviews));
        }
    }

    public static class Soru1Reducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            
            int sum = 0;
            for(IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }


    public static class Soru2Mapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] tokens = line.split("\t");
            if (tokens.length < 7) {
                return;
            }

            String year = tokens[5];
            int price;
            try {
                price = Integer.parseInt(tokens[4]);
            } catch (NumberFormatException e) {
                return;
            }

            context.write(new Text(year), new IntWritable(price));
        }
    }

    public static class Soru2Reducer
            extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            double average = (count == 0) ? 0 : (sum / count);
            context.write(key, new DoubleWritable(average));
        }
    }


    public static class Soru3Mapper
            extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] tokens = line.split("\t");
            if (tokens.length < 7) {
                return;
            }

            String year = tokens[5];
            String genre = tokens[6];
            double userrating;
            try {
                userrating = Double.parseDouble(tokens[2]);
            } catch (NumberFormatException e) {
                return;
            }

            String outKey = year + "_" + genre;
            context.write(new Text(outKey), new DoubleWritable(userrating));
        }
    }

    public static class Soru3Reducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            double average = (count == 0) ? 0 : (sum / count);
            context.write(key, new DoubleWritable(average));
        }
    }


    public static class Soru4Mapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] tokens = line.split("\t");
            if (tokens.length < 7) {
                return;
            }

            String genre = tokens[6];
            String author = tokens[1];
            String reviews = tokens[3];

            context.write(new Text(genre), new Text(author + "=" + reviews));
        }
    }

    public static class Soru4Reducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Integer> authorReviewMap = new HashMap<>();

            for (Text val : values) {
                String[] parts = val.toString().split("=");
                if (parts.length != 2) {
                    continue;
                }
                String author = parts[0];
                int reviews;
                try {
                    reviews = Integer.parseInt(parts[1]);
                } catch (NumberFormatException e) {
                    continue;
                }
                authorReviewMap.put(author, authorReviewMap.getOrDefault(author, 0) + reviews);
            }

            List<Map.Entry<String, Integer>> list = new ArrayList<>(authorReviewMap.entrySet());
            list.sort((a, b) -> b.getValue().compareTo(a.getValue())); 

            int limit = Math.min(3, list.size());
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < limit; i++) {
                sb.append(list.get(i).getKey()).append(":").append(list.get(i).getValue());
                if (i < limit - 1) {
                    sb.append(", ");
                }
            }
            context.write(key, new Text(sb.toString()));
        }
    }


    public static class Soru5Mapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private String getYearGroup(int year) {
            if (year >= 2009 && year <= 2012) return "2009-2012";
            else if (year >= 2013 && year <= 2016) return "2013-2016";
            else if (year >= 2017 && year <= 2019) return "2017-2019";
            else return "other";
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] tokens = line.split("\t");
            if (tokens.length < 7) {
                return;
            }

            int yearVal;
            int reviews;
            try {
                yearVal = Integer.parseInt(tokens[5]);   
                reviews = Integer.parseInt(tokens[3]);  
            } catch (NumberFormatException e) {
                return;
            }

            String group = getYearGroup(yearVal);
            String genre = tokens[6];

            context.write(new Text(group + "_" + genre), new IntWritable(reviews));
        }
    }

    public static class Soru5Reducer
            extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            int count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            double avg = (count == 0) ? 0 : ((double) sum / count);
            context.write(key, new DoubleWritable(avg));
        }
    }


    public static class Soru6Mapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private String getYearGroup(int year) {
            if (year >= 2009 && year <= 2012) return "2009-2012";
            else if (year >= 2013 && year <= 2016) return "2013-2016";
            else if (year >= 2017 && year <= 2019) return "2017-2019";
            else return "other";
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] tokens = line.split("\t");
            if (tokens.length < 7) {
                return;
            }

            int yearVal;
            try {
                yearVal = Integer.parseInt(tokens[5]); 
            } catch (NumberFormatException e) {
                return;
            }

            String group = getYearGroup(yearVal);
            String genre = tokens[6];
            if (!genre.equalsIgnoreCase("Fiction")) {
                return; 
            }

            String bookName = tokens[0];
            int reviews;
            double userrating;
            int price;
            try {
                userrating = Double.parseDouble(tokens[2]); 
                reviews = Integer.parseInt(tokens[3]);     
                price = Integer.parseInt(tokens[4]);      
            } catch (Exception e) {
                return;
            }


            String outValue = bookName + ";" + reviews + ";" + userrating + ";" + price;
            context.write(new Text(group), new Text(outValue));
        }
    }

    public static class Soru6Reducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


            int maxReviews = -1;
            String chosenBook = "";
            double chosenRating = 0.0;
            int chosenPrice = 0;
            boolean foundExact = false;

            double bestScoreForClose = Double.MAX_VALUE;
            String closeBook = "";
            int closeReviews = 0;
            double closeRating = 0.0;
            int closePrice = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(";");
                if (parts.length < 4) {
                    continue;
                }
                String bookName = parts[0];
                int reviews = Integer.parseInt(parts[1]);
                double rating = Double.parseDouble(parts[2]);
                int price = Integer.parseInt(parts[3]);


                if (reviews > maxReviews) {
                    maxReviews = reviews;
                    chosenBook = bookName;
                    chosenRating = rating;
                    chosenPrice = price;
                }


                if (rating > 4.30 && price > 20) {
                    foundExact = true;
                }


                double dist = Math.pow(4.30 - rating, 2) + Math.pow(20 - price, 2);
                if (dist < bestScoreForClose) {
                    bestScoreForClose = dist;
                    closeBook = bookName;
                    closeReviews = reviews;
                    closeRating = rating;
                    closePrice = price;
                }
            }


            String output;
            if (foundExact && chosenRating > 4.30 && chosenPrice > 20) {
                output = String.format("Book=%s, Reviews=%d, Rating=%.2f, Price=%d",
                        chosenBook, maxReviews, chosenRating, chosenPrice);
            } else {
                output = String.format("(Closest) Book=%s, Reviews=%d, Rating=%.2f, Price=%d",
                        closeBook, closeReviews, closeRating, closePrice);
            }

            context.write(key, new Text(output));
        }
    }


    public static void main(String[] args) throws Exception {

        if(args.length < 3) {
            System.err.println("Kullanim: hadoop jar Hw1.jar Hw1 <komut> <giris> <cikis>");
            System.exit(1);
        }

        String command = args[0];
        String inputPath = args[1];
        String outputPath = args[2];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(Hw1.class);

        switch(command){
            case "total-reviews":      
                job.setMapperClass(Soru1Mapper.class);
                job.setReducerClass(Soru1Reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                break;

            case "average-price":     
                job.setMapperClass(Soru2Mapper.class);
                job.setReducerClass(Soru2Reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                break;

            case "user-rating":     
                job.setMapperClass(Soru3Mapper.class);
                job.setReducerClass(Soru3Reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);
                break;

            case "popular-authors":    
                job.setMapperClass(Soru4Mapper.class);
                job.setReducerClass(Soru4Reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                break;

            case "year-partition":    
                job.setMapperClass(Soru5Mapper.class);
                job.setReducerClass(Soru5Reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                break;

            case "conditional-split": 
                job.setMapperClass(Soru6Mapper.class);
                job.setReducerClass(Soru6Reducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                break;

            default:
                System.err.println("Gecersiz komut: " + command);
                System.exit(2);
        }

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
