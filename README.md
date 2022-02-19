#Join in Hadoop and Spark

##map-side
### 1.hadoop
应用场景（大表和小表之间的join）

算法思想：

1. 将小表存放于内存中，分发到各个执行计算的服务器上面。
2. 根据对应的Join key进行连接。
3. 最后汇总成连接结果。

实现代码

Mapper:
```java
    private ArrayList<FinalBean> finalBeans = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException {

        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            String stationID = line.substring(4,10)+"-"+line.substring(10,15);
            String airTemp;
            if(line.charAt(87) == '+'){
                airTemp = line.substring(88,92);
            }else{
                airTemp = line.substring(87,92);
            }
            String timestamp = line.substring(15,27);
            FinalBean finalBean = new FinalBean();
            finalBean.setStationID(stationID);
            finalBean.setTemperature(airTemp);
            finalBean.setTimestamp(timestamp);
            finalBeans.add(finalBean);
        }
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String stationID = line.split(" ")[0] + "-" + line.split(" ")[1];
        String name = line.substring(13,42);
        FinalBean finalBean = new FinalBean();
        finalBean.setStationID(stationID);
        finalBean.setStationName(name);
        for(FinalBean finalBean1: finalBeans){
            if(finalBean1.getStationID().equals(stationID)){
                finalBean.setTimestamp(finalBean1.getTimestamp());
                finalBean.setTemperature(finalBean1.getTemperature());
                Text tostring = new Text();
                tostring.set(finalBean.toString());
                context.write(tostring, NullWritable.get());
            }
        }
    }
```

Driver:

```java
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(MapJoinDriver.class);

        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.addCacheFile(new URI(args[0]));
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
```

##reduce-side

hadoop

算法思想：
1. 对数据进行标记，以便于区分两个表格。
2. 在reduce时，进行两个表的Join。（需要注意的是，Iterable里面的数据是一个地址，不能直接进行保存，需要进行传值实例化保存）

Mapper:

```java

  public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, FinalBean> {

    private String filename;
    private Text outK = new Text();

    @Override
    protected void setup(Context context){
        InputSplit split = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) split;
        this.filename = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        FinalBean finalBean = new FinalBean();
        if(filename.contains("sample.txt")){
            String stationID = line.substring(4,10)+"-"+line.substring(10,15);
            String airTemp;
            if(line.charAt(87) == '+'){
                airTemp = line.substring(88,92);
            }else{
                airTemp = line.substring(87,92);
            }
            String timestamp = line.substring(15,27);
            outK.set(stationID);
            finalBean.setStationID(stationID);
            finalBean.setTemperature(airTemp);
            finalBean.setTimestamp(timestamp);
            finalBean.setFlag("record.txt");
        }else{
            String stationID = line.split(" ")[0] + "-" + line.split(" ")[1];
            String name = line.substring(13,42);
            outK.set(stationID);
            finalBean.setStationID(stationID);
            finalBean.setStationName(name);
            finalBean.setFlag("station");
        }
        context.write(outK,finalBean);
    }
}
```
Reducer:
```java
    @Override
    protected void reduce(Text key, Iterable<FinalBean> finalBeans, Context context) throws IOException, InterruptedException {
        ArrayList<FinalBean> finalBeanArrayList = new ArrayList<>();
        String keyName = "null";
        for(FinalBean value: finalBeans){
            if("station".equals(value.getFlag())){
                keyName = value.getStationName();
            }else if("record.txt".equals(value.getFlag())){
                FinalBean tmp = new FinalBean();
                try {
                    BeanUtils.copyProperties(tmp,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                finalBeanArrayList.add(tmp);
            }
        }
        for(int i=0;i<finalBeanArrayList.size();i++){

            System.out.println(finalBeanArrayList.get(i).toString());
            finalBeanArrayList.get(i).setStationName(keyName);
            Text testing = new Text();
            testing.set(finalBeanArrayList.get(i).toString());
            context.write(testing,NullWritable.get());
        }

    }
```

Driver:

```java
  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(ReduceJoinDriver.class);
        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FinalBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
```

Spark

代码：

```scala
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("map side Join")
    val sc = new SparkContext(sparkConf)
    val input1: RDD[String] = sc.textFile("src/main/resources/record.txt")
    val input2: RDD[String] = sc.textFile("src/main/resources/stationName.txt")

    val record = input1.map(line =>{
      val stationID = line.substring(4,10) + "-" + line.substring(10,15)
      var airTemp = ""
      if(line.charAt(87) == '+'){
        airTemp = line.substring(88,92)
      }else{
        airTemp = line.substring(87,92)
      }
      val timestamp = line.substring(15,27)
      (stationID, timestamp + " " + airTemp)
    })


    val station = input2.filter(_.length>42).map(line => {
      val stationID = line.split(" ")(0) + "-" + line.split(" ")(1)
      val name = line.substring(13, 42).trim
      (stationID, name)
    })

    val output = record.join(record)
    output.foreach(println)
    sc.stop()
  }
  
``` 

  实现的底层逻辑
1.   首先用groupwith()方法将两个RDD根据key返回（key，Iterable[], Iterable[]）
2.  然后两个Iterable数组进行一一对应。

groupwith() 方法实现逻辑：
1. 新建一个CoGroupedRDD[K](Seq(self, other), partitioner)
2. 剩下的就不懂了



