spark-submit --class spark.Task --master yarn --deploy-mode client --num-executors 2 --executor-memory 1g --executor-cores 2 target/project2-1.0-SNAPSHOT-jar-with-dependencies.jar
