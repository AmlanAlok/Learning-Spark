
mvn install

spark-submit --class Twitter target/cse6331-project7-0.1.jar dummy.csv

spark-submit --class Twitter target/cse6331-project7-0.1.jar small-twitter.csv

spark-submit --class Twitter target/cse6331-project7-0.1.jar small-twitter.csv &> results.txt

