# Stock-Price-Prediction
Collecting tweets from twitter, applying sentiment analysis, and predicting stock prices.


In the project we will be collecting tweets from twitter and apply sentiment analysis on it.
For now, we will start by focusing our efforts on prediction of stock prices for a large number of companies. We will gather all tweets for these companies, determine the role of the user in the company (executive, engineer, customer, etc.), perform sentiment analysis on the tweet, gather a sentiment coefficient (from an external API), and raise or reduce the predicted stock price for the company.

Our main challenge is to handle large amounts of tweet for each company. The data that comes in will be large in volume, have different forms, and come at different velocities at different times of the day.

__________________________________________________________

How to run on VCL (Ubuntu 16.04)?

1. git clone ||specify url||

2. cd Stock-Price-Prediction/TwitterDataCollection

3. To run Flume

source hadoop-setup-vcl.sh

4. To run Files conversion application

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

./hadoop-2.9.2/bin/hadoop jar JSONParsingDIC.jar DirectoryParser hdfs://localhost:9000

5. To run MapReduce TFIDF and Stock prediction

source mapreduce-tfidf.sh

___________________________________________________________

Fetching twitter data may take a long time. Check the output in logs folder in TwitterDataCollection folder. These are logs from flume. If you get some files, you may stop the process and run the following command (change path if not in TwitterDataCollection directory):

./hadoop-2.9.2/bin/hadoop dfs -ls /user/tweets

This should show some files. If yes, data is present in HDFS. To get this data on local directory, run the following:

./hadoop-2.9.2/bin/hadoop dfs -get /user/tweets

This should download a folder named 'tweets' on the current directory. Files should be in JSON format.

____________________________________________________________

Do not push the files from flume/conf directory and hadoop-setup-vcl.sh unless necessary!

___________________________________________________________
