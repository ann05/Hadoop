1. Collect tweets using twitteR
2. Collect tweets about
# Game of Thrones.
3. Run “wordcount” on the tweets (may be on the @word and #tags) and visualize the output using “tag cloud” or “word cloud’.
(This can be dynamic and realtime too!) 

4.Input: Tweets for a given domain Output: Word-cloud for the Input Processing: MR on HDFS

5. The csv file is copied to a virtual machine containing Hadoop and MapReduce algorithm on WordCount is implemented on the tweets. The count of each word is copied to a text field, 'Output.txt' which is then loaded into the R notebook. Wordcloud package is used to display the wordcloud of the tweets using count of the word as weights.
