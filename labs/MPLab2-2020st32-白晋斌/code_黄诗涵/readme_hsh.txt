1. 文档倒排算法：InvertedIndex.java
运行方法：hadoop jar InvertedIndex.jar /inputpath1 /outputpath1
2.全局排序：GlobalOrding.java
以1的输出作为输入
运行方法：hadoop jar GlobalOrding.jar /outputpath1/filename /outputpath2
3.计算TF-IDF：TFIDF.java
输入同1
hadoop jar TFIDF.jar /inputpath1 /outputpath3