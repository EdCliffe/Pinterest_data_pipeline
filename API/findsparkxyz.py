#%% 

import findspark
#os.environ["SPARK_HOME"] = "~/Programs/spark-3.2.1-bin-hadoop3.2" 
findspark.init()
#findspark.init('~/Programs/spark-3.2.1-bin-hadoop3.2')
print("running")

findspark.find()


