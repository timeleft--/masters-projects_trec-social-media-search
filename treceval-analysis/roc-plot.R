# TODO: Add comment
# 
# Author: yaboulna
###############################################################################

#runFile produced by: cat uwcmb12BL.roc | grep roc | tr -d '\[\]roc' > baselineROC.cs

runFile <- 'baselineROC.csv'
runROC <- read.table(paste('/home/yaboulnaga/Dropbox/trec2012/results/',runFile, sep=""))
runROC <- runROC[,!(names(runROC) == "V4")]
plot(runROC)
for(i in 51:110){
  qROC <- runROC[which(runROC[["V1"]] == i),]
  plot(V2 ~ V3, data=qROC, type='l')
}