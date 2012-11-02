# TODO: Add comment
# 
# Author: yaboulna
###############################################################################

library(gplots)

qeval <- read.table('/u2/yaboulnaga/data/twitter-trec2011/runs2011/weightCountRecip_freqFIS/nFromTopPatterns_bm25-b0.2-k0.0-lavg9.636764_i100-t10_closedtrue-proptrue-subsetidffalse-parseModeOR-parseTQtrue-stop0-stemmedIDFtrue-mpdwfalse/TREC2011MicroblogTrack.categories.csv', header=TRUE, sep='\t', quote="\"")

anovaAndAssumptions <- function (categories){
  
  par(mfrow=c(2,2))
#We are hoping not to see
#1. Outliers — these will be apparent as separated points on the boxplots. The default is to extend the
#whiskers of the boxplot no more than one and half times the interquartiles range from the quartiles.
#Any points further away than this are plotted separately.
#2. Skewness — this will be apparent from an asymmetrical form for the boxes.
#3. Unequal variance — this will be apparent from clearly unequal box sizes. Some care is required
#because often there is very little data be used in the construction of the boxplots and so even when the
#variances truly are equal in the groups, we can expect a great deal of variability
  plot(P...30 ~ categories, data=qeval)
  
# fitting a linear model
  nclm <- lm(P...30 ~ categories-1 , data=qeval)
  
# the low p value rejects the assumption that there is no relation between y and x
  summary(nclm)
#anova(nclm) I don't know how to interpret this
  qf(0.99,11,38)
  
# if the points lie on the y=x line then the normality assumption is satisfied
  qqnorm(nclm$res)
  qqy <- (-2:2)
  qqx <- (-2:2)
  lines(qqy~qqx)
  
# Check that the dots do not cover each other that much, rght?? Whatever!
# plot(nclm$fit,nclm$res,xlab="Fitted",ylab="Residuals",main="Residual-Fitted plot")
# plot(jitter(nclm$fit),nclm$res,xlab="Fitted",ylab="Residuals",main="Jittered Residual-Fitted plot")
  par(mfrow=c(1,1))
  
  
  
#homoscedisticity
#Use Bartlett’s test if your data follow a normal, bell-shaped distribution. 
#If your samples are small, or your data are not normal (or you don’t know whether they’re normal), 
#use Levene’s test.
#If the p-value is less than the level of significance for the test (typically, 0.05), 
#the variances are not all the same. In that case, you can conclude the groups are heteroscedastic,
#as they are in the output above. (Notice that this matches the results for these 3 groups when 
#using the rule-of-thumb test and the boxplots.)
#Box.test(nclm,lag=as.numeric(ceiling(24*60/kEpochMins)),type="Ljung-Box",fitdf=length(uniFit$par))
  if(library(car,logical.return=TRUE)){
    leveneTest(nclm)
  }
}
#################### New Categories suitability of ANOVA ################
# big trouble to call!! anovaAndAssumptions(qevalNews.Categories) 


par(mfrow=c(2,2))
#We are hoping not to see
#1. Outliers — these will be apparent as separated points on the boxplots. The default is to extend the
#whiskers of the boxplot no more than one and half times the interquartiles range from the quartiles.
#Any points further away than this are plotted separately.
#2. Skewness — this will be apparent from an asymmetrical form for the boxes.
#3. Unequal variance — this will be apparent from clearly unequal box sizes. Some care is required
#because often there is very little data be used in the construction of the boxplots and so even when the
#variances truly are equal in the groups, we can expect a great deal of variability
plot(P...30 ~ News.Categories, data=qeval)

# fitting a linear model
nclm <- lm(P...30 ~ News.Categories -1 , data=qeval)

# the low p value rejects the assumption that there is no relation between y and x
summary(nclm)
#anova(nclm) I don't know how to interpret this
qf(0.99,11,38)

# if the points lie on the y=x line then the normality assumption is satisfied
qqnorm(nclm$res)
qqy <- (-2:2)
qqx <- (-2:2)
lines(qqy~qqx)

# Check that the dots do not cover each other that much, rght?? Whatever!
# plot(nclm$fit,nclm$res,xlab="Fitted",ylab="Residuals",main="Residual-Fitted plot")
# plot(jitter(nclm$fit),nclm$res,xlab="Fitted",ylab="Residuals",main="Jittered Residual-Fitted plot")
par(mfrow=c(1,1))



#homoscedisticity
#Use Bartlett’s test if your data follow a normal, bell-shaped distribution. 
#If your samples are small, or your data are not normal (or you don’t know whether they’re normal), 
#use Levene’s test.
#If the p-value is less than the level of significance for the test (typically, 0.05), 
#the variances are not all the same. In that case, you can conclude the groups are heteroscedastic,
#as they are in the output above. (Notice that this matches the results for these 3 groups when 
#using the rule-of-thumb test and the boxplots.)
#Box.test(nclm,lag=as.numeric(ceiling(24*60/kEpochMins)),type="Ljung-Box",fitdf=length(uniFit$par))
if(library(car,logical.return=TRUE)){
  leveneTest(nclm)
}
print(test <- aov(nclm))
summary(test)
#########################################################
par(mfrow=c(2,2))
plot(P...30 ~ Entity.Categories, data=qeval)
nclm <- lm(P...30 ~ Entity.Categories -1 , data=qeval)
summary(nclm)
anova(nclm)

qqnorm(nclm$res)
qqy <- (-2:2)
qqx <- (-2:2)
lines(qqy~qqx)

plot(nclm$fit,nclm$res,xlab="Fitted",ylab="Residuals",main="Residual-Fitted plot")
plot(jitter(nclm$fit),nclm$res,xlab="Fitted",ylab="Residuals",main="Jittered Residual-Fitted plot")

par(mfrow=c(1,1))

if(library(car,logical.return=TRUE)){
  leveneTest(nclm)
}
############################################################

par(mfrow=c(2,2))
plot(P...30 ~  National.International, data=qeval)
nclm <- lm(P...30 ~  National.International -1 , data=qeval)
summary(nclm)
anova(nclm)

qqnorm(nclm$res)
qqy <- (-2:2)
qqx <- (-2:2)
lines(qqy~qqx)

plot(nclm$fit,nclm$res,xlab="Fitted",ylab="Residuals",main="Residual-Fitted plot")
plot(jitter(nclm$fit),nclm$res,xlab="Fitted",ylab="Residuals",main="Jittered Residual-Fitted plot")

par(mfrow=c(1,1))

if(library(car,logical.return=TRUE)){
  leveneTest(nclm)
}
############################################################

############################################################

par(mfrow=c(1,2))
#plot(P...30 ~  National.International+News.Categories, data=qeval)
nclm <- lm(P...30 ~  National.International*News.Categories -1 , data=qeval)
summary(nclm)
anova(nclm)

qqnorm(nclm$res)
qqy <- (-2:2)
qqx <- (-2:2)
lines(qqy~qqx)

#plot(nclm$fit,nclm$res,xlab="Fitted",ylab="Residuals",main="Residual-Fitted plot")
plot(jitter(nclm$fit),nclm$res,xlab="Fitted",ylab="Residuals",main="Jittered Residual-Fitted plot")

par(mfrow=c(1,1))

if(library(car,logical.return=TRUE)){
  leveneTest(nclm)
}
############################################################

table(qeval$National.International, qeval$News.Categories)
table(qeval$Entity.Categories, qeval$News.Categories)
table(qeval$Entity.Categories, qeval$National.International)
table(qeval$Entity.Categories, qeval$News.Categories, qeval$National.International)


aovP30News <- aov(P...30 ~ News.Categories, data=qeval)
summary(aovP30News)

aovP30National <- aov(P...30 ~ National.International, data=qeval)
summary(aovP30National)

aovP30Ent <- aov(P...30 ~ Entity.Categories , data=qeval)
summary(aovP30Ent)

#aggregate(qeval$P...30, by=list(qeval$News.Categories), FUN=mean)
#aggregate(qeval$P...30, by=list(qeval$News.Categories), FUN=sd)


#plotmeans(P...30 ~ News.Categories, data=qeval, ylim=c(0,1), type="n")
#plotmeans(P...30 ~ Entity.Categories, data=qeval, ylim=c(0,1), type="n")
#plotmeans(P...30 ~ National.International, data=qeval, ylim=c(0,1), type="n")


    
aovP30NationalNews <- aov(P...30 ~ National.International * News.Categories, data=qeval)
summary(aovP30NationalNews)


aovP30NewsNational <- aov(P...30 ~ News.Categories * National.International, data=qeval)
summary(aovP30NewsNational)


aggregate(qeval$P...30, by=list(qeval$National.International, qeval$News.Categories), FUN=mean)
aggregate(qeval$P...30, by=list(qeval$National.International, qeval$News.Categories), FUN=sd)

# plotmeans(P...30 ~ National.International * News.Categories, data=qeval, ylim=c(0,1), type="n")

