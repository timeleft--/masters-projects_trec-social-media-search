# TODO: Add comment
# 
# Author: yaboulna
###############################################################################

library(gplots)

qeval <- read.table('/u2/yaboulnaga/data/twitter-trec2011/runs2011/weightCountRecip_freqFIS/nFromTopPatterns_bm25-b0.2-k0.0-lavg9.636764_i100-t10_closedtrue-proptrue-subsetidffalse-parseModeOR-parseTQtrue-stop0-stemmedIDFtrue-mpdwfalse/TREC2011MicroblogTrack.categories.csv', header=TRUE, sep='\t', quote="\"")

#################### New Categories suitability of ANOVA ################

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

# if the points lie on the y=x line then the normality assumption is satisfied
qqnorm(nclm$res)
qqy <- (-2:2)
qqx <- (-2:2)
lines(qqy~qqx)

# Check that the dots do not cover each other that much, rght?? Whatever!
# plot(nclm$fit,nclm$res,xlab="Fitted",ylab="Residuals",main="Residual-Fitted plot")
# plot(jitter(nclm$fit),nclm$res,xlab="Fitted",ylab="Residuals",main="Jittered Residual-Fitted plot")
par(mfrow=c(1,1))
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
############################################################

plot(P...30 ~ National.International, data=qeval)
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

