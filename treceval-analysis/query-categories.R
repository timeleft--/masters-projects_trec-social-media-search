# TODO: Add comment
# 
# Author: yaboulna
###############################################################################

library(gplots)

qeval <- read.table('/u2/yaboulnaga/data/twitter-trec2011/runs2011/weightCountRecip_freqFIS/nFromTopPatterns_bm25-b0.2-k0.0-lavg9.636764_i100-t10_closedtrue-proptrue-subsetidffalse-parseModeOR-parseTQtrue-stop0-stemmedIDFtrue-mpdwfalse/TREC2011MicroblogTrack.categories.csv', header=TRUE, sep='\t', quote="\"")

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

