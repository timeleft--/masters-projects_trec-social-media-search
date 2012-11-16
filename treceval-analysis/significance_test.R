# TODO: Add comment
# 
# Author: yaboulna
###############################################################################

require(plyr)
baselineP30 <- arrange(read.table('/u2/yaboulnaga/data/twitter-trec2011/runs2011/selection/baseline_bm25-b0.2-k0.0-lavg9.636764_i100-t10_closedtrue-proptrue-subsetidffalse-parseModeOR-parseTQtrue-stop0-stemmedIDFtrue/0708080306.p30')
                ,V2)
baselineMAP <- arrange(read.table('/u2/yaboulnaga/data/twitter-trec2011/runs2011/selection/baseline_bm25-b0.2-k0.0-lavg9.636764_i100-t10_closedtrue-proptrue-subsetidffalse-parseModeOR-parseTQtrue-stop0-stemmedIDFtrue/0708080306.map')
    ,V2)
nTopP30 <- arrange(read.table('/u2/yaboulnaga/data/twitter-trec2011/runs2011/selection/nFromTopPatterns_bm25-b0.2-k0.0-lavg9.636764_i100-t10_closedtrue-proptrue-subsetidffalse-parseModeOR-parseTQtrue-stop0-stemmedIDFtrue/0708080306.p30')
    ,V2)
nTopMAP <- arrange(read.table('/u2/yaboulnaga/data/twitter-trec2011/runs2011/selection/nFromTopPatterns_bm25-b0.2-k0.0-lavg9.636764_i100-t10_closedtrue-proptrue-subsetidffalse-parseModeOR-parseTQtrue-stop0-stemmedIDFtrue/0708080306.map')
    ,V2)
t.test(nTopP30$V3, baselineP30$V3, alternative="greater", conf.level=0.99, paired=TRUE) #,var.equal=TRUE)
t.test(nTopMAP$V3, baselineMAP$V3, alternative="greater", conf.level=0.99, paired=TRUE) #,var.equal=TRUE)

par(mfrow=c(2,2))
boxplot(V3 ~ V1, data=baselineP30, notch=TRUE, varwidth=TRUE, main="baseline P@30")
boxplot(V3 ~ V1, data=nTopP30, notch=TRUE, varwidth=TRUE, main="nFromTop P@30")
boxplot(V3 ~ V1, data=baselineMAP, notch=TRUE, varwidth=TRUE, main="baseline MAP")
boxplot(V3 ~ V1, data=nTopMAP, notch=TRUE, varwidth=TRUE, main="nFromTop MAP")