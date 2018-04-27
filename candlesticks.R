# Candlesticks
library(RTrade)
library(TTR)
library(rredis)
library(dplyr)
options(max.print=5.5E5)
realtime=TRUE

type="STK"
symbols<-"UPL"

# generate signal
# update alldata buy/sell/short/cover based on signals
# generate signals for stop pattern
# update alldata for sell/cover based on stop signals
# create combined df comprising signals with corresponding trades, if any

brokerage=0.007
trendBeginning=FALSE # if trend is checked at beginning of candle pattern
conservative=FALSE #if set as TRUE, the settle has be confirmed. if FALSE, intraday range needs confirmation
kBackTestStartDate="2015-01-01"
kBackTestEndDate="2018-12-31"
LongOnly=TRUE # only long trades
ShortOnly=TRUE # only short trades
UseCandleSticksForExit=TRUE # if TRUE, exits signals are generated using candlesticks
UseConfirmedSignalForExit=TRUE # if TRUE, exit signals ONLY use confirmed exits, generated as per conservative flag
Slippage=0.1
SARDays=1
maxpositions=100

getRowIndexContainingValueX <- function(index, datarange,data) {
        tmp <- which(datarange$startvalue<data[index] & datarange$endvalue>data[index])
        if(length(tmp>0))
        return(data[index])
}

alldata<-data.frame()
signals<-data.frame(date=as.POSIXct(character()),
                    symbol=character(),
                    signalname=character(),
                    confirmation=numeric(),
                    confirmed=logical(),
                    entryprice=numeric(),
                    confirmationdate=as.POSIXct(character()),
                    stoploss=numeric(),
                    duration=numeric(),
                    stringsAsFactors = FALSE
)
# redisConnect()
# redisSelect(2)
# a<-unlist(redisSMembers("splits")) # get values from redis in a vector
# tmp <- (strsplit(a, split="_")) # convert vector to list
# k<-lengths(tmp) # expansion size for each list element
# allvalues<-unlist(tmp) # convert list to vector
# splits <- data.frame(date=1:length(a), symbol=1:length(a),oldshares=1:length(a),newshares=1:length(a),reason=rep("",length(a)),stringsAsFactors = FALSE)
# for(i in 1:length(a)) {
#         for(j in 1:k[i]){
#                 runsum=cumsum(k)[i]
#                 splits[i, j] <- allvalues[runsum-k[i]+j]
#         }
# }
# splits$date=as.POSIXct(splits$date,format="%Y%m%d",tz="Asia/Kolkata")
# splits$oldshares<-as.numeric(splits$oldshares)
# splits$newshares<-as.numeric(splits$newshares)
# 
# #update symbol change
# a<-unlist(redisSMembers("symbolchange")) # get values from redis in a vector
# tmp <- (strsplit(a, split="_")) # convert vector to list
# k<-lengths(tmp) # expansion size for each list element
# allvalues<-unlist(tmp) # convert list to vector
# symbolchange <- data.frame(date=rep("",length(a)), key=rep("",length(a)),newsymbol=rep("",length(a)),stringsAsFactors = FALSE)
# for(i in 1:length(a)) {
#         for(j in 1:k[i]){
#                 runsum=cumsum(k)[i]
#                 symbolchange[i, j] <- allvalues[runsum-k[i]+j]
#         }
# }
# symbolchange$date=as.POSIXct(symbolchange$date,format="%Y%m%d",tz="Asia/Kolkata")
# symbolchange$key = gsub("[^0-9A-Za-z/-]", "", symbolchange$key)
# symbolchange$newsymbol = gsub("[^0-9A-Za-z/-]", "", symbolchange$newsymbol)
# redisClose()
# 
# niftysymbols <- createIndexConstituents(2, "nifty50", threshold = strftime(as.Date(kBackTestStartDate) -  365))
# #niftysymbols <- createFNOConstituents(2, "contractsize", threshold = strftime(as.Date(kBackTestStartDate) -  90))
# 
# niftysymbols<-niftysymbols[niftysymbols$startdate<=as.Date(kBackTestEndDate,tz=kTimeZone) & niftysymbols$enddate>=as.Date(kBackTestStartDate,tz=kTimeZone) ,]
# for(i in 1:nrow(niftysymbols)){
#         niftysymbols$symbol.latest[i]<-getMostRecentSymbol(niftysymbols$symbol[i],symbolchange$key,symbolchange$newsymbol)
# }
# 
# folots <- createFNOSize(2, "contractsize", threshold = strftime(as.Date(kBackTestStartDate) -  90))
# symbols <- niftysymbols$symbol

for(i in 1:length(symbols)){
        print(paste("Processing:",symbols[i]))
        df=candleStickPattern(symbols[i],trendBeginning = trendBeginning,conservative = conservative,realtime=realtime,type=type)
        if(nrow(alldata)==0){
                alldata<-df$marketdata
        }else{
                all<-rbind(alldata,df$marketdata)
        }
        if(nrow(signals)==0){
                signals<-df$pattern
        }else{
                signals<-rbind(signals,df$pattern)
        }
}

#### Process Long Exit Signals ####
if(UseConfirmedSignalForExit){
        signals.exit=signals[which(signals$confirmed==TRUE),]
}else{
        signals.exit=signals
}
signals.exit=signals.exit[!duplicated(data.frame(signals.exit$date,signals.exit$symbol)),]

alldata$sell=0
alldata$sellprice=0
if(UseCandleSticksForExit ){
        selldates=signals.exit[grepl("BEARISH",signals.exit$pattern),"confirmationdate"]
        sellsymbols=signals.exit[grepl("BEARISH",signals.exit$pattern),"symbol"]
        sellprice=signals.exit[grepl("BEARISH",signals.exit$pattern),"entryprice"]
        confirmationprice=signals.exit[grepl("BEARISH",signals.exit$pattern),"confirmationprice"]
        gap=(confirmationprice-sellprice)*0.5
        sellprice=sellprice+gap
        indices=which(alldata$date %in% selldates & alldata$symbol %in% sellsymbols)
        validated=indices>0
        if(conservative){
                validated=alldata$asettle[indices]<confirmationprice
        }
        indices=indices[validated]
        alldata$sell[indices]=1
        alldata$sellprice=0
        alldata$sellprice[indices]=sellprice[validated]
}

#### Process Long Signals ####
alldata$buy=0
alldata$buyprice=0
alldata$stoploss=NA_real_
signals.entry<-signals[which(signals$confirmed & abs(signals$entryprice-signals$confirmationprice)/signals$entryprice < Slippage),]
signals.entry=signals.entry[!duplicated(data.frame(signals.entry$date,signals.entry$symbol)),]
if(LongOnly){
        buydates=signals.entry[grepl("BULLISH",signals.entry$pattern),"confirmationdate"]
        buysymbols=signals.entry[grepl("BULLISH",signals.entry$pattern),"symbol"]
        buyprice=signals.entry[grepl("BULLISH",signals.entry$pattern),"entryprice"]
        stoploss=signals.entry[grepl("BULLISH",signals.entry$pattern),"stoploss"]
        confirmationprice=signals.entry[grepl("BULLISH",signals.entry$pattern),"confirmationprice"]
        gap=(buyprice-confirmationprice)*0.5
        buyprice=buyprice-gap
        indices=which(alldata$date %in% buydates & alldata$symbol %in% buysymbols)
        validated=indices>0
        if(conservative){
                validated=alldata$asettle[indices]>confirmationprice
        }
        indices=indices[validated]
        stoploss=stoploss[validated]
        buyprice=buyprice[validated]
        alldata$buy[indices]=1
        alldata$buyprice[indices]=ifelse(alldata$buy[indices]==1,buyprice,0)
        alldata$stoploss[indices]=ifelse(alldata$buy[indices]==1,stoploss,NA_real_)        
}


#### Process Short Exit Signals ####
alldata$cover=0
alldata$coverprice=0
if(UseCandleSticksForExit ){
        coverdates=signals.exit[grepl("BULLISH",signals.exit$pattern),"confirmationdate"]
        coversymbols=signals.exit[grepl("BULLISH",signals.exit$pattern),"symbol"]
        coverprice=signals.exit[grepl("BULLISH",signals.exit$pattern),"entryprice"]
        confirmationprice=signals.exit[grepl("BULLISH",signals.exit$pattern),"confirmationprice"]
        indices=which(alldata$date %in% coverdates & alldata$symbol %in% coversymbols)
        gap=(coverprice-confirmationprice)*0.5
        coverprice=coverprice-gap
        validated=indices>0
        if(conservative){
                validated=alldata$asettle[indices]>confirmationprice
        }
        indices=indices[validated]
        alldata$cover[indices]=1
        alldata$coverprice=0
        alldata$coverprice[indices]=coverprice[validated]
}

#### Process Short Signals ####

alldata$short=0
alldata$shortprice=0
if(ShortOnly){
        shortdates=signals.entry[grepl("BEARISH",signals.entry$pattern),"confirmationdate"]
        shortsymbols=signals.entry[grepl("BEARISH",signals.entry$pattern),"symbol"]
        shortprice=signals.entry[grepl("BEARISH",signals.entry$pattern),"entryprice"]
        stoploss=signals.entry[grepl("BEARISH",signals.entry$pattern),"stoploss"]
        confirmationprice=signals.entry[grepl("BEARISH",signals.entry$pattern),"confirmationprice"]
        gap=(confirmationprice-shortprice)*0.5
        shortprice=shortprice+gap
        indices=which(alldata$date %in% shortdates & alldata$symbol %in% shortsymbols)
        validated=indices>0
        if(conservative){
                validated=alldata$asettle[indices]<confirmationprice
        }
        indices=indices[validated]
        stoploss=stoploss[validated]
        shortprice=shortprice[validated]
        alldata$short[indices]=1
        alldata$shortprice[indices]=ifelse(alldata$short[indices]==1,shortprice,0)
        alldata$stoploss[indices]=ifelse(alldata$short[indices]==1,stoploss,alldata$stoploss)
}


#### REMOVE BUY AND SHORT SIGNALS FOR SAME DAY 
# This should not be required as confirmation event means that a signals is either long or short
zerobuyvalues=ifelse(alldata$buy==1 & alldata$short==1,0,alldata$buy)
zeroshortvalues=ifelse(alldata$buy==1 & alldata$short==1,0,alldata$short)
alldata$buy=zerobuyvalues
alldata$short=zeroshortvalues

#### Update Stop ####
alldata=alldata[order(alldata$symbol,alldata$date),]
alldata$stoploss=na.locf(alldata$stoploss,na.rm=FALSE)
alldata$stoploss[is.na(stoploss)]=0
alldata$inlongtrade=ContinuingLong(alldata$symbol,alldata$buy,alldata$sell,alldata$short)
alldata$inshorttrade=ContinuingShort(alldata$symbol,alldata$short,alldata$cover,alldata$buy)
stopsignals=data.frame()
### Stop 1. Close outside stoploss ###
sellindices=alldata$inlongtrade==1 & alldata$asettle<alldata$stoploss & alldata$blackcandle
sellindices.seq=seq_along(sellindices)[sellindices]
coverindices=alldata$inshorttrade==1 & alldata$asettle>alldata$stoploss & alldata$whitecandle
coverindices.seq=seq_along(coverindices)[coverindices]
for(i in seq_along(sellindices.seq)){
        out=data.frame(symbol=alldata$symbol[sellindices.seq[i]],date=alldata$date[sellindices.seq[i]],pattern="BEARISH ABSOLUTE STOP",confirmationprice=alldata$stoploss[sellindices.seq[i]],confirmed=TRUE,confirmationdate=alldata$date[sellindices.seq[i]],entryprice=alldata$sellprice[sellindices.seq[i]],stoploss=alldata$stoploss[sellindices.seq[i]],duration=1,stringsAsFactors = FALSE)
        stopsignals=rbind(stopsignals,out)
}
for(i in seq_along(coverindices.seq)){
        out=data.frame(symbol=alldata$symbol[coverindices.seq[i]],date=alldata$date[coverindices.seq[i]],pattern="BULLISH ABSOLUTE STOP",confirmationprice=alldata$stoploss[coverindices.seq[i]],confirmed=TRUE,confirmationdate=alldata$date[coverindices.seq[i]],entryprice=alldata$sellprice[coverindices.seq[i]],stoploss=alldata$stoploss[coverindices.seq[i]],duration=1,stringsAsFactors = FALSE)
        stopsignals=rbind(stopsignals,out)
}
alldata$sell[sellindices]=1
alldata$sellprice=ifelse(alldata$sell>0,alldata$asettle-(alldata$asettle-alldata$alow)*0.5,0)
alldata$cover[coverindices]=1
alldata$coverprice=ifelse(alldata$cover>0,(alldata$ahigh-alldata$asettle)*0.5+alldata$asettle,0)
alldata$inlongtrade=ContinuingLong(alldata$symbol,alldata$buy,alldata$sell,alldata$short)
alldata$inshorttrade=ContinuingShort(alldata$symbol,alldata$short,alldata$cover,alldata$buy)

### Stop 2. Two shadows outside stoploss ###
alldata$sellbearishstop=ifelse(alldata$blackcandle==1 & alldata$inlongtrade==1 & alldata$alow<alldata$stoploss & Ref(alldata$inlongtrade,-1) & Ref(alldata$alow,-1)<alldata$stoploss,1,0)
alldata$coverbullishstop=ifelse(alldata$whitecandle==1 & alldata$inshorttrade==1 & alldata$ahigh>alldata$stoploss & Ref(alldata$inshorttrade,-1)==1 & Ref(alldata$ahigh,-1)>alldata$stoploss,1,0)
# Sell Stop
indices=which(alldata$sellbearishstop>0)
signals.doublestop=data.frame()
for(i in seq_along(indices)){
                out=data.frame(symbol=alldata$symbol[indices[i]],date=alldata$date[indices[i]],pattern="BEARISH DOUBLE STOP",confirmationprice=alldata$stoploss[indices[i]],confirmed=TRUE,confirmationdate=alldata$date[indices[i]],entryprice=alldata$sellprice[indices[i]],stoploss=alldata$stoploss[indices[i]],duration=1,stringsAsFactors = FALSE)
                signals.doublestop=rbind(signals.doublestop,out)
}
buystartindices=which(alldata$inlongtrade==1 & alldata$inlongtrade!=Ref(alldata$inlongtrade,-1))
buyendindices=which(alldata$inlongtrade==0 & alldata$inlongtrade!=Ref(alldata$inlongtrade,-1))
buystartindices=buystartindices[1:length(buyendindices)]
doublestopindices=match(signals.doublestop$confirmationdate,alldata$date) # index into alldata
indicesrange=data.frame(startvalue=buystartindices,endvalue=buyendindices) # index into alldata
a<-sapply(seq_along(doublestopindices),getRowIndexContainingValueX,indicesrange, doublestopindices )
a<-unlist(a[!is.na(a)])
a<-unique(a)
alldata$sell[a]=1
alldata$sellprice=ifelse(alldata$sell>0,alldata$asettle-(alldata$asettle-alldata$alow)*0.5,0)
for(i in seq_along(a)){
        if(length(which(stopsignals$date==alldata$date[a[i]]))==0){
                out=data.frame(symbol=alldata$symbol[a[i]],date=alldata$date[a[i]],pattern="BEARISH DOUBLE STOP",confirmationprice=alldata$stoploss[a[i]],confirmed=TRUE,confirmationdate=alldata$date[a[i]],entryprice=alldata$sellprice[a[i]],stoploss=alldata$stoploss[a[i]],duration=1,stringsAsFactors = FALSE)
                stopsignals=rbind(stopsignals,out)
        }
}

# Cover Stop

indices=which(alldata$coverbullishstop>0)
signals.doublestop=data.frame()
for(i in seq_along(indices)){
        out=data.frame(symbol=alldata$symbol[indices[i]],date=alldata$date[indices[i]],pattern="BULLISH DOUBLE STOP",confirmationprice=alldata$stoploss[indices[i]],confirmed=TRUE,confirmationdate=alldata$date[indices[i]],entryprice=alldata$sellprice[indices[i]],stoploss=alldata$stoploss[indices[i]],duration=1,stringsAsFactors = FALSE)
        signals.doublestop=rbind(signals.doublestop,out)
}
shortstartindices=which(alldata$inshorttrade==1 & alldata$inshorttrade!=Ref(alldata$inshorttrade,-1))
shortendindices=which(alldata$inshorttrade==0 & alldata$inshorttrade!=Ref(alldata$inshorttrade,-1))
shortstartindices=shortstartindices[1:length(shortendindices)]
doublestopindices=match(signals.doublestop$confirmationdate,alldata$date) # index into alldata
indicesrange=data.frame(startvalue=shortstartindices,endvalue=shortendindices) # index into alldata
a<-sapply(seq_along(doublestopindices),getRowIndexContainingValueX,indicesrange, doublestopindices )
a<-unlist(a[!is.na(a)])
a<-unique(a)
alldata$cover[a]=1
alldata$coverprice=ifelse(alldata$cover>0,(alldata$ahigh-alldata$asettle)*0.5+alldata$asettle,0)
for(i in seq_along(a)){
        if(length(which(stopsignals$date==alldata$date[a[i]]))==0){
                out=data.frame(symbol=alldata$symbol[a[i]],date=alldata$date[a[i]],pattern="BULLISH DOUBLE STOP",confirmationprice=alldata$stoploss[a[i]],confirmed=TRUE,confirmationdate=alldata$date[a[i]],entryprice=alldata$sellprice[a[i]],stoploss=alldata$stoploss[a[i]],duration=1,stringsAsFactors = FALSE)
                stopsignals=rbind(stopsignals,out)
        }
}


alldata$positionscore=100
alldata.small=alldata[alldata$date>=kBackTestStartDate,]
trades=ProcessSignals(alldata.small,slamount=rep(0,nrow(alldata)),tpamount=rep(0,nrow(alldata)),rep(1000,nrow(alldata)),maxposition=maxpositions,scalein=TRUE,debug=FALSE)

disallowtrades=logical()
lasttrade=0
for(t in 2:nrow(trades)){
        if(trades$bars[t-1]>SARDays & trades$exittime[t-1]==trades$entrytime[t] & t-1==lasttrade){
                disallowtrades[t]=TRUE
        }else{
                disallowtrades[t]=FALSE
                lasttrade=t
        }
}
disallowtrades[1]=FALSE
trades=trades[!disallowtrades,]

# prevent scalein
disallowtrades=logical()
lasttrade=""
for(t in 2:nrow(trades)){
        if(trades$trade[t-1]==trades$trade[t] & trades$exittime[t-1]>trades$entrytime[t]){
                disallowtrades[t]=TRUE
        }else{
                disallowtrades[t]=FALSE
                lasttrade=trades$trade[t]
        }
}
disallowtrades[1]=FALSE
trades=trades[!disallowtrades,]


signals.exit<-rbind(signals.exit,stopsignals)
signals.exit=signals.exit[order(signals.exit$date),]

if(nrow(trades)>0){
        trades$pnlpercent=ifelse(trades$trade=="BUY",(trades$exitprice-trades$entryprice)/trades$entryprice,-(trades$exitprice-trades$entryprice)/trades$entryprice)
        trades$pnlpercent=ifelse(trades$exitprice==0,0,trades$pnlpercent)
        trades$pnlpercent=trades$pnlpercent-0.007
        trades$equity.end=Reduce("*", trades$pnlpercent+1, accumulate = T)*100
        trades$equity.start=Ref(trades$equity.end,-1)
        trades$equity.start[1]=100
        shortlisted.columns=names(trades)
        colnames(signals.entry)[7]="signal.price"
        colnames(signals.entry)[2]="signal.date"
        #colnames(signals.entry)[3]="pattern.entry"
        trades.entry=merge(trades,signals.entry,by.x=c("symbol","entrytime"),by.y =c("symbol","confirmationdate"),all.x = TRUE,all.y = TRUE)
        trades.entry$entrytime=dplyr::if_else(is.na(trades.entry$trade),as.POSIXct(NA_character_),trades.entry$entrytime)
        trades.entry=trades.entry[,c("pattern","signal.date",shortlisted.columns)]

        colnames(signals.exit)[7]="signal.price"
        colnames(signals.exit)[2]="signal.date"
       # colnames(signals.exit)[3]="pattern.exit"
        
        signals.exit.long=signals.exit[grepl("BEARISH",signals.exit$pattern),]
        trades.exit.long=merge(trades[which(trades$trade=="BUY"),],signals.exit.long[,c("symbol","confirmationdate","pattern","signal.date")],by.x=c("symbol","exittime"),by.y =c("symbol","confirmationdate"),all.x = TRUE)
        trades.exit.long=trades.exit.long[,c("pattern","signal.date",shortlisted.columns)]
        
        signals.exit.short=signals.exit[grepl("BULLISH",signals.exit$pattern),]
        trades.exit.short=merge(trades[which(trades$trade=="SHORT"),],signals.exit.short[,c("symbol","confirmationdate","pattern","signal.date")],by.x=c("symbol","exittime"),by.y =c("symbol","confirmationdate"),all.x = TRUE)
        trades.exit.short=trades.exit.short[,c("pattern","signal.date",shortlisted.columns)]

        trades.exit=rbind(trades.exit.long,trades.exit.short)
        trades.exit$trade=ifelse(trades.exit$trade=="BUY","SELL",ifelse(trades.exit$trade=="SHORT","COVER",NA_character_))
        trades.all=rbind(trades.entry,trades.exit)
        #trades.all=merge(trades.entry,trades.exit,by=names(trades.exit)[-1],all.x = TRUE,all.y=TRUE)
        trades.all=trades.all[order(trades.all$signal.date),]
        trades.all$entrytime=if_else(trades.all$trade=="BUY"|trades.all$trade=="SHORT",trades.all$entrytime,as.POSIXct(NA_character_))
        trades.all$exittime=if_else(trades.all$trade=="SELL"|trades.all$trade=="COVER",trades.all$exittime,as.POSIXct(NA_character_))
        
        trades.all$entryprice=if_else(trades.all$trade=="BUY"|trades.all$trade=="SHORT",trades.all$entryprice,NA_real_)
        trades.all$exitprice=if_else(trades.all$trade=="SELL"|trades.all$trade=="COVER",trades.all$exitprice,NA_real_)
        trades.all=trades.all[which(!is.na(trades.all$pattern)),]
        print(paste("Percent Confirmation:",sum(!is.na(signals$confirmationdate))/nrow(signals)))
        print(paste("Returns Since ",strftime(trades$entrytime[1],format="%Y-%m-%d")," : ",(trades$equity.end[nrow(trades)]-trades$equity.start[1])/trades$equity.start[1]))
        print(tail(trades.all))
        startmonth=strftime(Sys.Date()-90,"%Y%m")
        currentmonth=strftime(Sys.Date(),"%Y%m")
        QuickChart(symbols[1],startmonth,currentmonth,realtime,type)
}