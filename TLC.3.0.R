# Trend, LargeCap, CandleSticks, Option Trades Buy

timer.start=Sys.time()
library(RTrade)
library(log4r)
library(TTR)
library(tableHTML)
library(gmailr)

options(scipen=999)

#### PARAMETERS ####
args.commandline=commandArgs(trailingOnly=TRUE)
if(length(args.commandline)>0){
        args<-args.commandline
}

# args[1] is a flag 1 => Run before bod, update sl levels in redis, 2=> Generate Signals in Production 3=> Backtest 
redisConnect()
redisSelect(1)

if(length(args)>1){
        static<-redisHGetAll(toupper(args[2]))
}else{
        static<-redisHGetAll("TREND-LC")
}
redisClose()
newargs<-unlist(strsplit(static$args,","))
if(length(args)<=1 && length(newargs>1)){
        args<-newargs
}

today=strftime(Sys.Date(),tz=kTimeZone,format="%Y-%m-%d")
bod<-paste(today, "09:08:00 IST",sep=" ")
eod<-paste(today, "15:30:00 IST",sep=" ")

if(Sys.time()<bod){
        args[1]=1
}else if(Sys.time()<eod){
        args[1]=2
}else{
        args[1]=3
}

kMaxPositions=as.numeric(static$MaxPositions)
kScaleIn=as.numeric(static$ScaleIn)
kMaxBars=as.numeric(static$MaxBars)
kReverse=as.logical(static$Reverse)
kWriteToRedis <- as.logical(static$WriteToRedis)
kBackTestStartDate<-static$BackTestStartDate
kBackTestEndDate<-static$BackTestEndDate
#kBackTestStartDate<-"2017-01-01"
#kBackTestEndDate<-"2017-12-31"
kTimeZone <- static$TimeZone
kUnderlyingStrategy=as.character(static$UnderlyingStrategy)
kCommittedCapital=as.numeric(static$CommittedCapital)
kHomeDirectory=static$HomeDirectory
kLogFile=static$LogFile
if(!is.null(kHomeDirectory)){
        setwd(kHomeDirectory)
}
strategyname = args[2]
redisDB = args[3]
kTradeSize=500000
kMargin=as.numeric(static$Margin)
kInvestmentReturn=as.numeric(static$InvestmentReturn)
kOverdraftPenalty=as.numeric(static$OverdraftInterest)
kBackTest=as.logical(static$BackTest)
kSubscribers=fromJSON(static$Subscribers)
kBrokerage=fromJSON(static$Brokerage)
kRealtime=as.logical(static$Realtime)

logger <- create.logger()
logfile(logger) <- kLogFile
level(logger) <- 'INFO'

kSubscribers$subscribers=as.data.frame(kSubscribers$subscribers)
today = strftime(Sys.Date(), tz = kTimeZone, format = "%Y-%m-%d")
holidays=readRDS(paste(datafolder,"static/holidays.rds",sep=""))
RQuantLib::addHolidays("India",holidays)

#### FUNCTIONS ####
longshortSignals<-function(s,realtime=FALSE){
        print(paste(s,"...",sep=""))
        df=candleStickPattern(s,realtime=realtime)
        md<-df$marketdata
        pattern<-df$pattern
        
        pattern.complete=pattern[complete.cases(pattern),]
        pattern.complete=pattern.complete[order(pattern.complete$confirmationdate,-pattern.complete$duration),]
        pattern.complete=pattern.complete[!duplicated(pattern.complete$confirmationdate),]
        pattern.complete<-pattern.complete[,c("pattern","confirmationdate","stoploss","duration")]
        names(pattern.complete)[1]="pattern.complete"
        pattern.complete=unique(pattern.complete)
        
        pattern.incomplete<-pattern[!complete.cases(pattern),c("pattern","date")]
        pattern.incomplete=aggregate(pattern~date,data=pattern.incomplete,c)
        pattern.incomplete$pattern=sapply(pattern.incomplete$pattern,function(x) paste(unlist(x),collapse=","))
        pattern.incomplete=pattern.incomplete[!duplicated(pattern.incomplete$date),]
        names(pattern.incomplete)[2]="pattern.incomplete"
        md<-merge(md,pattern.complete,by.x=c("date"),by.y=c("confirmationdate"),all.x = TRUE)
        md<-merge(md,pattern.incomplete,by.x=c("date"),by.y=c("date"),all.x = TRUE)
        
        pattern.suggested<-pattern[,c("pattern","date")]
        pattern.suggested=aggregate(pattern~date,data=pattern.suggested,c)
        pattern.suggested$pattern=sapply(pattern.suggested$pattern,function(x) paste(unlist(x),collapse=","))
        pattern.suggested=pattern.suggested[!duplicated(pattern.incomplete$date),]
        names(pattern.suggested)[2]="pattern.suggested"
        md<-merge(md,pattern.suggested,by.x=c("date"),by.y=c("date"),all.x = TRUE)
        
        md$srsi<-RSI(md[,c("asettle")],n=23)
        md$lrsi<-RSI(md[,c("alow")],n=23)
        md$hrsi<-RSI(md[,c("ahigh")],n=23)
        md$adx <-ADX(md[, c("ahigh", "alow", "asettle")])[, c("ADX")]
        md$stoploss=na.locf(md$stoploss,na.rm=FALSE)
        md$daysinupswing <- BarsSince(md$updownbar <= 0)
        md$daysindownswing <- BarsSince(md$updownbar >= 0)
        md=md[md$date<=kBackTestEndDate,]
        
        if(!is.na(md)[1]){
                daysInUpTrend=BarsSince(md$trend!=1)
                daysInDownTrend=BarsSince(md$trend!=-1)
                daysIndeterminate=BarsSince(md$trend!=0)
                daysinTrend=0
                trend=0
                trend<-ifelse(daysInUpTrend>daysInDownTrend,1,ifelse(daysInUpTrend<daysInDownTrend,-1,0))
                daysinTrend<-ifelse(trend==1,daysInUpTrend,ifelse(trend==-1,daysInDownTrend,daysIndeterminate))
                
                uptrendsl =ifelse(md$trend==1,
                                  ifelse(md$swinghigh>md$swinghighhigh_1 & md$updownbar==1,md$asettle-md$swinglow,
                                         ifelse(md$swinghigh>md$swinghighhigh_1 & md$updownbar==-1,md$asettle-md$swinglowlow_1,
                                                ifelse(md$updownbar==-1,md$asettle-md$swinglow,md$asettle-md$alow))),0)
                
                
                dntrendsl =ifelse(md$trend==-1,
                                  ifelse(md$swinglow<md$swinglowlow_1 & md$updownbar==-1,md$swinghigh-md$asettle,
                                         ifelse(md$swinglow<md$swinglowlow_1 & md$updownbar==1,md$swinghighhigh_1-md$asettle,
                                                ifelse(md$updownbar==1,md$swinghigh-md$asettle,md$ahigh-md$asettle))),0)
                
                sl=uptrendsl+dntrendsl
                atr<-ATR(md[,c("ahigh","alow","asettle")],n=3)
                md$tr<-atr[,"tr"]
                md$atr<-atr[,"atr"]
                sl.level=ifelse(trend==1,md$asettle-sl, ifelse(trend==-1,md$asettle+sl,0))
                md$days.in.trend=daysinTrend
                md$sl = specify_decimal(sl,2)
                md$sl.level=sl.level
                md=md[md$date>=kBackTestStartDate & md$date<=kBackTestEndDate,]
                i=which(niftysymbols$symbol==s)[1]
                md$eligible = ifelse(as.Date(md$date) >= niftysymbols[i, c("startdate")] & as.Date(md$date) <= niftysymbols[i, c("enddate")],1,0)
                md$buy<-ifelse(md$eligible==1 & md$trend==-1 & grepl("BULLISH",md$pattern.complete) &  !grepl("BULLISH",md$pattern.suggested),1,0)
                md$short<-ifelse(md$eligible==1 & md$trend==1 & grepl("BEARISH",md$pattern.complete) & !grepl("BEARISH",md$pattern.suggested) ,1,0)
                md$sell=0
                md$cover=0
                md$inlongtrade=ContinuingLong(md$symbol, md$buy,md$sell,md$short)
                md$inshorttrade=ContinuingShort(md$symbol,md$short,md$cover,md$buy)
                
                ### Stop 1. Close outside stoploss ###
                md$sell=ifelse(md$inlongtrade & md$asettle <md$stoploss,1,md$sell)
                md$cover=ifelse(md$inshorttrade & md$asettle >md$stoploss,1,md$cover)
                
                ### Stop 2. Two shadows outside stoploss ###
                md$sltouched.sell=ifelse(md$inlongtrade==1 & Ref(md$inlongtrade,-1) & Ref(md$alow,-1)<md$stoploss,1,0)
                md$sltouched.cover=ifelse(md$inshorttrade==1 & Ref(md$inshorttrade,-1) & Ref(md$ahigh,-1)>md$stoploss,1,0)
                md$sellbearishstop=ifelse(md$alow<md$stoploss & md$sltouched.sell,1,0)
                md$coverbullishstop=ifelse(md$ahigh>md$stoploss & md$sltouched.cover,1,0)
                #md$sellbearishstop=ifelse(md$inlongtrade==1 & md$alow<md$stoploss & Ref(md$inlongtrade,-1) & Ref(md$alow,-1)<md$stoploss,1,0)
                #md$coverbullishstop=ifelse(md$inshorttrade==1 & md$ahigh>md$stoploss & Ref(md$inshorttrade,-1)==1 & Ref(md$ahigh,-1)>md$stoploss,1,0)
                md$sell=ifelse(md$sellbearishstop,1,md$sell)
                md$cover=ifelse(md$coverbullishstop,1,md$cover)
                
                ### Stop 3. Reverse Candle
                # #Original Start
                # md$sell<-ifelse(md$inlongtrade & (grepl("BEARISH",md$pattern.complete)|md$trend>=0),1,md$sell)
                # md$cover<-ifelse(md$inshorttrade & (grepl("BULLISH",md$pattern.complete)|md$trend<=0),1,md$cover)
                # # ORiginal end
                md$sell<-ifelse(md$inlongtrade & (grepl("BEARISH",md$pattern.complete)),1,md$sell)
                md$cover<-ifelse(md$inshorttrade & (grepl("BULLISH",md$pattern.complete)),1,md$cover)
                md$sell=ifelse(md$sell |md$short,1,0)
                md$cover=ifelse(md$cover |md$buy,1,0)
                
                #md$buy=RTrade::ExRem(md$buy,md$sell)
                #md$short=RTrade::ExRem(md$short,md$cover)
                
                ### Set price arrays
                md$buyprice = md$asettle
                md$sellprice = md$asettle
                md$shortprice = md$asettle
                md$coverprice = md$asettle
                
                # takeprofit
                md$tp.level=0
                md$tp.level=ifelse(md$buy==1,5*md$buyprice-4*md$stoploss,ifelse(md$short==1,5*md$shortprice-4*md$stoploss,NA_real_))
                md$tp.level=na.locf(md$tp.level,na.rm = FALSE)
                #mdsellprice=ifelse(md$trend>=0 & Ref(md$trend,-1)==-1,md$sl.level,md$sellprice)
                #mdcoverprice=ifelse(md$trend<=0 & Ref(md$trend,-1)==1,md$sl.level,md$coverprice)
                
                md$sellprice=ifelse(md$sellbearishstop,pmin(md$stoploss,md$aopen),md$sellprice)
                md$coverprice=ifelse(md$coverbullishstop,pmax(md$stoploss,md$aopen),md$coverprice)
                
                md$positionscore<-ifelse(md$buy>0,100-md$srsi,md$srsi)
                md$inlongtrade=ContinuingLong(md$symbol,md$buy,md$sell,md$short)
                md$inshorttrade=ContinuingShort(md$symbol,md$short,md$cover,md$buy)
                md$buycount<-with(md, ave(md$buy, cumsum(md$inlongtrade == 0), FUN = cumsum))
                md$shortcount<-with(md, ave(md$short, cumsum(md$inshorttrade == 0), FUN = cumsum))
                # md$exclude=(md$buy==1 & (Ref(grepl("BULLISH",md$pattern.suggested),0)))|(md$short==1 & (Ref(grepl("BEARISH",md$pattern.suggested),0)))
                # md$buy=ifelse(md$exclude,0,md$buy)
                # md$short=ifelse(md$exclude,0,md$short)
                md<-unique(md)
                # print(paste("completed: ",s,sep=""))
        }
        return(md)
}
#### GENERATE SYMBOLS ####
redisConnect()
redisSelect(2)
a<-unlist(redisSMembers("splits")) # get values from redis in a vector
tmp <- (strsplit(a, split="_")) # convert vector to list
k<-lengths(tmp) # expansion size for each list element
allvalues<-unlist(tmp) # convert list to vector
splits <- data.frame(date=1:length(a), symbol=1:length(a),oldshares=1:length(a),newshares=1:length(a),reason=rep("",length(a)),stringsAsFactors = FALSE)
for(i in 1:length(a)) {
        for(j in 1:k[i]){
                runsum=cumsum(k)[i]
                splits[i, j] <- allvalues[runsum-k[i]+j]
        }
}
splits$date=as.POSIXct(splits$date,format="%Y%m%d",tz="Asia/Kolkata")
splits$oldshares<-as.numeric(splits$oldshares)
splits$newshares<-as.numeric(splits$newshares)

#update symbol change
a<-unlist(redisSMembers("symbolchange")) # get values from redis in a vector
tmp <- (strsplit(a, split="_")) # convert vector to list
k<-lengths(tmp) # expansion size for each list element
allvalues<-unlist(tmp) # convert list to vector
symbolchange <- data.frame(date=rep("",length(a)), key=rep("",length(a)),newsymbol=rep("",length(a)),stringsAsFactors = FALSE)
for(i in 1:length(a)) {
        for(j in 1:k[i]){
                runsum=cumsum(k)[i]
                symbolchange[i, j] <- allvalues[runsum-k[i]+j]
        }
}
symbolchange$date=as.POSIXct(symbolchange$date,format="%Y%m%d",tz="Asia/Kolkata")
symbolchange$key = gsub("[^0-9A-Za-z/-]", "", symbolchange$key)
symbolchange$newsymbol = gsub("[^0-9A-Za-z/-]", "", symbolchange$newsymbol)
redisClose()

niftysymbols <- createIndexConstituents(2, "nifty50", threshold = strftime(as.Date(kBackTestStartDate) -  365))
#niftysymbols <- createFNOConstituents(2, "contractsize", threshold = strftime(as.Date(kBackTestStartDate) -  90))

niftysymbols<-niftysymbols[niftysymbols$startdate<=as.Date(kBackTestEndDate,tz=kTimeZone) & niftysymbols$enddate>=as.Date(kBackTestStartDate,tz=kTimeZone) ,]
for(i in 1:nrow(niftysymbols)){
        niftysymbols$symbol.latest[i]<-getMostRecentSymbol(niftysymbols$symbol[i])
}
niftysymbols$symbol=paste(niftysymbols$symbol,"_STK___",sep="")

folots <- createFNOSize(2, "contractsize", threshold = strftime(as.Date(kBackTestStartDate) -  90))
symbols <- niftysymbols$symbol

#### GENERATE SIGNALS ####
md<-vector("list",length(symbols))
out <- data.frame()
signals<-data.frame()
allmd <- list()
for(i in 1:length(symbols)){
        df=longshortSignals(symbols[i],kRealtime)
        df$symbol<-niftysymbols$symbol[grepl(paste("^",symbols[i],"$",sep=""),niftysymbols$symbol)][1]
        if(nrow(signals)==0){
                signals<-df
        }else{
                signals<-rbind(signals,df)
        }
        
}
signals$aclose <- signals$asettle
dates <- unique(signals[order(signals$date), c("date")])
signals<-signals[order(signals$date,signals$symbol),]

if(kReverse){
        origbuy=signals$buy
        origsell=signals$sell
        origshort=signals$short
        origcover=signals$cover
        signals$buy=origsell
        signals$short=origbuy
        signals$sell=origcover
        signals$cover=origsell
        signals$tp.level=ifelse(signals$buy>0,signals$buyprice+signals$tp,ifelse(signals$short>0,signals$shortprice+signals$tp,0))
        signals$sl.level=ifelse(signals$buy>0,signals$buyprice-signals$sl,ifelse(signals$short>0,signals$shortprice+signals$sl,0))
}
# symbol might have changed. update to changed symbol #
x<-sapply(signals$symbol,grep,symbolchange$key)
potentialnames<-names(x)
index.symbolchange<-match(signals$symbol,symbolchange$key,nomatch = 1)
signals$symbol<-ifelse(index.symbolchange>1 & signals$date>=symbolchange$date[index.symbolchange],potentialnames,signals$symbol)
#signals$cashsymbol=sapply(strsplit(signals$symbol,"_"),"[",1)
if(!kBackTest){
        saveRDS(signals,paste("signals","_",strftime(Sys.time(),,format="%Y-%m-%d %H-%M-%S"),".rds",sep=""))
}

#### GENERATE TRADES ####
trades<-ProcessSignals(signals,rep(0,nrow(signals)),signals$tp.level, maxbar=rep(kMaxBars,nrow(signals)),volatilesl = TRUE,volatiletp = TRUE,maxposition=kMaxPositions,scalein=kScaleIn,debug=FALSE)
#### MAP TO DERIVATIES ####
if(nrow(trades)>0){
        trades$size=0
        trades=revalPortfolio(trades,kBrokerage,realtime=TRUE)
        #futureTrades=MapToFutureTrades(trades,rollover=TRUE,tz=kTimeZone)
        optionTrades<-MapToOptionTradesLO(trades,rollover=TRUE,underlying="FUT",sourceInstrument = "CASH")
}

# update size and calculate pnl #
if(nrow(trades)>0 && nrow(optionTrades)>0){
        optionTrades <- optionTrades[order(optionTrades$entrytime), ]
        optionTrades$cashsymbol<-paste(sapply(strsplit(optionTrades$symbol,"_"),"[",1),"_STK___",sep="")
        getcontractsize <- function (x, size) {
                a <- size[size$startdate <= as.Date(x) & size$enddate >= as.Date(x), ]
                if (nrow(a) > 0) {
                        a <- head(a, 1)
                }
                if (nrow(a) > 0) {
                        return(a$contractsize)
                } else
                        return(0)
        }
        
        optionTrades$entrysize=NULL
        novalue=strptime(NA_character_,"%Y-%m-%d")
        for(i in 1:nrow(optionTrades)){
                symbolsvector=unlist(strsplit(optionTrades$symbol[i],"_"))
                allsize = folots[folots$symbol == symbolsvector[1], ]
                optionTrades$size[i]=getcontractsize(optionTrades$entrytime[i],allsize)
                if(as.numeric(optionTrades$exittime[i])==0){
                        optionTrades$exittime[i]=novalue
                }
        }
        
        optionTrades=revalPortfolio(optionTrades,kBrokerage,realtime = TRUE)
        # add sl and tp levels to trade
        optionTrades.plus.signals<-merge(optionTrades,signals,by.x=c("entrytime","cashsymbol"),by.y=c("date","symbol"))
        shortlisted.columns<-c("symbol","trade","entrytime","entryprice","exittime","exitprice","exitreason",
                               "bars","size","pnl","sl.level","splitadjust","cashsymbol")
        optionTrades<-optionTrades.plus.signals[,shortlisted.columns]
        names(optionTrades)[names(optionTrades) == 'splitadjust'] <- 'entry.splitadjust'
        #futureTrades$cashsymbol<-sapply(strsplit(futureTrades$symbol,"_"),"[",1)
        optionTrades.plus.signals<-merge(optionTrades,signals[,!names(signals)%in%c("sl.level","tp.level")],by.x=c("exittime","cashsymbol"),by.y=c("date","symbol"),all.x = TRUE)
        shortlisted.columns<-c("symbol","trade","entrytime","entryprice","exittime","exitprice","exitreason",
                               "bars","size","pnl","sl.level","entry.splitadjust","splitadjust","cashsymbol")
        optionTrades<-optionTrades.plus.signals[,shortlisted.columns]
        names(optionTrades)[names(optionTrades) == 'splitadjust'] <- 'exit.splitadjust'
        optionTrades$exit.splitadjust<-ifelse(is.na(optionTrades$exit.splitadjust),1,optionTrades$exit.splitadjust)
        
        # Adjust exit price for any splits during trade
        # uncomment the next line only for futures
        # futureTrades$exitprice=futureTrades$exitprice*futureTrades$entry.splitadjust/futureTrades$exit.splitadjust
        #futureTrades$cashsymbol=sapply(strsplit(futureTrades$symbol,"_"),"[",1)
        optionTrades=merge(optionTrades,signals[,c("date","symbol","pattern.complete")],all.x = TRUE,by.x=c("entrytime","cashsymbol"),by.y=c("date","symbol") )
        optionTrades$barrierlimitprice.entry=optionTrades$entryprice
        optionTrades$barrierlimitprice.exit=optionTrades$exitprice
}
print(filter(optionTrades,exitreason=="Open"))
#### WRITE TO REDIS ####
if(!kBackTest && args[1]==2 && kWriteToRedis & nrow(optionTrades)>0){
        levellog(logger, "INFO", paste("Starting scan for writing to Redis for ",args[2], sep = ""))
        saveRDS(optionTrades,paste("optionTrades","_",strftime(Sys.time(),,format="%Y-%m-%d %H-%M-%S"),".rds",sep=""))
        # referencetime=as.POSIXlt(Sys.time(),tz=kTimeZone)
        # referencetime$hour=0
        # referencetime$min=0
        # referencetime$sec=0
        # referencetime=as.POSIXct(referencetime)
        referencetime=as.POSIXct(today,tz=kTimeZone)
        order=data.frame( OrderType="CUSTOMREL",
                          OrderStage="INIT",
                          TriggerPrice="0",
                          Scale="FALSE",
                          OrderReference=tolower(args[2]),
                          TIF="GTC",
                          stringsAsFactors = FALSE)
        placeRedisOrder(optionTrades,referencetime,order,args[3])
}

if(args[1]==1 & kWriteToRedis){
        # write sl and tp levels on BOD
        # update strategy
        levellog(logger, "INFO", paste("Starting scan for sl and tp update for ",args[2], sep = ""))
        strategyTrades<-createPNLSummary(args[3],paste("*trades*",tolower(kUnderlyingStrategy),"*",sep=""),kBackTestStartDate,kBackTestEndDate)
        opentrades.index<-which(is.na(strategyTrades$exittime))
        if(length(opentrades.index)>0){
                opentrades.index<-sort(opentrades.index)
                for(i in 1:length(opentrades.index)){
                        ind<-opentrades.index[i]
                        symbol<-strsplit(strategyTrades[ind,c("symbol")],"_")[[1]][1]
                        signals.symbol<-signals[signals$symbol==paste(symbol,"_STK___",sep=""),]
                        signals.symbol<-signals.symbol[order(signals.symbol$date),]
                        df<-signals.symbol[nrow(signals.symbol),]
                        trade.sl<-df$sl.level
                        trade.tp<-df$tp.level
                        if(length(trade.tp)>0){
                                #                               rredis::redisHSet(strategyTrades[ind,c("key")],"StopLoss",charToRaw(as.character(trade.sl)))
                                rredis::redisHSet(strategyTrades[ind,c("key")],"TakeProfit",charToRaw(as.character(trade.tp)))
                        }
                }
        }
        # update execution
        strategyTrades<-createPNLSummary(0,paste("*trades*",tolower(kUnderlyingStrategy),"*",sep=""),kBackTestStartDate,kBackTestEndDate)
        opentrades.index<-which(is.na(strategyTrades$exittime))
        if(length(opentrades.index)>0){
                opentrades.index<-sort(opentrades.index)
                for(i in 1:length(opentrades.index)){
                        ind<-opentrades.index[i]
                        symbol<-strsplit(strategyTrades[ind,c("symbol")],"_")[[1]][1]
                        signals.symbol<-signals[signals$symbol==paste(symbol,"_STK___",sep=""),]
                        signals.symbol<-signals.symbol[order(signals.symbol$date),]
                        df<-signals.symbol[nrow(signals.symbol),]
                        trade.sl<-df$sl.level
                        trade.tp<-df$tp.level
                        if(length(trade.tp)>0){
                                rredis::redisHSet(strategyTrades[ind,c("key")],"TakeProfit",charToRaw(as.character(trade.tp)))
                        }
                        
                }
        }
}

#### BACKTEST ####
if(kBackTest){
        bizdays=unique(signals$date)
        bizdays=bizdays[bizdays>=as.POSIXct(kBackTestStartDate,tz=kTimeZone) & bizdays<=as.POSIXct(kBackTestEndDate,tz=kTimeZone)]
        pnl<-data.frame(bizdays,realized=0,unrealized=0,brokerage=0)
        trades$size=kTradeSize/trades$entryprice
        trades$entrybrokerage=ifelse(trades$entryprice==0,0,ifelse(grepl("BUY",trades$trade),kPerContractBrokerage+trades$entryprice*trades$size*kValueBrokerage,kPerContractBrokerage+trades$entryprice*trades$size*(kValueBrokerage+kSTTSell)))
        trades$exitbrokerage=ifelse(trades$exitprice==0,0,ifelse(grepl("BUY",trades$trade),kPerContractBrokerage+trades$exitprice*trades$size*kValueBrokerage,kPerContractBrokerage+trades$exitprice*trades$size*(kValueBrokerage+kSTTSell)))
        trades$brokerage=(trades$entrybrokerage+trades$exitbrokerage)/(2*trades$size)
        trades$percentprofit<-ifelse(grepl("BUY",trades$trade),(trades$exitprice-trades$entryprice)/trades$entryprice,-(trades$exitprice-trades$entryprice)/trades$entryprice)
        trades$percentprofit<-ifelse(trades$exitprice==0|trades$entryprice==0,0,trades$percentprofit)
        trades$netpercentprofit <- trades$percentprofit - trades$brokerage/(trades$entryprice+trades$exitprice)/2
        trades$abspnl=ifelse(trades$trade=="BUY",trades$size*(trades$exitprice-trades$entryprice),-trades$size*(trades$exitprice-trades$entryprice))-trades$entrybrokerage-trades$exitbrokerage
        trades$abspnl=ifelse(trades$exitprice==0,0,trades$abspnl)
        cumpnl<-CalculateDailyPNL(trades,pnl,kBrokerage,deriv=FALSE)
        cumpnl$idlecash=kMaxPositions*kTradeSize-cumpnl$cashdeployed
        cumpnl$daysdeployed=as.numeric(c(diff.POSIXt(cumpnl$bizdays),0))
        cumpnl$investmentreturn=ifelse(cumpnl$idlecash>0,cumpnl$idlecash*cumpnl$daysdeployed*0.06/365,-cumpnl$idlecash*cumpnl$daysdeployed*0.2/365)
        
        # calculate sharpe
        CumPNL <-  cumpnl$realized + cumpnl$unrealized - cumpnl$brokerage + cumpnl$investmentreturn
        DailyPNLWorking <-  CumPNL - Ref(CumPNL, -1)
        DailyPNLWorking <-  ifelse(is.na(DailyPNLWorking),0,DailyPNLWorking)
        DailyReturnWorking <-  ifelse(cumpnl$longnpv +cumpnl$shortnpv== 0, 0,DailyPNLWorking / kMaxPositions*kTradeSize)
        sharpe <- sharpe(DailyReturnWorking)
        
        # calculate IRR
        cumpnl$cashflow[nrow(cumpnl)]=cumpnl$cashflow[nrow(cumpnl)]+(cumpnl$longnpv+cumpnl$shortnpv)[nrow(cumpnl)]
        xirr=xirr(cumpnl$cashflow,cumpnl$bizdays,trace = TRUE)
        
        print(paste("# Trades:",nrow(trades)))
        print(paste("Profit 1:",sum(DailyPNLWorking)))
        print(paste("Profit 2:",sum(trades$abspnl)))
        print(paste("Return:",sum(trades$abspnl)*100/(kTradeSize*kMaxPositions)*365/as.numeric((min(as.Date(kBackTestEndDate),Sys.Date())-as.Date(kBackTestStartDate)))))
        print(paste("sharpe:", specify_decimal(sharpe,2), sep = ""))
        print(paste("xirr:", specify_decimal(xirr,2),sep=""))
        print(paste("Win Ratio:",sum(trades$abspnl>0)*100/nrow(trades)))
        print(paste("Avg % Profit Per Trade:",sum(trades$abspnl)*100/nrow(trades)/kTradeSize))
        print(paste("Avg Holding Days:",sum(trades$bars)/nrow(trades)))
        print("************* Long Trades ****************")
        longtrades=trades[trades$trade=="BUY",]
        print(paste("# Long Trades:",nrow(longtrades)))
        print(paste("Profit:",sum(longtrades$abspnl)))
        print(paste("Return:",sum(longtrades$abspnl)*100/(kTradeSize*kMaxPositions)*365/as.numeric((min(as.Date(kBackTestEndDate),Sys.Date())-as.Date(kBackTestStartDate)))))
        print(paste("Win Ratio:",sum(longtrades$abspnl>0)*100/nrow(longtrades)))
        print(paste("Avg % Profit Per Trade:",sum(longtrades$abspnl)*100/nrow(longtrades)/kTradeSize))
        print(paste("Avg Holding Days:",sum(longtrades$bars)/nrow(longtrades)))
        print("************* Short Trades ****************")
        shorttrades=trades[trades$trade=="SHORT",]
        print(paste("# Short Trades:",nrow(shorttrades)))
        print(paste("Profit:",sum(shorttrades$abspnl)))
        print(paste("Return:",sum(shorttrades$abspnl)*100/(kTradeSize*kMaxPositions)*365/as.numeric((min(as.Date(kBackTestEndDate),Sys.Date())-as.Date(kBackTestStartDate)))))
        print(paste("Win Ratio:",sum(shorttrades$abspnl>0)*100/nrow(shorttrades)))
        print(paste("Avg % Profit Per Trade:",sum(shorttrades$abspnl)*100/nrow(shorttrades)/kTradeSize))
        print(paste("Avg Holding Days:",sum(shorttrades$bars)/nrow(shorttrades)))
}
#### EXECUTION SUMMARY ####
if(!kBackTest){
        generateExecutionSummary(optionTrades,unique(signals$date), kBackTestStartDate,kBackTestEndDate,args[2],args[3],kSubscribers,kBrokerage,kCommittedCapital,kMargin = kMargin,kMarginOnUnrealized = TRUE,realtime=TRUE)
}

#### PRINT RUN TIME ####
timer.end=Sys.time()
runtime=timer.end-timer.start
print(runtime)