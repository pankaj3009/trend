library(RTrade)
library(zoo)
library(rredis)
library(log4r)
library(jsonlite)
library(TTR)
library(PerformanceAnalytics)
options(scipen=999)

symbols<-c("SBIN")
type="STK"
realtime=TRUE
s=symbols[1]
#### Parameters ####
args.commandline=commandArgs(trailingOnly=TRUE)
if(length(args.commandline)>0){
        args<-args.commandline
}
# args<-c("2","trend-lc-01","4")
# args[1] is a flag 1 => Run before bod, update sl levels in redis, 2=> Generate Signals in Production 3=> Backtest 
redisConnect()
redisSelect(1)
today=strftime(Sys.Date(),tz=kTimeZone,format="%Y-%m-%d")
bod<-paste(today, "09:08:00 IST",sep=" ")
eod<-paste(today, "15:30:00 IST",sep=" ")
if(length(args)>1){
        static<-redisHGetAll(toupper(args[2]))
}else{
        static<-redisHGetAll("TREND-LC-01")
}

newargs<-unlist(strsplit(static$args,","))
if(length(args)<=1 && length(newargs>1)){
        args<-newargs
}
redisClose()
if(Sys.time()<bod){
        args[1]=3
}else if(Sys.time()<eod){
        args[1]=3
}else{
        args[1]=3
}

md.cutoff<-Sys.time()
if(args[1]==4){
        signals.yesterday<-readRDS("signals.rds")
        trades.yesterday<-readRDS("trades.rds")
        opentrades<-trades.yesterday[trades.yesterday$exitreason=="",]
        opentrades<-opentrades[order(opentrades$entrytime),]
        if(nrow(opentrades>0)){
                md.cutoff<-opentrades$entrytime[1]
        }
}
kMaxPositions=5
kScaleIn=FALSE
kMaxBars=1000
kDelay=0
reverse=FALSE
kWriteToRedis <- as.logical(static$WriteToRedis)
kGetMarketData<-as.logical(static$GetMarketData)
kUseSystemDate<-as.logical(static$UseSystemDate)
kDataCutOffBefore<-static$DataCutOffBefore
kBackTestStartDate<-static$BackTestStartDate
kBackTestEndDate<-static$BackTestEndDate
#kBackTestStartDate<-"2017-01-01"
#kBackTestEndDate<-"2017-12-31"
kFNODataFolder <- static$FNODataFolder
kNiftyDataFolder <- static$CashDataFolder
kTimeZone <- static$TimeZone
kValueBrokerage<-as.numeric(static$SingleLegBrokerageAsPercentOfValue)/100
kPerContractBrokerage=as.numeric(static$SingleLegBrokerageAsValuePerContract)
kSTTSell=as.numeric(static$SingleLegSTTSell)/100
kMaxContracts=as.numeric(static$MaxContracts)
kRollingSwingAverage=as.numeric(static$RollingSwingAverage)
kHomeDirectory=static$HomeDirectory
kLogFile=static$LogFile
setwd(kHomeDirectory)
strategyname = args[2]
redisDB = args[3]

### parameters for metrics
kTradeSize=100000

logger <- create.logger()
logfile(logger) <- kLogFile
level(logger) <- 'INFO'

today=strftime(Sys.Date(),tz=kTimeZone,format="%Y-%m-%d")


#### functions ####
longshortSignals<-function(s,realtime=FALSE,type=NA_character_){
        print(paste(s,"...",sep=""))
        df=candleStickPattern(s,realtime=realtime)
        md<-df$marketdata
        
        pattern<-df$pattern
        pattern<-pattern[,c("pattern","date","confirmationdate","stoploss")]
        pattern.complete=pattern[complete.cases(pattern),c("pattern","confirmationdate","stoploss")]
        names(pattern.complete)[1]="pattern.complete"
        pattern.incomplete<-pattern[!complete.cases(pattern),c("pattern","date")]
        pattern.incomplete=pattern.incomplete[!duplicated(pattern.incomplete$date),]
        names(pattern.incomplete)[1]="pattern.incomplete"
        md<-merge(md,pattern.complete,by.x=c("date"),by.y=c("confirmationdate"),all.x = TRUE)
        md<-merge(md,pattern.incomplete,by.x=c("date"),by.y=c("date"),all.x = TRUE)
        md$srsi<-RSI(md[,c("asettle")],n=23)
        md$lrsi<-RSI(md[,c("alow")],n=23)
        md$hrsi<-RSI(md[,c("ahigh")],n=23)
        md$adx <-ADX(md[, c("ahigh", "alow", "asettle")])[, c("ADX")]
        md$stoploss=na.locf(md$stoploss,na.rm=FALSE)
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
                md$buy<-ifelse(md$eligible==1 & md$trend==-1 & grepl("BULLISH",md$pattern.complete) &  !grepl("BULLISH",md$pattern.incomplete),1,0)
                md$short<-ifelse(md$eligible==1  & md$trend==1 & grepl("BEARISH",md$pattern.complete) & !grepl("BEARISH",md$pattern.incomplete) ,1,0)
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
                md$sell<-ifelse(md$inlongtrade & (grepl("BEARISH",md$pattern.complete)|md$trend>=0),1,md$sell)
                md$cover<-ifelse(md$inshorttrade & (grepl("BULLISH",md$pattern.complete)|md$trend<=0),1,md$cover)
                md$sell=ifelse(md$sell |md$short,1,0)
                md$cover=ifelse(md$cover |md$buy,1,0)
                
                #md$buy=RTrade::ExRem(md$buy,md$sell)
                #md$short=RTrade::ExRem(md$short,md$cover)
                
                ### Set price arrays
                md$buyprice = md$asettle
                md$sellprice = md$asettle
                md$shortprice = md$asettle
                md$coverprice = md$asettle
                
                #mdsellprice=ifelse(md$trend>=0 & Ref(md$trend,-1)==-1,md$sl.level,md$sellprice)
                #mdcoverprice=ifelse(md$trend<=0 & Ref(md$trend,-1)==1,md$sl.level,md$coverprice)
                
                md$sellprice=ifelse(md$sellbearishstop,pmin(md$stoploss,md$aopen),md$sellprice)
                md$coverprice=ifelse(md$coverbullishstop,pmax(md$stoploss,md$aopen),md$coverprice)
                
                md$positionscore<-ifelse(md$buy>0,100-md$srsi,md$srsi)
                md$inlongtrade=ContinuingLong(md$symbol,md$buy,md$sell,md$short)
                md$inshorttrade=ContinuingShort(md$symbol,md$short,md$cover,md$buy)
                md$buycount<-with(md, ave(md$buy, cumsum(md$inlongtrade == 0), FUN = cumsum))
                md$shortcount<-with(md, ave(md$short, cumsum(md$inshorttrade == 0), FUN = cumsum))
                #md$buy=md$buy>0 & md$buycount>1
                #md$short=md$short>0 & md$shortcount>1
                md<-unique(md)
                # print(paste("completed: ",s,sep=""))
        }
        return(md)
}
#### Script ####
#update splits
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
niftysymbols=data.frame(symbol=symbols,startdate=as.Date(kBackTestStartDate,tz=kTimeZone),enddate=as.Date(kBackTestEndDate,tz=kTimeZone),stringsAsFactors = FALSE)

for(i in 1:nrow(niftysymbols)){
        niftysymbols$symbol.latest[i]<-getMostRecentSymbol(niftysymbols$symbol[i],symbolchange$key,symbolchange$newsymbol)
}

folots <- createFNOSize(2, "contractsize", threshold = strftime(as.Date(kBackTestStartDate) -  90))
symbols <- niftysymbols$symbol
options(scipen = 999)
today = strftime(Sys.Date(), tz = kTimeZone, format = "%Y-%m-%d")
md<-vector("list",length(symbols))
out <- data.frame()
signals<-data.frame()
allmd <- list()
signals<-data.frame()

for(i in 1:length(symbols)){
        df=longshortSignals(symbols[i],realtime,"STK")
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

#### symbol might have changed. update to changed symbol ####
x<-sapply(signals$symbol,grep,symbolchange$key)
potentialnames<-names(x)
index.symbolchange<-match(signals$symbol,symbolchange$key,nomatch = 1)
signals$symbol<-ifelse(index.symbolchange>1 & signals$date>=symbolchange$date[index.symbolchange],potentialnames,signals$symbol)

#### generate underlying trades ####
#trades<-ProcessSignals(signals,signals$sl.level,signals$tp.level, maxbar=rep(kMaxBars,nrow(signals)),volatilesl = TRUE,volatiletp = TRUE,maxposition=kMaxPositions,scalein=kScaleIn,debug=TRUE)
trades<-ProcessSignals(signals,rep(0,nrow(signals)),rep(0,nrow(signals)), maxbar=rep(kMaxBars,nrow(signals)),volatilesl = TRUE,volatiletp = TRUE,maxposition=kMaxPositions,scalein=kScaleIn,debug=FALSE)
trades.open.index=which(trades$exitreason=="Open")
# update open position mtm price
if(length(trades.open.index)>0){
        for(i in 1:length(trades.open.index)){
                symbol=trades$symbol[trades.open.index[i]]
                md=loadSymbol(symbol,realtime = realtime)
                md=md[which(as.Date(md$date,tz=kTimeZone)<=min(as.Date(kBackTestEndDate),Sys.Date())),]
                trades$exitprice[trades.open.index[i]]=tail(md$asettle,1)
        }
}

### metric reporting ####
if(TRUE){
        pnl<-data.frame(bizdays=as.Date(unique(signals$date),tz=kTimeZone),realized=0,unrealized=0,brokerage=0)
        trades$size=kTradeSize/trades$entryprice
        trades$entrybrokerage=ifelse(trades$entryprice==0,0,ifelse(grepl("BUY",trades$trade),kPerContractBrokerage+trades$entryprice*trades$size*kValueBrokerage,kPerContractBrokerage+trades$entryprice*trades$size*(kValueBrokerage+kSTTSell)))
        trades$exitbrokerage=ifelse(trades$exitprice==0,0,ifelse(grepl("BUY",trades$trade),kPerContractBrokerage+trades$exitprice*trades$size*kValueBrokerage,kPerContractBrokerage+trades$exitprice*trades$size*(kValueBrokerage+kSTTSell)))
        trades$brokerage=(trades$entrybrokerage+trades$exitbrokerage)/(2*trades$size)
        trades$percentprofit<-ifelse(grepl("BUY",trades$trade),(trades$exitprice-trades$entryprice)/trades$entryprice,-(trades$exitprice-trades$entryprice)/trades$entryprice)
        trades$percentprofit<-ifelse(trades$exitprice==0|trades$entryprice==0,0,trades$percentprofit)
        trades$netpercentprofit <- trades$percentprofit - trades$brokerage/(trades$entryprice+trades$exitprice)/2
        trades$abspnl=ifelse(trades$trade=="BUY",trades$size*(trades$exitprice-trades$entryprice),-trades$size*(trades$exitprice-trades$entryprice))-trades$entrybrokerage-trades$exitbrokerage
        trades$abspnl=ifelse(trades$exitprice==0,0,trades$abspnl)
        
        trades$exittime=dplyr::if_else(trades$exitreason=="Open",as.POSIXct(NA_character_),trades$exittime)
        cumpnl<-CalculateDailyPNL(trades,pnl,kNiftyDataFolder,trades$brokerage,deriv=FALSE)
        DailyPNL <- (cumpnl$realized + cumpnl$unrealized-cumpnl$brokerage) - Ref(cumpnl$realized + cumpnl$unrealized-cumpnl$brokerage, -1)
        DailyPNL <- ifelse(is.na(DailyPNL), 0, DailyPNL)
        DailyReturn <- DailyPNL/(kMaxPositions*kTradeSize)
        df <- data.frame(time = as.Date(unique(signals$date),tz=kTimeZone), return = DailyReturn)
        df <- read.zoo(df)
        sharpe <-  SharpeRatio((df[df != 0][, 1, drop = FALSE]), Rf = .07 / 365, FUN = "StdDev") * sqrt(252)
        print(paste("# Trades:",nrow(trades)))
        print(paste("Profit 1:",sum(DailyPNL)))
        print(paste("Profit 2:",sum(trades$abspnl)))
        print(paste("Return:",sum(trades$abspnl)*100/(kTradeSize*kMaxPositions)*365/as.numeric((min(as.Date(kBackTestEndDate),Sys.Date())-as.Date(kBackTestStartDate)))))
        print(paste("sharpe:", sharpe, sep = ""))
        print(paste("Win Ratio:",sum(trades$abspnl>0)*100/nrow(trades)))
        print(paste("Avg Holding Days:",sum(trades$bars)/nrow(trades)))
}
QuickChart(symbols[1],strftime(kBackTestStartDate,"%Y%m%d"),strftime(kBackTestEndDate,"%Y%m%d"),realtime,type)

