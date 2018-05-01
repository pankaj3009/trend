library(RTrade)
library(zoo)
library(rredis)
library(log4r)
library(jsonlite)
library(TTR)
library(PerformanceAnalytics)
options(scipen=999)

#### functions ####
longshortSignals<-function(s,realtime=FALSE,type=NA_character_){
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
        args[1]=1
}else if(Sys.time()<eod){
        args[1]=2
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

realtime=TRUE
today=strftime(Sys.Date(),tz=kTimeZone,format="%Y-%m-%d")


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

niftysymbols <- createIndexConstituents(2, "nifty50", threshold = strftime(as.Date(kBackTestStartDate) -  365))
#niftysymbols <- createFNOConstituents(2, "contractsize", threshold = strftime(as.Date(kBackTestStartDate) -  90))

niftysymbols<-niftysymbols[niftysymbols$startdate<=as.Date(kBackTestEndDate,tz=kTimeZone) & niftysymbols$enddate>=as.Date(kBackTestStartDate,tz=kTimeZone) ,]
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

#symbols<-c("ZEEL")

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
# nsenifty<-loadSymbol("NSENIFTY",realtime,"IND")
# trend<-Trend(nsenifty$date,nsenifty$high,nsenifty$low,nsenifty$settle)
# nsenifty$trendindex<-trend$trend
# signals<-merge(signals,nsenifty[,c("date","trendindex")],by=c("date"))
# signals$buy<-ifelse(Ref(signals$buy,-kDelay)==1 & signals$inlongtrade==1 & signals$trendindex<=0,1,0)
# signals$short<-ifelse(Ref(signals$short,-kDelay)==1 & signals$inshorttrade==1 & signals$trendindex>=0,1,0)
signals$aclose <- signals$asettle
dates <- unique(signals[order(signals$date), c("date")])
signals<-signals[order(signals$date,signals$symbol),]

if(reverse){
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

if(args[1]==2){
        saveRDS(signals,paste("signals","_",strftime(Sys.time(),"%Y%m%d %H:%M:%S"),".rds",sep=""))
}

#### symbol might have changed. update to changed symbol ####
x<-sapply(signals$symbol,grep,symbolchange$key)
potentialnames<-names(x)
index.symbolchange<-match(signals$symbol,symbolchange$key,nomatch = 1)
signals$symbol<-ifelse(index.symbolchange>1 & signals$date>=symbolchange$date[index.symbolchange],potentialnames,signals$symbol)

#### generate underlying trades ####
#trades<-ProcessSignals(signals,signals$sl.level,signals$tp.level, maxbar=rep(kMaxBars,nrow(signals)),volatilesl = TRUE,volatiletp = TRUE,maxposition=kMaxPositions,scalein=kScaleIn,debug=TRUE)
trades<-ProcessSignals(signals,rep(0,nrow(signals)),signals$tp.level, maxbar=rep(kMaxBars,nrow(signals)),volatilesl = TRUE,volatiletp = TRUE,maxposition=kMaxPositions,scalein=kScaleIn,debug=FALSE)
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
if(FALSE){
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


if(TRUE){
        #### Handle Derivaties Mapping ####
        if(nrow(trades)>0){
                trades$entrymonth <- as.Date(sapply(trades$entrytime, getExpiryDate), tz = kTimeZone)
                nextexpiry <- as.Date(sapply(as.Date(trades$entrymonth + 20, tz = kTimeZone), getExpiryDate), tz = kTimeZone)
                trades$entrycontractexpiry <- as.Date(ifelse(businessDaysBetween("India",as.Date(trades$entrytime, tz = kTimeZone),trades$entrymonth) < 1,nextexpiry,trades$entrymonth),tz = kTimeZone)
                trades$exitmonth <- as.Date(sapply(trades$exittime, getExpiryDate), tz = kTimeZone)
                nextexpiry <- as.Date(sapply(as.Date(trades$exitmonth + 20, tz = kTimeZone), getExpiryDate), tz = kTimeZone)
                trades$exitcontractexpiry <- as.Date(ifelse(businessDaysBetween("India",as.Date(trades$exittime, tz = kTimeZone),trades$exitmonth) < 1,nextexpiry,trades$exitmonth),tz = kTimeZone)
                trades<-getStrikeByClosestSettlePrice(trades,kFNODataFolder,kNiftyDataFolder,kTimeZone)
                futureTrades<-MapToFutureTrades(trades,kFNODataFolder,kNiftyDataFolder,rollover=TRUE)
        }
        
        #### update size and calculate pnl ####
        if(nrow(trades)>0 && nrow(futureTrades)>0){
                futureTrades <- futureTrades[order(futureTrades$entrytime), ]
                futureTrades$cashsymbol<-sapply(strsplit(futureTrades$symbol,"_"),"[",1)
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
                
                futureTrades$entrysize=NULL
                novalue=strptime(NA_character_,"%Y-%m-%d")
                for(i in 1:nrow(futureTrades)){
                        symbolsvector=unlist(strsplit(futureTrades$symbol[i],"_"))
                        allsize = folots[folots$symbol == symbolsvector[1], ]
                        futureTrades$size[i]=getcontractsize(futureTrades$entrytime[i],allsize)
                        if(as.numeric(futureTrades$exittime[i])==0){
                                futureTrades$exittime[i]=novalue
                        }
                }        
                
                futureTrades$entrybrokerage=ifelse(futureTrades$entryprice==0,0,ifelse(grepl("BUY",futureTrades$trade),kPerContractBrokerage+futureTrades$entryprice*futureTrades$size*kValueBrokerage,kPerContractBrokerage+futureTrades$entryprice*futureTrades$size*(kValueBrokerage+kSTTSell)))
                futureTrades$exitbrokerage=ifelse(futureTrades$exitprice==0,0,ifelse(grepl("BUY",futureTrades$trade),kPerContractBrokerage+futureTrades$entryprice*futureTrades$size*kValueBrokerage,kPerContractBrokerage+futureTrades$entryprice*futureTrades$size*(kValueBrokerage+kSTTSell)))
                futureTrades$brokerageamount=futureTrades$exitbrokerage+futureTrades$entrybrokerage
                futureTrades$brokerage=futureTrades$brokerageamount/((futureTrades$entryprice+futureTrades$exitprice)*futureTrades$size)
                futureTrades$percentprofit<-ifelse(grepl("BUY",futureTrades$trade),(futureTrades$exitprice-futureTrades$entryprice)/futureTrades$entryprice,-(futureTrades$exitprice-futureTrades$entryprice)/futureTrades$entryprice)
                futureTrades$percentprofit<-ifelse(futureTrades$exitprice==0|futureTrades$entryprice==0,0,futureTrades$percentprofit)
                futureTrades$netpercentprofit <- futureTrades$percentprofit - futureTrades$brokerage
                futureTrades$pnl<-ifelse(futureTrades$exitprice==0|futureTrades$entryprice==0,0,futureTrades$entryprice*futureTrades$netpercentprofit*futureTrades$size)
                
                ### add sl and tp levels to trade
                futureTrades.plus.signals<-merge(futureTrades,signals,by.x=c("entrytime","cashsymbol"),by.y=c("date","symbol"))
                shortlisted.columns<-c("symbol","trade","entrytime","entryprice","exittime","exitprice","exitreason","percentprofit",
                                       "bars","size","brokerage","netpercentprofit","pnl","sl.level","splitadjust")
                futureTrades<-futureTrades.plus.signals[,shortlisted.columns]
                names(futureTrades)[names(futureTrades) == 'splitadjust'] <- 'entry.splitadjust'
                futureTrades$cashsymbol<-sapply(strsplit(futureTrades$symbol,"_"),"[",1)
                futureTrades.plus.signals<-merge(futureTrades,signals[,!names(signals)%in%c("sl.level","tp.level")],by.x=c("exittime","cashsymbol"),by.y=c("date","symbol"),all.x = TRUE)
                shortlisted.columns<-c("symbol","trade","entrytime","entryprice","exittime","exitprice","exitreason","percentprofit",
                                       "bars","size","brokerage","netpercentprofit","pnl","sl.level","entry.splitadjust","splitadjust")
                futureTrades<-futureTrades.plus.signals[,shortlisted.columns]
                names(futureTrades)[names(futureTrades) == 'splitadjust'] <- 'exit.splitadjust'
                futureTrades$exit.splitadjust<-ifelse(is.na(futureTrades$exit.splitadjust),1,futureTrades$exit.splitadjust)
                
                # Adjust exit price for any splits during trade
                # uncomment the next line only for futures
                # futureTrades$exitprice=futureTrades$exitprice*futureTrades$entry.splitadjust/futureTrades$exit.splitadjust
                futureTrades$percentprofit=ifelse(grepl("SHORT",futureTrades$trade),specify_decimal((futureTrades$entryprice-futureTrades$exitprice)/(futureTrades$entryprice),2),specify_decimal((futureTrades$exitprice-futureTrades$entryprice)/(futureTrades$entryprice),2))
                futureTrades$entrybrokerage=ifelse(futureTrades$entryprice==0,0,ifelse(grepl("BUY",futureTrades$trade),kPerContractBrokerage+futureTrades$entryprice*futureTrades$size*kValueBrokerage,kPerContractBrokerage+futureTrades$entryprice*futureTrades$size*(kValueBrokerage+kSTTSell)))
                futureTrades$exitbrokerage=ifelse(futureTrades$exitprice==0,0,ifelse(grepl("BUY",futureTrades$trade),kPerContractBrokerage+futureTrades$entryprice*futureTrades$size*kValueBrokerage,kPerContractBrokerage+futureTrades$entryprice*futureTrades$size*(kValueBrokerage+kSTTSell)))
                futureTrades$brokerageamount=futureTrades$exitbrokerage+futureTrades$entrybrokerage
                futureTrades$brokerage=futureTrades$brokerageamount/((futureTrades$entryprice+futureTrades$exitprice)*futureTrades$size)
                futureTrades$percentprofit<-ifelse(futureTrades$exitprice==0|futureTrades$entryprice==0,0,futureTrades$percentprofit)
                futureTrades$netpercentprofit <- futureTrades$percentprofit - futureTrades$brokerage
                futureTrades$pnl<-ifelse(futureTrades$exitprice==0|futureTrades$entryprice==0,0,futureTrades$entryprice*futureTrades$netpercentprofit*futureTrades$size)
                
                
                #### Write to Redis ####
                if(args[1]==2 && kWriteToRedis){
                        levellog(logger, "INFO", paste("Starting scan for writing to Redis for ",args[2], sep = ""))
                        entrysize = 0
                        exitsize = 0
                        if (length(which(as.Date(futureTrades$entrytime,tz=kTimeZone) == Sys.Date())) >= 1) {
                                entrytime=which(as.Date(futureTrades$entrytime,tz=kTimeZone) == Sys.Date())
                                entrysize = sum(futureTrades[entrytime, c("size")])
                        }
                        if (length(which(as.Date(futureTrades$exittime,tz=kTimeZone) == Sys.Date() & futureTrades$exitreason!="Open")) >= 1) {
                                exittime=which(as.Date(futureTrades$exittime,tz=kTimeZone) == Sys.Date() &  futureTrades$exitreason!="Open")
                                exitsize = sum(futureTrades[exittime, c("size")])
                        }
                        
                        #Exit First, then enter
                        #Write Exit to Redis
                        
                        if (exitsize > 0 & kWriteToRedis) {
                                redisConnect()
                                redisSelect(args[3])
                                exitindices<-which(as.Date(futureTrades$exittime,tz=kTimeZone) == Sys.Date() & futureTrades$exitreason!="Open")
                                out <- futureTrades[exitindices,]
                                for (o in 1:nrow(out)) {
                                        change = 0
                                        side = "UNDEFINED"
                                        # calculate starting positions by excluding futureTrades already considered in this & prior iterations. 
                                        # Effectively, the abs(startingposition) should keep reducing for duplicate symbols.
                                        startingpositionexcluding.this=GetCurrentPosition(out[o, "symbol"], futureTrades[-exitindices[1:o],],trades.till = Sys.Date()-1,position.on = Sys.Date()-1)
                                        if(grepl("BUY",out[o,"trade"])){
                                                change=-out[o,"size"]*out[o,"entry.splitadjust"]/out[o,"exit.splitadjust"]
                                                side="SELL"
                                        }else{
                                                change=out[o,"size"]*out[o,"entry.splitadjust"]/out[o,"exit.splitadjust"]
                                                side="COVER"
                                        }
                                        startingposition = startingpositionexcluding.this-change
                                        
                                        #                        startingposition = endingposition - change
                                        order=data.frame(
                                                ParentDisplayName=out[o,"symbol"],
                                                ChildDisplayName=out[o,"symbol"],
                                                OrderSide=side,
                                                OrderReason="REGULAREXIT",
                                                OrderType="CUSTOMREL",
                                                OrderStage="INIT",
                                                StrategyOrderSize=out[o,"size"]*out[o,"entry.splitadjust"]/out[o,"exit.splitadjust"],
                                                StrategyStartingPosition=as.character(abs(startingposition)),
                                                DisplaySize=out[o,"size"],
                                                TriggerPrice="0",
                                                Scale="TRUE",
                                                OrderReference=args[2],
                                                stringsAsFactors = FALSE
                                        )
                                        redisString=toJSON(order,dataframe = c("columns"),auto_unbox = TRUE)
                                        redisString<-gsub("\\[","",redisString)
                                        redisString<-gsub("\\]","",redisString)
                                        redisRPush(paste("trades", args[2], sep = ":"),charToRaw(redisString))
                                        levellog(logger, "INFO", paste(args[2], redisString, sep = ":"))
                                }
                                redisClose()
                        }
                        
                        if (entrysize > 0 & kWriteToRedis) {
                                redisConnect()
                                redisSelect(args[3])
                                out <- futureTrades[which(as.Date(futureTrades$entrytime,tz=kTimeZone) == Sys.Date()),]
                                for (o in 1:nrow(out)) {
                                        endingposition=GetCurrentPosition(out[o, "symbol"], futureTrades)
                                        change = 0
                                        side = "UNDEFINED"
                                        if(grepl("BUY",out[o,"trade"])){
                                                change=out[o,"size"]
                                                side="BUY"
                                        }else{
                                                change=-out[o,"size"]
                                                side="SHORT"
                                        }
                                        startingposition = endingposition - change
                                        order=data.frame(
                                                ParentDisplayName=out[o,"symbol"],
                                                ChildDisplayName=out[o,"symbol"],
                                                OrderSide=side,
                                                OrderReason="REGULARENTRY",
                                                OrderType="CUSTOMREL",
                                                OrderStage="INIT",
                                                StrategyOrderSize=out[o,"size"],
                                                StrategyStartingPosition=as.character(abs(startingposition)),
                                                DisplaySize=out[o,"size"],
                                                TriggerPrice="0",
                                                Scale="TRUE",
                                                OrderReference=args[2],
                                                stringsAsFactors = FALSE
                                        )
                                        redisString=toJSON(order,dataframe = c("columns"),auto_unbox = TRUE)
                                        redisString<-gsub("\\[","",redisString)
                                        redisString<-gsub("\\]","",redisString)
                                        redisRPush(paste("trades", args[2], sep = ":"),charToRaw(redisString))
                                        levellog(logger, "INFO", paste(args[2], redisString, sep = ":"))
                                }
                                redisClose()
                        }
                        saveRDS(futureTrades,paste("futureTrades","_",strftime(Sys.time(),"%Y%m%d %H:%M:%S"),".rds",sep=""))
                        
                }
                
                if(args[1]==1 & kWriteToRedis){
                        # write sl levels on BOD
                        # update strategy
                        levellog(logger, "INFO", paste("Starting scan for sl update for ",args[2], sep = ""))
                        strategyTrades<-createPNLSummary(args[3],args[2],kBackTestStartDate,kBackTestEndDate,mdpath=kFNODataFolder,deriv=TRUE)
                        opentrades.index<-which(is.na(strategyTrades$exittime))
                        if(length(opentrades.index)>0){
                                opentrades.index<-sort(opentrades.index)
                                for(i in 1:length(opentrades.index)){
                                        ind<-opentrades.index[i]
                                        symbol<-strsplit(strategyTrades[ind,c("symbol")],"_")[[1]][1]
                                        signals.symbol<-signals[signals$symbol==symbol,]
                                        signals.symbol<-signals.symbol[order(signals.symbol$date),]
                                        df<-signals.symbol[nrow(signals.symbol),]
                                        trade.sl<-df$stoploss
                                        trade.tp<-df$tp.level
                                        if(length(trade.sl)>0 && (df$sltouched.sell==1||df$sltouched.cover==1)){
                                                rredis::redisHSet(strategyTrades[ind,c("key")],"StopLoss",charToRaw(as.character(trade.sl)))
                                        }else{
                                                rredis::redisHSet(strategyTrades[ind,c("key")],"StopLoss",charToRaw(as.character(0)))
                                        }
                                        if(length(trade.tp)>0){
                                                rredis::redisHSet(strategyTrades[ind,c("key")],"TakeProfit",charToRaw(as.character(trade.tp)))
                                        }else{
                                                rredis::redisHSet(strategyTrades[ind,c("key")],"StopLoss",charToRaw(as.character(0)))
                                        }
                                }
                        }
                }
                
                #### Print Open Positions ####
                #futureTrades <- GenerateTrades(a)
                print(paste("Profit:",sum(futureTrades$pnl)))
                print(paste("Win Ratio:",sum(futureTrades$netpercentprofit>0)/nrow(futureTrades)))
                print(paste("# Trades:",nrow(futureTrades)))
                print(futureTrades[futureTrades$exitreason=="Open",])
                filename=paste(strftime(Sys.time(),"%Y%m%d %H:%M:%S"),"trades.csv",sep="_")
                #write.csv(trades,file=filename)
                filename=paste(strftime(Sys.time(),"%Y%m%d %H:%M:%S"),"signals.csv",sep="_")
                #write.csv(a,file=filename)  
        }
}
