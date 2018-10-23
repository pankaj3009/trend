# Trend, LargeCap, CandleSticks, Option Trades Buy

timer.start=Sys.time()
library(RTrade)
library(log4r)
library(tableHTML)
library(gmailr)


#### PARAMETERS ####
options(scipen = 999)
#options(warn=2)

args.commandline = commandArgs(trailingOnly = TRUE)
if (length(args.commandline) > 0) {
        args <- args.commandline
}

### Read Parameters ###
if (length(args) > 1) {
        static <- readRDS(paste(tolower(args[2]),".rds",sep=""))
} else{
        static <- readRDS("trend-lc.rds")
        args<-c(1,tolower(static$core$kStrategy))
}

static$core$kBackTestEndDate = strftime(adjust("India", as.Date(static$core$kBackTestEndDate, tz = static$core$kTimeZone), bdc = 2), "%Y-%m-%d")
static$core$kBackTestStartDate = strftime(adjust("India", as.Date(static$core$kBackTestStartDate, tz = static$core$kTimeZone), bdc = 0), "%Y-%m-%d")

today=strftime(Sys.Date(),tz=kTimeZone,format="%Y-%m-%d")
bod<-paste(today, "09:08:00 IST",sep=" ")
eod<-paste(today, "15:30:00 IST",sep=" ")
bizdate=adjust('India',Sys.Date(),bdc=2) #PRECEDING

if(Sys.time()<bod){
        args[1]=1
}else if(Sys.time()<eod){
        args[1]=2
}else{
        args[1]=3
}

logger <- create.logger()
logfile(logger) <- static$core$kLogFile
level(logger) <- 'INFO'
levellog(logger, "INFO", "Starting EOD Scan")

holidays=readRDS(paste(datafolder,"static/holidays.rds",sep=""))
RQuantLib::addHolidays("India",holidays)

if(get_os()=="windows"){
        setwd(static$core$kHomeDirectoryWindows)
}else if(get_os()=="linux"){
        setwd(static$core$kHomeDirectoryLinux)        
}

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
        md=md[md$date<=static$core$kBackTestEndDate,]
        
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
                md=md[md$date>=static$core$kBackTestStartDate & md$date<=static$core$kBackTestEndDate,]
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
splits=RTrade::getSplitInfo(complete = TRUE)
symbolchange=RTrade::getSymbolChange()
niftysymbols <- createIndexConstituents(2, "nifty50", threshold = strftime(as.Date(static$core$kBackTestStartDate) -  365))
#niftysymbols <- createFNOConstituents(2, "contractsize", threshold = strftime(as.Date(kBackTestStartDate) -  90))
niftysymbols<-niftysymbols[niftysymbols$startdate<=as.Date(static$core$kBackTestEndDate,tz=static$core$kTimeZone) & niftysymbols$enddate>=as.Date(static$core$kBackTestStartDate,tz=static$core$kTimeZone) ,]
for(i in 1:nrow(niftysymbols)){
        niftysymbols$symbol.latest[i]<-getMostRecentSymbol(niftysymbols$symbol[i])
}
niftysymbols$symbol=paste(niftysymbols$symbol,"_STK___",sep="")

folots <- createFNOSize(2, "contractsize", threshold = strftime(as.Date(static$core$kBackTestStartDate) -  90))
symbols <- niftysymbols$symbol

#### GENERATE SIGNALS ####
md<-vector("list",length(symbols))
out <- data.frame()
signals<-data.frame()
allmd <- list()
for(i in 1:length(symbols)){
        df=longshortSignals(symbols[i],static$core$kRealTime)
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

if(static$core$kReverse){
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
# this logic is now commented as we are using getMostRecentSymbol() upstairs
# x<-sapply(signals$symbol,grep,symbolchange$oldsymbol)
# potentialnames<-names(x)
# index.symbolchange<-match(signals$symbol,symbolchange$oldsymbol,nomatch = 1)
# signals$symbol<-ifelse(index.symbolchange>1 & signals$date>=symbolchange$date[index.symbolchange],potentialnames,signals$symbol)

if(!static$core$kBackTest){
        saveRDS(signals,paste("signals","_",strftime(Sys.time(),,format="%Y-%m-%d %H-%M-%S"),".rds",sep=""))
}

#### GENERATE TRADES ####
trades<-ProcessSignals(signals,rep(0,nrow(signals)),signals$tp.level, maxbar=rep(static$core$kMaxBars,nrow(signals)),volatilesl = TRUE,volatiletp = TRUE,maxposition=static$core$kMaxPositions,scalein=static$core$kScaleIn,debug=FALSE)
#### MAP TO DERIVATIES ####
if(nrow(trades)>0){
        trades$size=0
        trades=revalPortfolio(trades,static$core$kBrokerage,realtime=static$core$kRealTime)
        #futureTrades=MapToFutureTrades(trades,rollover=TRUE,tz=kTimeZone)
        #opentrades=filter(trades,exitreason=="Open")
        openorders=createPNLSummary(static$core$kSubscribers$redisdb[1],pattern=paste0("opentrades_",static$core$kStrategy,"*"),static$core$kBackTestStartDate,static$core$kBackTestEndDate)
        if(nrow(openorders)>0){
                names(openorders)[which(names(openorders)=="symbol")]="optionsymbol"
                openorders$symbol=sapply(strsplit(openorders$optionsymbol,"_"),"[",1)
                openorders$symbol=paste(openorders$symbol,"_STK___",sep="")
                openorders$exitreason="Open"
                trades$ModExitReason=ifelse(trades$exitreason!="Open" & as.Date(trades$exittime,tz=static$core$kTimeZone)==bizdate,"Open",trades$exitreason)
                partialMappedTrades=merge(trades,openorders[,c("symbol","exitreason","optionsymbol")],by.x = c("symbol","ModExitReason"),by.y=c("symbol","exitreason"),all.x = TRUE)
                partialMappedTrades=partialMappedTrades[order(partialMappedTrades$entrytime),]
                partialMappedTrades$symbol=ifelse(!is.na(partialMappedTrades$optionsymbol),partialMappedTrades$optionsymbol,partialMappedTrades$symbol)
                partialMappedTrades=partialMappedTrades[,!names(partialMappedTrades) %in% c("optionsymbol")]
        }else{
                partialMappedTrades=trades
        }
        optionTrades<-MapToOptionTradesLO(partialMappedTrades,rollover=TRUE,underlying="FUT",sourceInstrument = "CASH")
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
        
        optionTrades=revalPortfolio(optionTrades,static$core$kBrokerage,realtime = static$core$kRealTime)
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
if(!static$core$kBackTest && args[1]==2 && static$core$kWriteToRedis & nrow(optionTrades)>0){
        levellog(logger, "INFO", paste("Starting scan for writing to Redis for ",args[2], sep = ""))
        saveRDS(optionTrades,paste("optionTrades","_",strftime(Sys.time(),,format="%Y-%m-%d %H-%M-%S"),".rds",sep=""))
        # referencetime=as.POSIXlt(Sys.time(),tz=kTimeZone)
        # referencetime$hour=0
        # referencetime$min=0
        # referencetime$sec=0
        # referencetime=as.POSIXct(referencetime)
        referencetime=as.POSIXct(today,tz=static$core$kTimeZone)
        order=data.frame( OrderType="CUSTOMREL",
                          OrderStage="INIT",
                          TriggerPrice="0",
                          Scale="FALSE",
                          OrderReference=tolower(static$core$kStrategy),
                          OrderTime=Sys.time(),
                          TIF="GTC",
                          stringsAsFactors = FALSE)
        placeRedisOrder(optionTrades,referencetime,order,static$core$kSubscribers$redisdb[1])
}

if(args[1]==1 & static$core$kWriteToRedis){
        # write sl and tp levels on BOD
        # update strategy
        levellog(logger, "INFO", paste("Starting scan for sl and tp update for ",args[2], sep = ""))
        strategyTrades<-createPNLSummary(static$core$kSubscribers$redisdb[1],paste("*trades*",tolower(static$core$kStrategy),"*",sep=""),static$core$kBackTestStartDate,static$core$kBackTestEndDate)
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
                                redisConnect()
                                redisSelect(static$core$kSubscribers$redisdb[1])
                                rredis::redisHSet(strategyTrades[ind,c("key")],"TakeProfit",charToRaw(as.character(trade.tp)))
                                redisClose()
                        }
                }
        }
        # update execution
        strategyTrades<-createPNLSummary(0,paste("*trades*",tolower(static$core$kStrategy),"*",sep=""),static$core$kBackTestStartDate,static$core$kBackTestEndDate)
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
                                redisConnect()
                                redisSelect(0)
                                rredis::redisHSet(strategyTrades[ind,c("key")],"TakeProfit",charToRaw(as.character(trade.tp)))
                                redisClose()
                        }
                        
                }
        }
}

#### BACKTEST ####
if(static$core$kBackTest){
        trades$size=static$kTradeSize/trades$entryprice
        trades=revalPortfolio(trades,static$core$kBrokerage,realtime=FALSE,allocation=1)
        bizdays=unique(signals$date)
        bizdays=bizdays[bizdays>=as.POSIXct(static$core$kBackTestStartDate,tz=static$core$kTimeZone) & bizdays<=as.POSIXct(static$core$kBackTestEndDate,tz=static$core$kTimeZone)]
        pnl<-data.frame(bizdays,realized=0,unrealized=0,brokerage=0)
        cumpnl<-CalculateDailyPNL(trades,pnl,static$core$kBrokerage)
        cumpnl$idlecash=static$core$kCommittedCapital*static$core$kSubscribers$allocation[1]-cumpnl$cashdeployed
        cumpnl$daysdeployed=as.numeric(c(diff.POSIXt(cumpnl$bizdays),0))
        cumpnl$investmentreturn=ifelse(cumpnl$idlecash>0,cumpnl$idlecash*cumpnl$daysdeployed*static$core$kInvestmentReturn/365,-cumpnl$idlecash*cumpnl$daysdeployed*static$core$kOverdraftPenalty/365)
        cumpnl$investmentreturn=cumsum(cumpnl$investmentreturn)
        
        # calculate sharpe
        pnl <-  cumpnl$realized + cumpnl$unrealized - cumpnl$brokerage + cumpnl$investmentreturn
        dailypnl <-  pnl - Ref(pnl, -1)
        dailypnl <-  ifelse(is.na(dailypnl),0,dailypnl)
        dailyreturn <-  ifelse(cumpnl$longnpv +cumpnl$shortnpv== 0, 0,dailypnl / static$core$kCommittedCapital)
        sharpe <- sharpe(dailyreturn)
        sharpe=formatC(sharpe,format="f",digits=2)
        
        # calculate IRR
        xirr=xirr(cumpnl$cashflow,cumpnl$bizdays)*100
        xirr=formatC(xirr,format="f",digits=2)
        xirr=paste0(xirr,"%")
        
        print(paste("# Trades:",nrow(trades)))
        print(paste("Profit:",sum(trades$pnl)))
        print(paste("Return:",sum(trades$pnl)*100/(static$kTradeSize*static$core$kMaxPositions)*365/as.numeric((min(as.Date(static$core$kBackTestEndDate),Sys.Date())-as.Date(static$core$kBackTestStartDate)))))
        print(paste("sharpe:", sharpe, sep = ""))
        print(paste("xirr:", xirr,sep=""))
        print(paste("Win Ratio:",sum(trades$pnl>0)*100/nrow(trades)))
        print(paste("Avg % Profit Per Trade:",sum(trades$pnl)*100/nrow(trades)/static$kTradeSize))
        print(paste("Avg Holding Days:",sum(trades$bars)/nrow(trades)))
        print("************* Long Trades ****************")
        longtrades=trades[trades$trade=="BUY",]
        print(paste("# Long Trades:",nrow(longtrades)))
        print(paste("Profit:",sum(longtrades$pnl)))
        print(paste("Return:",sum(longtrades$pnl)*100/(static$kTradeSize*static$core$kMaxPositions)*365/as.numeric((min(as.Date(static$core$kBackTestEndDate),Sys.Date())-as.Date(static$core$kBackTestStartDate)))))
        print(paste("Win Ratio:",sum(longtrades$pnl>0)*100/nrow(longtrades)))
        print(paste("Avg % Profit Per Trade:",sum(longtrades$pnl)*100/nrow(longtrades)/static$kTradeSize))
        print(paste("Avg Holding Days:",sum(longtrades$bars)/nrow(longtrades)))
        print("************* Short Trades ****************")
        shorttrades=trades[trades$trade=="SHORT",]
        print(paste("# Short Trades:",nrow(shorttrades)))
        print(paste("Profit:",sum(shorttrades$pnl)))
        print(paste("Return:",sum(shorttrades$pnl)*100/(static$kTradeSize*static$core$kMaxPositions)*365/as.numeric((min(as.Date(static$core$kBackTestEndDate),Sys.Date())-as.Date(static$core$kBackTestStartDate)))))
        print(paste("Win Ratio:",sum(shorttrades$pnl>0)*100/nrow(shorttrades)))
        print(paste("Avg % Profit Per Trade:",sum(shorttrades$pnl)*100/nrow(shorttrades)/static$kTradeSize))
        print(paste("Avg Holding Days:",sum(shorttrades$bars)/nrow(shorttrades)))
}
#### EXECUTION SUMMARY ####
if(!static$core$kBackTest){
        generateExecutionSummary(optionTrades,unique(signals$date), static$core$kBackTestStartDate,static$core$kBackTestEndDate,static$core$kStrategy,static$core$kSubscribers,static$core$kBrokerage,static$core$kCommittedCapital,static$core$kMargin,kMarginOnUnrealized = FALSE,kInvestmentReturn=static$core$kInvestmentReturn,kOverdraftPenalty=static$core$kOverdraftPenalty,realtime=TRUE)
}

#### PRINT RUN TIME ####
timer.end=Sys.time()
runtime=timer.end-timer.start
print(runtime)