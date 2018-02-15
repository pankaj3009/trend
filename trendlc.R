library(RTrade)
library(zoo)
library(rredis)
library(log4r)
library(jsonlite)
options(scipen=999)

#### functions ####
longshortSignals<-function(s,realtime=FALSE,intraday=FALSE,type=NA_character_){
        print(paste("Processing: ",s,sep=""))
        md<-loadSymbol(s,realtime,type)
        if(!is.na(md)[1]){
                t<-Trend(md$date,md$ahigh,md$alow,md$settle)
                md<-merge(md,t,by="date")
                daysInUpTrend=RTrade::BarsSince(md$trend!=1)
                daysInDownTrend=RTrade::BarsSince(md$trend!=-1)
                daysIndeterminate=RTrade::BarsSince(md$trend!=0)
                daysinTrend=0
                trend=0
                trend<-ifelse(daysInUpTrend>daysInDownTrend,1,ifelse(daysInUpTrend<daysInDownTrend,-1,0))
                daysinTrend<-ifelse(trend==1,daysInUpTrend,ifelse(trend==-1,daysInDownTrend,daysIndeterminate))
                avgupswingindices<-which(diff(t$swinglevel)>0)
                avgupswing<-rollmean(diff(md$swinglevel)[avgupswingindices],kRollingSwingAverage)
                avgupswingindices<-avgupswingindices[(kRollingSwingAverage):length(avgupswingindices)]
                md$averageupswing<-NA_real_
                md$averageupswing[avgupswingindices]<-avgupswing
                md$averageupswing<-na.locf(md$averageupswing,na.rm = FALSE)
                md$averageupswing<-na.locf(md$averageupswing,fromLast = TRUE)
                
                avgdnswingindices<-which(diff(md$swinglevel)<0)
                avgdnswing<-rollmean(diff(md$swinglevel)[avgdnswingindices],kRollingSwingAverage)
                avgdnswingindices<-avgdnswingindices[(kRollingSwingAverage):length(avgdnswingindices)]
                md$averagednswing<-NA_real_
                md$averagednswing[avgdnswingindices]<- -1*avgdnswing
                md$averagednswing<-na.locf(md$averagednswing,na.rm = FALSE)
                md$averagednswing<-na.locf(md$averagednswing,fromLast = TRUE)
                adjswinghigh=ifelse(md$trend==-1 & md$updownbar==1,md$swinghighhigh_1,md$swinghigh)
                adjswinglow=ifelse(md$trend==1 & md$updownbar==-1,md$swinglowlow_1,md$swinglow)
                
                if(intraday){
                        uptrendsl =ifelse(md$trend==1,
                                          ifelse(md$updownbarclean==1 & md$updownbar==1,ifelse(Ref(md$updownbar,-1)==1,md$asettle-md$swinglowlow,md$asettle-md$swinglowlow_1),# handle scenario where we get an outside bar by eod whilst updownbarclean==1 during the day
                                                 ifelse(md$updownbarclean==-1 & md$updownbar==-1,md$asettle-md$swinglowlow_1,
                                                        ifelse(md$updownbar==1 & md$outsidebar==1,md$asettle-md$swinglowlow_1,
                                                               ifelse(md$updownbar==-1 & md$outsidebar==1,md$asettle-md$swinglowlow_1,
                                                                      ifelse(md$updownbar==1 & md$insidebar==1,md$asettle-md$swinglowlow,
                                                                             ifelse(md$updownbar==-1 & md$insidebar==1,md$asettle-md$swinglowlow_1,0)))))),0)
                        
                        dntrendsl =ifelse(md$trend==-1,
                                          ifelse(md$updownbarclean==1 & md$updownbar==1,md$swinghighhigh_1-md$asettle,
                                                 ifelse(md$updownbarclean==-1 & md$updownbar==-1,ifelse(Ref(md$updownbar,-1)==-1,md$swinghighhigh-md$asettle,md$swinghighhigh_1-md$asettle),
                                                        ifelse(md$updownbar==1 & md$outsidebar==1,md$swinghighhigh_1-md$asettle,
                                                               ifelse(md$updownbar==-1 & md$outsidebar==1,md$swinghighhigh_1-md$asettle,
                                                                      ifelse(md$updownbar==1 & md$insidebar==1,md$swinghighhigh_1-md$asettle,
                                                                             ifelse(md$updownbar==-1 & md$insidebar==1,md$swinghighhigh-md$asettle,0)))))),0)
                        
                }else{
                        uptrendsl =ifelse(md$trend==1,
                                          ifelse(md$updownbarclean==1 & md$updownbar==1,md$asettle-md$swinglowlow,
                                                 ifelse(md$updownbarclean==-1 & md$updownbar==-1,md$asettle-md$swinglowlow_1,
                                                        ifelse(md$updownbar==1 & md$outsidebar==1,md$asettle-md$swinglowlow_1,
                                                               ifelse(md$updownbar==-1 & md$outsidebar==1,md$asettle-md$swinglowlow_1,
                                                                      ifelse(md$updownbar==1 & md$insidebar==1,md$asettle-md$swinglowlow,
                                                                             ifelse(md$updownbar==-1 & md$insidebar==1,md$asettle-md$swinglowlow_1,0)))))),0)
                        
                        dntrendsl =ifelse(md$trend==-1,
                                          ifelse(md$updownbarclean==1 & md$updownbar==1,md$swinghighhigh_1-md$asettle,
                                                 ifelse(md$updownbarclean==-1 & md$updownbar==-1,md$swinghighhigh-md$asettle,
                                                        ifelse(md$updownbar==1 & md$outsidebar==1,md$swinghighhigh_1-md$asettle,
                                                               ifelse(md$updownbar==-1 & md$outsidebar==1,md$swinghighhigh_1-md$asettle,
                                                                      ifelse(md$updownbar==1 & md$insidebar==1,md$swinghighhigh_1-md$asettle,
                                                                             ifelse(md$updownbar==-1 & md$insidebar==1,md$swinghighhigh-md$asettle,0)))))),0)
                        
                }
                
                sl=uptrendsl+dntrendsl
                
                tp<-ifelse(trend==1,md$averageupswing-(md$asettle-md$swinglow),ifelse(trend==-1,md$averagednswing-(md$swinghigh-md$asettle),0))
                averagemove=ifelse(trend==1,md$averageupswing,ifelse(trend==-1,md$averagednswing,ifelse(md$updownbar==1,md$averageupswing,md$averagednswing)))
                
                hh=t$numberhh
                ll=t$numberll
                risk<-ifelse(tp!=0 & sl/tp>0,(sl/tp),1000000 )
                averagemoveperday=(md$movementsettle/daysinTrend)/md$asettle
                index=nrow(t)
                sl.level=0
                tp.level=0
                sl.level=ifelse(trend==1,md$asettle-sl, ifelse(trend==-1,md$asettle+sl,0))
                tp.level=ifelse(trend==1,md$asettle+specify_decimal(tp,2),ifelse(trend==-1,md$asettle-specify_decimal(tp,2),0))
                md$days.in.trend=daysinTrend
                md$sl = specify_decimal(sl,2)
                md$tp = specify_decimal(tp,2)
                md$trend.daily.pr.move = averagemoveperday*100
                md$risk = risk
                md$sl.level=sl.level
                md$tp.level=tp.level
                md[md$date>=kBackTestStartDate & md$date<=kBackTestEndDate,]
                # candidates<-md[md$date>backteststart & md$date<backtestend & md$risk>0 & md$trend!=0 & md$risk<1,]
                # candidates[complete.cases(candidates),]
        }
}

#### Parameters ####
args.commandline=commandArgs(trailingOnly=TRUE)
if(length(args.commandline)>0){
        args<-args.commandline
}
# args<-c("2","swing01","3")
# args[1] is a flag for model building. 0=> Build Model, 1=> Generate Signals in Production 2=> Backtest and BootStrap 4=>Save BOD Signals to Redis
# args[2] is the strategy name
# args[3] is the redisdatabase
redisConnect()
redisSelect(1)
if(length(args)>1){
        static<-redisHGetAll(toupper(args[2]))
}else{
        static<-redisHGetAll("TREND-LC")
}

newargs<-unlist(strsplit(static$args,","))
if(length(args)<=1 && length(newargs>1)){
        args<-newargs
}
redisClose()


kWriteToRedis <- as.logical(static$WriteToRedis)
kGetMarketData<-as.logical(static$GetMarketData)
kUseSystemDate<-as.logical(static$UseSystemDate)
kDataCutOffBefore<-static$DataCutOffBefore
kBackTestStartDate<-static$BackTestStartDate
kBackTestEndDate<-static$BackTestEndDate
kFNODataFolder <- static$FNODataFolder
kNiftyDataFolder <- static$CashDataFolder
kTimeZone <- static$TimeZone
kBrokerage<-as.numeric(static$SingleLegBrokerageAsPercentOfValue)/100
kPerContractBrokerage=as.numeric(static$SingleLegBrokerageAsValuePerContract)
kMaxContracts=as.numeric(static$MaxContracts)
kRollingSwingAverage=as.numeric(static$RollingSwingAverage)
kHomeDirectory=static$HomeDirectory
kLogFile=static$LogFile
setwd(kHomeDirectory)
strategyname = args[2]
redisDB = args[3]

logger <- create.logger()
logfile(logger) <- kLogFile
level(logger) <- 'INFO'

realtime=TRUE
intraday=FALSE
today=strftime(Sys.Date(),tz=kTimeZone,format="%Y-%m-%d")

#### Script ####

niftysymbols <- createIndexConstituents(2, "nifty50", threshold = strftime(Sys.Date() -  90))
#niftysymbols<-data.frame(symbol="YESBANK",startdate=as.Date("2017-01-01"),enddate=as.Date("2018-02-28"))
#niftysymbols <- createFNOConstituents(2, "contractsize", threshold = strftime(Sys.Date() -  90))
folots <- createFNOSize(2, "contractsize", threshold = strftime(Sys.Date() - 90))
symbols <- niftysymbols$symbol
options(scipen = 999)
today = strftime(Sys.Date(), tz = kTimeZone, format = "%Y-%m-%d")
alldata<-vector("list",length(symbols))
out <- data.frame()
signals<-data.frame()
allmd <- list()

trades<-data.frame(
        symbol=as.character(),
        trend = as.numeric(),
        days.in.trend = as.numeric(),
        exp.swing.move = as.numeric(),
        sl = as.numeric(),
        tp = as.numeric(),
        hh = as.numeric(),
        ll = as.numeric(),
        trend.hl.move = as.numeric(),
        trend.settle.move = as.numeric(),
        trend.daily.pr.move = as.numeric(),
        risk = as.numeric(),
        close = as.numeric(),
        sl.level=as.numeric(),
        tp.level=as.numeric(),
        change = as.numeric(),
        stringsAsFactors = FALSE
)
signals<-data.frame()
for(s in symbols){
        df=longshortSignals(s,realtime,intraday,"STK")
        if(nrow(signals)==0){
                signals<-df
        }else{
                signals<-rbind(signals,df)
        }
}
signals$buy<-ifelse(signals$trend==1 & signals$risk<0.5 & signals$trend.daily.pr.move>0 & signals$days.in.trend>1,1,0)
signals$short<-ifelse(signals$trend==-1 & signals$risk<0.5 & signals$trend.daily.pr.move<0 & signals$days.in.trend>1,1,0)
signals$sell<-ifelse(signals$trend!=1,1,0)
signals$cover<-ifelse(signals$trend!=-1,1,0)
signals$buyprice = signals$asettle
signals$sellprice = signals$asettle
signals$shortprice = signals$asettle
signals$coverprice = signals$asettle
signals$positionscore<-100-signals$risk
signals[is.na(signals)]<-0
signals$aclose <- signals$asettle
dates <- unique(signals[order(signals$date), c("date")])
signals$inlongtrade=ContinuingLong(signals$symbol,signals$buy,signals$sell,signals$short)
signals$inshorttrade=ContinuingShort(signals$symbol,signals$short,signals$cover,signals$buy)


if((args[1]==2) & file.exists("signals.rds") & file.exists("trades.rds")){
        # Replace signals data where signals$date >= earliest opening trade
        signals.old<-readRDS("signals.rds")
        trades.old<-readRDS("trades.rds")
        opentrades.date=trades.old[which(trades.old$exitreason==""),c("entrytime")]
        if(length(opentrades.date)>0){
                opentrades.date<-sort(opentrades.date)
                cutoffdate<-opentrades.date[1]
                signals.1.keep<-signals[signals$date<cutoffdate|as.Date(signals$date)==Sys.Date(),]
                signals.2.keep<-signals.old[signals.old$date>=cutoffdate,]
                keep<-names(signals.2.keep)
                signals.1.keep<-signals.1.keep[,names(signals.1.keep)%in%keep]
                signals<-rbind(signals.1.keep,signals.2.keep)
        }
}

if(args[1]==2){
        saveRDS(signals,"signals.rds")
}

processedsignals<- ApplySLTP(signals,signals$sl,signals$tp)
existingcol<-names(processedsignals)
processedsignals<-cbind(processedsignals,signals[,!names(signals)%in%existingcol])
processedsignals <- processedsignals[order(processedsignals$date), ]

a <- ProcessPositionScore(processedsignals, 5, dates)
a$currentmonthexpiry <- as.Date(sapply(a$date, getExpiryDate), tz = kTimeZone)
nextexpiry <- as.Date(sapply(
        as.Date(a$currentmonthexpiry + 20, tz = kTimeZone),
        getExpiryDate), tz = kTimeZone)
a$entrycontractexpiry <- as.Date(ifelse(
        businessDaysBetween("India",as.Date(a$date, tz = kTimeZone),a$currentmonthexpiry) <= 1,
        nextexpiry,a$currentmonthexpiry),tz = kTimeZone)

a<-getClosestStrikeUniverse(a,kFNODataFolder,kNiftyDataFolder,kTimeZone)

multisymbol<-function(uniquesymbols,df,fnodatafolder,equitydatafolder){
        out=NULL
        for(i in 1:length(unique(df$symbol))){
                temp<-futureTradeSignals(df[df$symbol==unique(df$symbol)[i],],fnodatafolder,equitydatafolder,rollover=TRUE)
                out<-rbind(out,temp)
        }
        out
}

futureSignals<-multisymbol(unique(a$symbol),a,kFNODataFolder,kNiftyDataFolder)
futureSignals<-futureSignals[with(futureSignals,order(date,symbol,buy,sell)),]
trades <- GenerateTrades(futureSignals)
trades <- trades[order(trades$entrytime), ]
trades$cashsymbol<-sapply(strsplit(trades$symbol,"_"),"[",1)
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

trades$size=NULL
novalue=strptime(NA_character_,"%Y-%m-%d")
for(i in 1:nrow(trades)){
        symbolsvector=unlist(strsplit(trades$symbol[i],"_"))
        allsize = folots[folots$symbol == symbolsvector[1], ]
        trades$size[i]=getcontractsize(trades$entrytime[i],allsize)
        if(as.numeric(trades$exittime[i])==0){
                trades$exittime[i]=novalue
        }
}

trades$brokerage <- ifelse(trades$exitprice==0|trades$entryprice==0,0,(kPerContractBrokerage*2) / (trades$size*trades$entryprice))
trades$percentprofit<-ifelse(trades$exitprice==0|trades$entryprice==0,0,trades$percentprofit)
trades$netpercentprofit <- trades$percentprofit - trades$brokerage
trades$pnl<-ifelse(trades$exitprice==0|trades$entryprice==0,0,trades$entryprice*trades$netpercentprofit*trades$size)

### add sl and tp levels to trade
trades.plus.signals<-merge(trades,signals,by.x=c("entrytime","cashsymbol"),by.y=c("date","symbol"))
shortlisted.columns<-c("symbol","trade","entrytime","entryprice","exittime","exitprice","exitreason","percentprofit",
                       "bars","size","brokerage","netpercentprofit","pnl","sl.level","tp.level")
trades<-trades.plus.signals[,shortlisted.columns]

#### Write to Redis ####
if(args[1]==2 && kWriteToRedis){
        entrysize = 0
        exitsize = 0
        if (length(which(as.Date(trades$entrytime,tz=kTimeZone) == Sys.Date())) == 1) {
                entrysize = trades[as.Date(trades$entrytime,tz=kTimeZone) == Sys.Date(), c("size")][1]
        }
        if (length(which(as.Date(trades$exittime,tz=kTimeZone) == Sys.Date())) >= 1) {
                exittime=which(as.Date(trades$exittime,tz=kTimeZone) == Sys.Date())
                exitsize = sum(trades[exittime, c("size")])
        }
        
        
        # order=data.frame(
        #         ParentDisplayName=as.character(),
        #         ChildDisplayName=as.character(),
        #         OrderSide=as.character(),
        #         OrderReason=as.character(),
        #         OrderType=as.character(),
        #         OrderStage=as.character(),
        #         StrategyOrderSize=as.character(),
        #         StrategyStartingPosition=as.character(),
        #         MaximumOrderValue=as.character(),
        #         DisplaySize=as.character(),
        #         TriggerPrice=as.character(),
        #         StopLoss=as.character(),
        #         TakeProfit=as.character(),
        #         Scale=as.character(),
        #         OrderReference=as.character(),
        #         EffectiveFrom=as.character(),
        #         EffectiveTill=as.character(),
        #         stringsAsFactors = FALSE
        # )
        #Exit First, then enter
        #Write Exit to Redis
        
        if (exitsize > 0 & kWriteToRedis) {
                redisConnect()
                redisSelect(args[3])
                out <- trades[which(as.Date(trades$exittime,tz=kTimeZone) == Sys.Date()-2),]
                for (o in 1:nrow(out)) {
                        endingposition=GetCurrentPosition(out[o, "symbol"], trades)
                        change = 0
                        side = "UNDEFINED"
                        if(grepl("BUY",out[o,"trade"])){
                                change=-out[o,"size"]
                                side="SELL"
                        }else{
                                change=out[o,"size"]
                                side="COVER"
                        }
                        startingposition = endingposition - change
                        order=data.frame(
                                ParentDisplayName=out[o,"symbol"],
                                ChildDisplayName=out[o,"symbol"],
                                OrderSide=out[o,"trade"],
                                OrderReason="REGULAREXIT",
                                OrderType="CUSTOMREL",
                                OrderStage="INIT",
                                StrategyOrderSize=out[o,"size"],
                                StrategyStartingPosition=as.character(abs(startingposition)),
                                DisplaySize="1",
                                TriggerPrice="0",
                                Scale="TRUE",
                                OrderReference="TRENDLC",
                                stringsAsFactors = FALSE
                        )
                        redisString=toJSON(order,dataframe = c("columns"), simplifyVector=TRUE)
                        redisRPush(paste("trades", args[2], sep = ":"),
                                   charToRaw(redisString))
                        levellog(logger,
                                 "INFO",
                                 paste(args[2], redisString, sep = ":"))
                        
                }
                redisClose()
        }
        
        if (entrysize > 0 & kWriteToRedis) {
                redisConnect()
                redisSelect(args[3])
                out <- trades[which(as.Date(trades$entrytime,tz=kTimeZone) == Sys.Date()),]
                for (o in 1:nrow(out)) {
                        endingposition=GetCurrentPosition(out[o, "symbol"], trades)
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
                                DisplaySize="1",
                                TriggerPrice="0",
                                StopLoss=out[o,"sl.level"],
                                TakeProfit=out[o,"tp.level"],
                                Scale="TRUE",
                                OrderReference="TRENDLC",
                                stringsAsFactors = FALSE
                        )
                        redisString=toJSON(order,dataframe = c("columns"), simplifyVector=TRUE)
                        redisRPush(paste("trades", args[2], sep = ":"),
                                   charToRaw(redisString))
                        levellog(logger,
                                 "INFO",
                                 paste(args[2], redisString, sep = ":"))
                        
                }
                redisClose()
        }
        
        saveRDS(trades,"trades.rds")
        
}

if(args[1]==1 & kWriteToRedis){
        # write sl and tp levels on BOD
        # first check BOD has not started
        md<-loadSymbol("NSENIFTY",TRUE,"IND")
        if(length(which(as.Date(md$date,tz="Asia/Kolkata")==Sys.Date()))==0){ # no data for today
                # update strategy
                strategyTrades<-createPNLSummary(args[3],args[2],kBackTestStartDate,kBackTestEndDate,mdpath=kFNODataFolder)
                opentrades.index<-which(is.na(strategyTrades$exittime))
                if(length(opentrades.index)>0){
                        opentrades.index<-sort(opentrades.index)
                        for(i in 1:length(opentrades.index)){
                                ind<-opentrades.index[i]
                                symbol<-strsplit(strategyTrades[ind,c("symbol")],"_")[[1]][1]
                                signals.symbol<-signals[signals$symbol==symbol,]
                                signals.symbol<-signals.symbol[order(signals$date),]
                                df<-signals.symbol[nrow(signals.symbol),]
                                trade.sl<-df$sl.level
                                if(length(trade.sl)>0){
                                        rredis::redisHSet(strategyTrades[ind,c("key")],"sl",charToRaw(as.character(trade.sl)))
                                }
                        }
                }
                # update execution
                strategyTrades<-createPNLSummary(0,args[2],kBackTestStartDate,kBackTestEndDate,mdpath=kFNODataFolder)
                opentrades.index<-which(is.na(strategyTrades$exittime))
                if(length(opentrades.index)>0){
                        opentrades.index<-sort(opentrades.index)
                        for(i in 1:length(opentrades.index)){
                                ind<-opentrades.index[i]
                                symbol<-strsplit(strategyTrades[ind,c("symbol")],"_")[[1]][1]
                                signals.symbol<-signals[signals$symbol==symbol,]
                                signals.symbol<-signals.symbol[order(signals$date),]
                                df<-signals.symbol[nrow(signals.symbol),]
                                trade.sl<-df$sl.level
                                if(length(trade.sl)>0){
                                        rredis::redisHSet(strategyTrades[ind,c("key")],"sl",charToRaw(as.character(trade.sl)))
                                }
                        }
                }
        }
}
        
        #### Print to folder ####
        #trades <- GenerateTrades(a)
        print(paste("Profit:",sum(trades$percentprofit)))
        print(paste("Win Ratio:",sum(trades$percentprofit>0)/nrow(trades)))
        print(paste("# Trades:",nrow(trades)))
        print(trades[trades$exitreason=="",])
        filename=paste(strftime(Sys.time(),"%Y%m%d %H:%M:%S"),"trades.csv",sep="_")
        write.csv(trades,file=filename)
        filename=paste(strftime(Sys.time(),"%Y%m%d %H:%M:%S"),"signals.csv",sep="_")
        write.csv(a,file=filename)
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        