library(RTrade)
library(zoo)
library(rredis)
library(log4r)
library(jsonlite)
library(TTR)
options(scipen=999)

#### functions ####
longshortSignals<-function(s,realtime=FALSE,intraday=FALSE,type=NA_character_){
        print(paste("Processing: ",s,sep=""))
        md<-loadSymbol(s,realtime,type)
        md=md[md$date<=kBackTestEndDate,]
        if(!is.na(md) && args[1]==4){
                names.md<-names(md)
                md.part.1<-md[md$date<md.cutoff|as.Date(md$date,tz="Asia/Kolkata")==Sys.Date(),]
                md.part.2<-signals.yesterday[signals.yesterday$symbol==s & signals.yesterday$date>=md.cutoff & as.Date(signals.yesterday$date,tz="Asia/Kolkata")<Sys.Date(),names.md]
                md<-rbind(md.part.1,md.part.2)
                md<-md[order(md$date),]
        }
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
                
                if(volatile){
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
                }else{
                        avgupswingindices<-which(diff(t$swinglevel)>0)
                        upswingvalues<- diff(md$swinglevel)[avgupswingindices]
                        upswingvalues<-upswingvalues[-length(upswingvalues)]
                        avgupswingindices<-avgupswingindices+1
                        avgupswingindices<-avgupswingindices[-1]
                        avgupswing<-rollmean(upswingvalues,(kRollingSwingAverage-0))
                        avgupswingindices<-avgupswingindices[(kRollingSwingAverage-0):length(avgupswingindices)]
                        md$averageupswing<-NA_real_
                        md$averageupswing[avgupswingindices]<-avgupswing
                        md$averageupswing<-na.locf(md$averageupswing,na.rm = FALSE)
                        md$averageupswing<-na.locf(md$averageupswing,fromLast = TRUE)
                        md$averageupswing_1=md$averagednswing
                        # md$averageupswing<- (md$averageupswing*(kRollingSwingAverage-1)+(md$swinghigh-md$swinglowlow))/3
                        
                        avgdnswingindices<-which(diff(md$swinglevel)<0)
                        dnswingvalues<--1 * diff(md$swinglevel)[avgdnswingindices]
                        dnswingvalues<-dnswingvalues[-length(dnswingvalues)]
                        avgdnswingindices<-avgdnswingindices+1 # this moves the index to the row that has experienced the downswing
                        avgdnswingindices<-avgdnswingindices[-1]
                        #avgdnswingindices<-avgdnswingindices[1:(length(avgdnswingindices)-1)]
                        avgdnswing<-rollmean(dnswingvalues,(kRollingSwingAverage-0))
                        avgdnswingindices<-avgdnswingindices[(kRollingSwingAverage-0):length(avgdnswingindices)]
                        md$averagednswing<-NA_real_
                        md$averagednswing[avgdnswingindices]<- avgdnswing
                        md$averagednswing<-na.locf(md$averagednswing,na.rm = FALSE)
                        md$averagednswing<-na.locf(md$averagednswing,fromLast = TRUE) # handles leading NAs
                        md$averagednswing_1<-md$averagednswing
                        # md$averagednswing<- (md$averagednswing*(kRollingSwingAverage-1)+(md$swinghighhigh-md$swinglow))/3
                }
                

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
                        uptrendsl =ifelse(md$trend==1,
                                             ifelse(md$swinghigh>md$swinghighhigh_1 & md$updownbar==1,md$asettle-md$swinglow,
                                                    ifelse(md$swinghigh>md$swinghighhigh_1 & md$updownbar==-1,md$asettle-md$swinglowlow_1,
                                                           ifelse(md$updownbar==-1,md$asettle-md$swinglow,md$asettle-md$alow))),0)


                        dntrendsl =ifelse(md$trend==-1,
                                             ifelse(md$swinglow<md$swinglowlow_1 & md$updownbar==-1,md$swinghigh-md$asettle,
                                                    ifelse(md$swinglow<md$swinglowlow_1 & md$updownbar==1,md$swinghighhigh_1-md$asettle,
                                                           ifelse(md$updownbar==1,md$swinghigh-md$asettle,md$ahigh-md$asettle))),0)

                        
                }
                
                sl=uptrendsl+dntrendsl
                atr<-ATR(md[,c("ahigh","alow","asettle")],n=3)
                md$tr<-atr[,"tr"]
                md$atr<-atr[,"atr"]
                tp<-ifelse(trend==1,md$averageupswing-(md$asettle-md$swinglow),ifelse(trend==-1,md$averagednswing-(md$swinghigh-md$asettle),0))
               # tp<-md$atr*5
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
                md=md[md$date>=kBackTestStartDate & md$date<=kBackTestEndDate,]
                md$eligible<-1
                # candidates<-md[md$date>backteststart & md$date<backtestend & md$risk>0 & md$trend!=0 & md$risk<1,]
                # candidates[complete.cases(candidates),]
                # md$sl.level<-Ref(md$sl.level,-1)
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
bod<-paste(today, "09:08:00 IST",sep="")
eod<-paste(today, "15:30:00 IST",sep="")
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

kWriteToRedis <- as.logical(static$WriteToRedis)
kGetMarketData<-as.logical(static$GetMarketData)
kUseSystemDate<-as.logical(static$UseSystemDate)
kDataCutOffBefore<-static$DataCutOffBefore
kBackTestStartDate<-static$BackTestStartDate
kBackTestEndDate<-static$BackTestEndDate
# kBackTestStartDate<-"2017-01-01"
# kBackTestEndDate<-"2017-12-31"
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
volatile=TRUE
intraday=FALSE
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

niftysymbols <- createIndexConstituents(2, "nifty50", threshold = strftime(as.Date(kBackTestStartDate) -  90))
#niftysymbols <- createFNOConstituents(2, "contractsize", threshold = strftime(as.Date(kBackTestStartDate) -  90))

niftysymbols<-niftysymbols[niftysymbols$startdate<=as.Date(kBackTestEndDate,tz=kTimeZone) & niftysymbols$enddate>=as.Date(kBackTestStartDate,tz=kTimeZone) ,]
for(i in 1:nrow(niftysymbols)){
        niftysymbols$symbol.latest[i]<-getMostRecentSymbol(niftysymbols$symbol[i],symbolchange$key,symbolchange$newsymbol)
}

folots <- createFNOSize(2, "contractsize", threshold = strftime(as.Date(kBackTestStartDate) -  90))
symbols <- niftysymbols$symbol
#symbols<-c("ICICIBANK")
options(scipen = 999)
today = strftime(Sys.Date(), tz = kTimeZone, format = "%Y-%m-%d")
alldata<-vector("list",length(symbols))
out <- data.frame()
signals<-data.frame()
allmd <- list()

signals<-data.frame()
for(i in 1:length(symbols)){
        df=longshortSignals(symbols[i],realtime,intraday,"STK")
        df$eligible = ifelse(as.Date(df$date) >= niftysymbols[i, c("startdate")] & as.Date(df$date) <= niftysymbols[i, c("enddate")],1,0)
        df$symbol<-niftysymbols$symbol[grepl(paste("^",symbols[i],"$",sep=""),niftysymbols$symbol)]
        if(nrow(signals)==0){
                signals<-df
        }else{
                signals<-rbind(signals,df)
        }
        
}
signals$buy<-ifelse(signals$eligible==1 & signals$trend==1 & signals$risk<0.5 & signals$trend.daily.pr.move>0 & signals$days.in.trend>1,1,0)
signals$short<-ifelse(signals$eligible==1  & signals$trend==-1 & signals$risk<0.5 & signals$trend.daily.pr.move<0 & signals$days.in.trend>1,1,0)
#signals$buy<-ifelse(signals$eligible==1 & signals$trend==1 & signals$risk<0.5  & signals$aopen<signals$asettle & signals$days.in.trend>1,1,0)
#signals$short<-ifelse(signals$eligible==1  & signals$trend==-1 & signals$risk<0.5 & signals$aopen>signals$asettle & signals$days.in.trend>1,1,0)
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
signals1<-signals
signals<-signals[order(signals$date,signals$symbol),]


if(args[1]==2){
        saveRDS(signals,paste("signals","_",strftime(Sys.time(),"%Y%m%d %H:%M:%S"),".rds",sep=""))
}

a<-ProcessSignals(signals,signals$sl.level,signals$tp.level,volatilesl = TRUE,volatiletp = TRUE,maxposition=5,debug=FALSE)
# symbol might have changed. update to changed symbol
x<-sapply(a$symbol,grep,symbolchange$key)
potentialnames<-names(x)
index.symbolchange<-match(a$symbol,symbolchange$key,nomatch = 1)
a$symbol<-ifelse(index.symbolchange>1 & a$date>=symbolchange$date[index.symbolchange],potentialnames,a$symbol)
a$currentmonthexpiry <- as.Date(sapply(a$date, getExpiryDate), tz = kTimeZone)
nextexpiry <- as.Date(sapply(
        as.Date(a$currentmonthexpiry + 20, tz = kTimeZone),
        getExpiryDate), tz = kTimeZone)
a$entrycontractexpiry <- as.Date(ifelse(
        businessDaysBetween("India",as.Date(a$date, tz = kTimeZone),a$currentmonthexpiry) < 1,
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

trades$entrysize=NULL
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
                       "bars","size","brokerage","netpercentprofit","pnl","sl.level","tp.level","splitadjust")
trades<-trades.plus.signals[,shortlisted.columns]
names(trades)[names(trades) == 'splitadjust'] <- 'entry.splitadjust'
trades$cashsymbol<-sapply(strsplit(trades$symbol,"_"),"[",1)
trades.plus.signals<-merge(trades,signals[,!names(signals)%in%c("sl.level","tp.level")],by.x=c("exittime","cashsymbol"),by.y=c("date","symbol"),all.x = TRUE)
shortlisted.columns<-c("symbol","trade","entrytime","entryprice","exittime","exitprice","exitreason","percentprofit",
                       "bars","size","brokerage","netpercentprofit","pnl","sl.level","tp.level","entry.splitadjust","splitadjust")
trades<-trades.plus.signals[,shortlisted.columns]
names(trades)[names(trades) == 'splitadjust'] <- 'exit.splitadjust'
trades$exit.splitadjust<-ifelse(is.na(trades$exit.splitadjust),1,trades$exit.splitadjust)

# Adjust exit price for any splits during trade
trades$exitprice=trades$exitprice*trades$entry.splitadjust/trades$exit.splitadjust
trades$percentprofit=ifelse(grepl("SHORT",trades$trade),specify_decimal((trades$entryprice-trades$exitprice)/(trades$entryprice),2),specify_decimal((trades$exitprice-trades$entryprice)/(trades$entryprice),2))
trades$brokerage <- ifelse(trades$exitprice==0|trades$entryprice==0,0,(kPerContractBrokerage*2) / (trades$size*trades$entryprice))
trades$percentprofit<-ifelse(trades$exitprice==0|trades$entryprice==0,0,trades$percentprofit)
trades$netpercentprofit <- trades$percentprofit - trades$brokerage
trades$pnl<-ifelse(trades$exitprice==0|trades$entryprice==0,0,trades$entryprice*trades$netpercentprofit*trades$size)


#### Write to Redis ####
if(args[1]==2 && kWriteToRedis){
        levellog(logger, "INFO", paste("Starting scan for writing to Redis for ",args[2], sep = ""))
        entrysize = 0
        exitsize = 0
        if (length(which(as.Date(trades$entrytime,tz=kTimeZone) == Sys.Date())) >= 1) {
                entrytime=which(as.Date(trades$entrytime,tz=kTimeZone) == Sys.Date())
                entrysize = sum(trades[entrytime, c("size")])
        }
        if (length(which(as.Date(trades$exittime,tz=kTimeZone) == Sys.Date())) >= 1) {
                exittime=which(as.Date(trades$exittime,tz=kTimeZone) == Sys.Date())
                exitsize = sum(trades[exittime, c("size")])
        }
        
        #Exit First, then enter
        #Write Exit to Redis
        
        if (exitsize > 0 & kWriteToRedis) {
                redisConnect()
                redisSelect(args[3])
                exitindices<-which(as.Date(trades$exittime,tz=kTimeZone) == Sys.Date())
                out <- trades[exitindices,]
                for (o in 1:nrow(out)) {
                        change = 0
                        side = "UNDEFINED"
                        # calculate starting positions by excluding trades already considered in this & prior iterations. 
                        # Effectively, the abs(startingposition) should keep reducing for duplicate symbols.
                        startingpositionexcluding.this=GetCurrentPosition(out[o, "symbol"], trades[-exitindices[1:o],],trades.till = Sys.Date()-1,position.on = Sys.Date()-1)
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
                                DisplaySize=out[o,"size"],
                                TriggerPrice="0",
                                StopLoss=out[o,"sl.level"],
                                TakeProfit=out[o,"tp.level"],
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
        saveRDS(trades,paste("trades","_",strftime(Sys.time(),"%Y%m%d %H:%M:%S"),".rds",sep=""))
        
}

if(args[1]==1 & kWriteToRedis){
        # write sl and tp levels on BOD
        # update strategy
        levellog(logger, "INFO", paste("Starting scan for sl and tp update for ",args[2], sep = ""))
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
                        trade.sl<-df$sl.level
                        trade.tp<-df$tp.level
                        if(length(trade.sl)>0){
                                rredis::redisHSet(strategyTrades[ind,c("key")],"StopLoss",charToRaw(as.character(trade.sl)))
                                rredis::redisHSet(strategyTrades[ind,c("key")],"TakeProfit",charToRaw(as.character(trade.tp)))
                        }
                }
        }
        # update execution
        strategyTrades<-createPNLSummary(0,args[2],kBackTestStartDate,kBackTestEndDate,mdpath=kFNODataFolder,deriv=TRUE)
        opentrades.index<-which(is.na(strategyTrades$exittime))
        if(length(opentrades.index)>0){
                opentrades.index<-sort(opentrades.index)
                for(i in 1:length(opentrades.index)){
                        ind<-opentrades.index[i]
                        symbol<-strsplit(strategyTrades[ind,c("symbol")],"_")[[1]][1]
                        processedsignals.symbol<-processedsignals[processedsignals$symbol==symbol,]
                        processedsignals.symbol<-processedsignals.symbol[order(processedsignals.symbol$date),]
                        df<-processedsignals.symbol[nrow(processedsignals.symbol),]
                        trade.sl<-df$sl.level
                        trade.tp<-df$tp.level
                        if(length(trade.sl)>0){
                                if(trade.sl>0){
                                        rredis::redisHSet(strategyTrades[ind,c("key")],"StopLoss",charToRaw(as.character(trade.sl)))        
                                }else{
                                        signals.symbol<-signals[signals$symbol==symbol,]
                                        signals.symbol<-signals.symbol[order(signals.symbol$date),]
                                        df<-signals.symbol[nrow(signals.symbol),]
                                        trade.sl<-df$sl.level
                                        if(length(trade.sl)>0){
                                                rredis::redisHSet(strategyTrades[ind,c("key")],"StopLoss",charToRaw(as.character(trade.sl)))        
                                        }
                                }
                        }
                        if(length(trade.tp)>0){
                                if(trade.tp>0){
                                        rredis::redisHSet(strategyTrades[ind,c("key")],"TakeProfit",charToRaw(as.character(trade.tp)))
                                }else{
                                        signals.symbol<-signals[signals$symbol==symbol,]
                                        signals.symbol<-signals.symbol[order(signals.symbol$date),]
                                        df<-signals.symbol[nrow(signals.symbol),]
                                        trade.tp<-df$tp.level
                                        if(length(trade.tp)>0){
                                                rredis::redisHSet(strategyTrades[ind,c("key")],"TakeProfit",charToRaw(as.character(trade.tp)))        
                                        }
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
#write.csv(trades,file=filename)
filename=paste(strftime(Sys.time(),"%Y%m%d %H:%M:%S"),"signals.csv",sep="_")
#write.csv(a,file=filename)