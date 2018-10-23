# Trades in index based on trend breakout.

timer.start=Sys.time()
library(RTrade)
library(log4r)
library(tableHTML)
library(gmailr)
options(scipen=999)

#### PARAMETERS ####
args.commandline = commandArgs(trailingOnly = TRUE)
if (length(args.commandline) > 0) {
        args <- args.commandline
}

### Read Parameters ###
if (length(args) > 1) {
        static <- readRDS(paste(tolower(args[2]),".rds",sep=""))
} else{
        static <- readRDS("indexid01.rds")
        args<-c(1,tolower(static$core$kStrategy))
}

static$core$kBackTestEndDate = strftime(adjust("India", as.Date(static$core$kBackTestEndDate, tz = kTimeZone), bdc = 2), "%Y-%m-%d")
static$core$kBackTestStartDate = strftime(adjust("India", as.Date(static$core$kBackTestStartDate, tz = kTimeZone), bdc = 0), "%Y-%m-%d")

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
INDEXID01generateSignals<-function(longname,realtime,atrperiod,days){
        shortname=strsplit(longname,"_")[[1]][1]
        type=strsplit(longname,"_")[[1]][2]
        md.intraday=loadSymbol(longname,days=days,src="persecond",dest="5minutes",realtime=TRUE)
        pattern=RTrade::candleStickPattern(md.intraday)$pattern
        pattern.complete=pattern[complete.cases(pattern),]
        pattern.complete=pattern.complete[order(pattern.complete$confirmationdate,-pattern.complete$duration),]
        pattern.complete=pattern.complete[!duplicated(pattern.complete$confirmationdate),]
        pattern.complete<-pattern.complete[,c("pattern","confirmationdate","stoploss","duration")]
        names(pattern.complete)[1]="pattern.complete"
        pattern.complete=unique(pattern.complete)
        colnames(pattern.complete)[]
        colnames(pattern.complete)[which(names(pattern.complete)=="stoploss")]="candlesl"
        
        md.daily=loadSymbol(longname,realtime)
        t=Trend(md.daily$date,md.daily$ahigh,md.daily$alow,md.daily$asettle)
        trend=ifelse(t$trend!=0,t$trend,ifelse(BarsSince(t$trend==1)>BarsSince(t$trend==-1),1,-1))
        md.daily$side=ifelse(trend==1,"BUY","SHORT")
        
        t.intraday=Trend(md.intraday$date,md.intraday$high,md.intraday$low,md.intraday$close)
        t.intraday$trend.intraday=ifelse(t.intraday$trend!=0,t.intraday$trend,ifelse(BarsSince(t.intraday$trend==1)>BarsSince(t.intraday$trend==-1),1,-1))
        # t.intraday$trend=t.intraday$trend
        md.intraday=merge(md.intraday,t.intraday[,c("date","trend.intraday")],by=c("date"),all.x=TRUE)
        md.daily$nextday=RQuantLib::advance(calendar = "India",dates=as.Date(strftime(md.daily$date,"%Y-%m-%d")),n=1,timeUnit = 0,bdc=0,emr=FALSE) 
        #bizdays::add.bizdays(md.daily$date,1)
        md.daily$nextday=as.POSIXct(strptime(as.character(md.daily$nextday),format="%Y-%m-%d",tz="Asia/Kolkata"))
        md.daily$nextday=md.daily$nextday+9*60*60+15*60
        md=merge(md.intraday,md.daily[,c("nextday","side")],by.x=c("date"),by.y=c("nextday"),all.x = TRUE)
        md$side=na.locf(md$side,na.rm=FALSE)
        atr<-ATR(md[,c("high","low","close")],n=21)
        md$atr<-atrperiod*atr[,"atr"]
        md<-na.omit(md)
        md1=generateSignalsBoundByATR(md)
        md=merge(md,md1[,c("date","trade","stoploss")],by.x=c("date"),by.y=c("date"),all.x=TRUE)
        md<-merge(md,pattern.complete,by.x=c("date"),by.y=c("confirmationdate"),all.x = TRUE)
        #md$buy=md$trade==1 & md$side=="BUY" & Ref(md$trade,-1)!=md$trade
        #md$short=md$trade==-1 & md$side=="SHORT" & Ref(md$trade,-1)!=md$trade
        md$buy1=(md$side=="BUY" & md$trend.intraday==1 & Ref(md$trend.intraday,-1)!=md$trend.intraday & md$trade==1)
        md$buy2=(grepl("BULLISH",md$pattern.complete) & md$trade==1)
        md$buy=md$buy1
        md$short1=(md$side=="SHORT" & md$trend.intraday==-1 & Ref(md$trend.intraday,-1)!=md$trend.intraday & md$trade==-1)
        md$short2=(grepl("BEARISH",md$pattern.complete) & md$trade==-1)
        md$short=md$short1
        # md$buy=md$trend.intraday==1 & Ref(md$trend.intraday,-1)!=md$trend.intraday
        # md$short=md$trend.intraday==-1 & Ref(md$trend.intraday,-1)!=md$trend.intraday
        md$sell=md$trade==-1 & Ref(md$trade,-1)==1 
        md$cover=md$trade==1 & Ref(md$trade,-1)==-1
        md$buyprice=md$close
        md$sellprice=md$close
        md$shortprice=md$close
        md$coverprice=md$close
        #        md=na.omit(md)
        md$buy=as.numeric(md$buy)
        md$sell=as.numeric(md$sell)
        md$short=as.numeric(md$short)
        md$cover=as.numeric(md$cover)
        #        colnames(md)[which(names(md)=="open")]="aopen"
        md$tp=ifelse(md$buy,md$close*1.01,ifelse(md$short,md$close*0.99,0))
        md$positionscore=100
        md
}
#### GENERATE SYMBOLS ####
symbols=c("NSENIFTY_IND___")
#### GENERATE SIGNALS ####
signalsBacktest=data.frame()
days=7
if(args[1]!=2){
        days=as.numeric(Sys.Date()-as.Date(static$core$kBackTestStartDate))
}
for(i in 1:length(symbols)){
        signalsBacktest=INDEXID01generateSignals(symbols[i],static$core$kRealTime,4,days=days)
}
signalsBacktest$shortname=sapply(strsplit(signalsBacktest$symbol,"_"),"[",1)
#### GENERATE TRADES ####
tradesBacktest=ProcessSignals(signalsBacktest,rep(0,nrow(signalsBacktest)),signalsBacktest$tp,rep(365,nrow(signalsBacktest)),1,debug = FALSE)
#### MAP TO DERIVATIVES ####
futureTrades=MapToFutureTrades(tradesBacktest,rollover=TRUE,tz=static$core$kTimeZone)
optionTrades=MapToOptionTradesLO(tradesBacktest,rollover=FALSE,tz=static$core$kTimeZone)
optionTrades<-merge(optionTrades,signalsBacktest[,c("date","shortname","tp")],by.x=c("entrytime","shortname"),by.y=c("date","shortname"),all.x = TRUE)
optionTrades$entry.splitadjust=1
optionTrades$exit.splitadjust=1
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
folots <- createFNOSize(2, "contractsize", threshold = strftime(as.Date(static$core$kBackTestStartDate) -  180))
for(i in seq_len(nrow(optionTrades))){
        symbolsvector=unlist(strsplit(optionTrades$symbol[i],"_"))
        allsize = folots[folots$symbol == symbolsvector[1], ]
        optionTrades$size[i]=getcontractsize(optionTrades$entrytime[i],allsize)
        if(as.numeric(optionTrades$exittime[i])==0){
                optionTrades$exittime[i]=novalue
        }
}        
optionTrades$size=optionTrades$size*static$kContractNum

#### WRITE TO REDIS ####
bartime=as.POSIXlt(Sys.time())
mod=bartime$min %% 5
bartime$min=bartime$min-mod-5
bartime$sec=0
bartime=as.POSIXct(bartime)
bartime=round(bartime,"mins")
last=which(optionTrades$entrytime==bartime)
if(static$core$kWriteToRedis && (length(last)==1||(length(which(optionTrades$exittime == bartime & optionTrades$exitreason!="Open"))==1))){
        order=data.frame( OrderType="CUSTOMREL",
                          OrderStage="INIT",
                          TriggerPrice="0",
                          Scale="FALSE",
                          OrderReference=tolower(static$core$kStrategy),
                          stringsAsFactors = FALSE)
        optionTrades$sl=ifelse(grepl("PUT",optionTrades$symbol),optionTrades$tp,0)
        optionTrades$tp=ifelse(grepl("CALL",optionTrades$symbol),optionTrades$tp,0)
        placeRedisOrder(optionTrades,bartime,order,args[3],map=FALSE)
        saveRDS(optionTrades,paste("trades","_",args[2],"_",strftime(Sys.time(),"%Y-%m-%d %H-%M-%S"),".rds",sep=""))
        
}
#### EXECUTION SUMMARY ####
if((!static$core$kBackTest && (length(last)==1||length(which(optionTrades$exittime == bartime & optionTrades$exitreason!="Open"))==1))||args[1]!=2){
        generateExecutionSummary(optionTrades,unique(as.POSIXct(strftime(signalsBacktest$date,format="%Y-%m-%d"))),static$core$kBackTestStartDate,static$core$kBackTestEndDate,static$core$kStrategy,static$core$kSubscribers,static$core$kBrokerage,static$core$kCommittedCapital,static$core$kMargin,kMarginOnUnrealized = TRUE, kInvestmentReturn=static$core$kInvestmentReturn,kOverdraftPenalty=static$core$kOverdraftPenalty,intraday=TRUE,realtime=TRUE)
        saveRDS(signals,file=paste("signals_",strftime(Sys.time(),format="%Y-%m-%d %H-%M-%S"),".rds",sep=""))
        saveRDS(optionTrades,paste("optionTrades","_",strftime(Sys.time(),format="%Y-%m-%d %H-%M-%S"),".rds",sep=""))
}

#### PRINT RUN TIME ####
timer.end=Sys.time()
runtime=timer.end-timer.start
print(runtime)
