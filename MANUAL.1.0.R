# Manual Trades
# One account in Redis is supported. 
# Other accounts should be in csv as the script allows for a single args[2] + args[3] pair. 

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
        static<-redisHGetAll("MANUAL01")
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

for(s in seq_len(nrow(kSubscribers$subscribers))){
        useForRedis=(kSubscribers$subscribers$account[s]!="")
        if(useForRedis){
                pattern=paste("*trades*",tolower(trimws(args[2])),"*","Order",sep="")
                trades=createPNLSummary(args[3],pattern,kBackTestStartDate,kBackTestEndDate)
        }else{
                trades=read.csv(kSubscribers$subscribers$externalfile[s],header = TRUE,stringsAsFactors = FALSE)
                trades$entrytime=as.POSIXct(trades$entrytime,format="%d-%m-%Y")
                trades$exittime=as.POSIXct(trades$exittime,format="%d-%m-%Y")
                trades$netposition=trades$size
        }
        md=loadSymbol("NSENIFTY_IND___",days=1000000,realtime=TRUE)
        bizdays=md[md$date>=min(trades$entrytime) & md$date<kBackTestEndDate,c("date")]
        a=list()
        a$subscribers=kSubscribers$subscribers[s,]
        generateExecutionSummary(trades,bizdays,kBackTestStartDate,kBackTestEndDate,args[2],args[3],kSubscribers = a,kBrokerage,kCommittedCapital=kCommittedCapital,kMarginOnUnrealized = FALSE,realtime=TRUE)
}
