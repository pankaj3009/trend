# Long term investment algorithm designed to mirror index return
# with potential of outperformance
timer.start=Sys.time()

library(plyr) # used for rbind.fill
library(RTrade)
library(log4r)
library(tableHTML)
library(gmailr)

#### PARAMETERS ####
args.commandline=commandArgs(trailingOnly=TRUE)
if(length(args.commandline)>0){
        args<-args.commandline
}
redisConnect()
redisSelect(1)

if(length(args)>1){
        static<-redisHGetAll(toupper(args[2]))
}else{
        static<-redisHGetAll("LTINVEST")
}

redisClose()

newargs<-unlist(strsplit(static$args,","))
if(length(args)<=1 && length(newargs>1)){
        args<-newargs
}

today=strftime(Sys.Date(),tz=kTimeZone,format="%Y-%m-%d")
kR2Filter=as.numeric(static$R2Filter)
kReturnFilter=as.numeric(static$ReturnFilter)
kDuration=as.numeric(static$Duration)
kMaxBars=as.numeric(static$MaxBars)
kTradeSize=as.numeric(static$TradeSize)
kRealtime=as.logical(static$Realtime)
kTimeZone=as.character(static$TimeZone)
kPerContractBrokerage=as.numeric(static$PerContractBrokerage)
kValueBrokerage=as.numeric(static$ValueBrokerage)
kMaxPositions=as.numeric(static$MaxPositions)
kScaleIn=as.numeric(static$ScaleIn)
kInvestmentReturn=as.numeric(static$InvestmentReturn)
kOverdraftInterest=as.numeric(static$OverdraftInterest)
kIndex=as.character(static$Index)
kBackTest=as.logical(static$BackTest)
kWriteToRedis=as.logical(static$WriteToRedis)
kBackTestStartDate=as.character(static$BackTestStartDate)
kBackTestEndDate=as.character(static$BackTestEndDate)
kSubscribers=fromJSON(static$Subscribers)
kBrokerage=fromJSON(static$Brokerage)
kMargin=as.numeric(static$Margin)

kHomeDirectory=static$HomeDirectory
if(!is.null(kHomeDirectory)){
        setwd(kHomeDirectory)
}
#### FUNCTIONS ####
#### GENERATE SYMBOLS ####

niftysymbols <- createIndexConstituents(2, kIndex, threshold = strftime(as.Date(kBackTestStartDate) -  365))
niftysymbols<-niftysymbols[niftysymbols$startdate<=as.Date(kBackTestEndDate),]
niftysymbols$symbol=paste(niftysymbols$symbol,"_STK___",sep="")
symbols=niftysymbols$symbol
symbols=getMostRecentSymbol(niftysymbols$symbol)

#### GENERATE SIGNALS ####
shortlist=data.frame(symbol=as.character(),monthlyreturn.regress=as.numeric(),monthly.return.actual=as.numeric(),r2=as.numeric(),predict=as.numeric(),actual=as.numeric(),stringsAsFactors = FALSE)
i=0
signals=data.frame()
#for(s in niftysymbols$exchangesymbol){
for(s in symbols){
        i=i+1
        print(paste(i,":",s))
        md.m=loadSymbol(s,dest = "monthly",days=1000,realtime = kRealtime)
        nextwd=adjust("India",as.Date(md.m$date[nrow(md.m)],tz="Asia/Kolkata")+1,bdc=0)
        nextwd=as.POSIXlt(as.POSIXct(nextwd,tz="Asia/Kolkata"))
        if(nextwd$mon<=as.POSIXlt(md.m$date[nrow(md.m)])$mon){
                md.m=md.m[-nrow(md.m),]
        }
        md.m$livemonths=as.numeric(rownames(md.m))
        if(!is.null(nrow(md.m)) && nrow(md.m)>kDuration){
                md.m$referenceprice=(md.m$ahigh+md.m$alow+md.m$asettle)/3
                md.m$slope=slope(md.m$referenceprice,period=kDuration)
                md.m$r2=r2(md.m$referenceprice,period=kDuration)
                md.m$predict=lmprediction(md.m$referenceprice,period=kDuration)
                md.m$monthlyreturn=log(md.m$referenceprice/Ref(md.m$referenceprice,-kDuration))
                # if(tail(md.m$slope,1)>returnfilter && tail(md.m$r2,1)>r2filter){
                #         print(s)
                #         shortlist=rbind(shortlist,data.frame(symbol=s,monthlyreturn.regress=RTrade::specify_decimal(tail(md.m$slope,1)*100,2),monthlyreturn.actual=RTrade::specify_decimal(tail(md.m$monthlyreturn,1),2),r2=tail(md.m$r2,1),predict=tail(md.m$predict,1),actual=tail(md.m$referenceprice,1),stringsAsFactors = FALSE))
                # }
                trend.m=Trend(md.m$date,md.m$ahigh,md.m$alow,md.m$asettle)
                md.m=merge(md.m,trend.m[,c("date","trend")],by=c("date"))
                colnames(md.m)[which(names(md.m)=="trend")]="trend.m"
                md.w=loadSymbol(s,dest="weekly",days=1000,realtime=kRealtime)
                nextwd=adjust("India",as.Date(md.w$date[nrow(md.w)],tz="Asia/Kolkata")+1,bdc=0)
                if(strftime(nextwd,"%V")<=strftime(md.w$date[nrow(md.w)],"%V")){
                        md.w=md.w[-nrow(md.w),]        
                }
                trend.w=Trend(md.w$date,md.w$ahigh,md.w$alow,md.w$asettle)
                md.w=merge(md.w,trend.w[,c("date","trend")],by=c("date"))
                colnames(md.w)[which(names(md.w)=="trend")]="trend.w"
                md.d=loadSymbol(s,days=1000,realtime=kRealtime)
                trend.d=Trend(md.d$date,md.d$ahigh,md.d$alow,md.d$asettle)
                md.d=merge(md.d,trend.d[,c("date","trend")],by=c("date"))
                colnames(md.d)[which(names(md.d)=="trend")]="trend.d"
                md.d=merge(md.d,md.m[,c("date","trend.m","slope","r2","predict","monthlyreturn","livemonths")],by=c("date"),all.x = TRUE)
                md.d$trend.m=na.locf(md.d$trend.m,na.rm = FALSE)
                md.d[md.d$date<="2018-06-15",]
                md.d$slope=na.locf(md.d$slope,na.rm = FALSE)
                md.d$r2=na.locf(md.d$r2,na.rm = FALSE)
                md.d$predict=na.locf(md.d$predict,na.rm = FALSE)
                md.d$monthlyreturn=na.locf(md.d$monthlyreturn,na.rm = FALSE)
                md.d$livemonths=na.locf(md.d$livemonths,na.rm=FALSE)
                md.d=merge(md.d,md.w[,c("date","trend.w")],by=c("date"),all.x = TRUE)
                md.d$trend.w=na.locf(md.d$trend.w,na.rm = FALSE)
                niftystarts=which(niftysymbols$symbol==s)
                md.d$eligible=0
                for(j in niftystarts ){
                        md.d$eligible = ifelse(as.Date(md.d$date) >= niftysymbols[j, c("startdate")] & as.Date(md.d$date) <= niftysymbols[j, c("enddate")],1,md.d$eligible)
                }
                md.d$drawdown=(hhv(md.d$asettle,-252)-md.d$asettle)/md.d$asettle
                md.d$buy=md.d$eligible==1 & md.d$livemonths>kDuration & md.d$slope>kReturnFilter & md.d$r2>kR2Filter & md.d$drawdown<0.25 & (md.d$predict-md.d$asettle)/md.d$asettle>0 & (md.d$predict-md.d$asettle)/md.d$asettle<0.25 & md.d$trend.m==1 & md.d$trend.d!=1 & md.d$monthlyreturn>0.01
                md.d$month=strftime(md.d$date,format="%y%m")
                md.d$week=strftime(md.d$date,format="%y%V")
                md.d$buycount<-ave(md.d$buy, md.d$month, FUN = cumsum)
                md.d$buy=ifelse(md.d$buycount==1 & Ref(md.d$buycount,-1)==0,1,0)
                md.d$sell=ifelse(md.d$asettle>1.2*md.d$predict|md.d$asettle<md.d$predict*0.5 |md.d$monthlyreturn<0.01,1,0)
                md.d$short=0
                md.d$cover=0
                md.d$rsi=RSI(md.d$asettle)
                
                # daysInUpTrend=BarsSince(md.d$trend.d!=1)
                # daysInDownTrend=BarsSince(md.d$trend.d!=-1)
                # daysIndeterminate=BarsSince(md.d$trend.d!=0)
                # md.d$daysintrend=daysInUpTrend+daysInDownTrend+daysIndeterminate
                # md.d$buy=md.d$eligible==1 & md.d$slope>returnfilter & md.d$r2>r2filter & (RTrade::hhv(md.d$asettle,-252)-md.d$asettle)/md.d$asettle<0.20 & (md.d$predict-md.d$asettle)/md.d$asettle>0 & (md.d$predict-md.d$asettle)/md.d$asettle<0.2 & md.d$trend.m==1 & md.d$trend.d==-1 & md.d$monthlyreturn>0.01
                # md.d$buycount<-ave(md.d$buy, md.d$month, FUN = cumsum)
                # md.d$buy=ifelse(md.d$buycount==1 & Ref(md.d$buycount,-1)==0,1,0)
                # md.d$sell=ifelse(md.d$asettle>1.2*md.d$predict|md.d$asettle<md.d$predict*0.4 |md.d$monthlyreturn<0.01|(RTrade::hhv(md.d$asettle,-252)-md.d$asettle)/md.d$asettle>0.4,1,0)
                
                md.d$positionscore=100-md.d$rsi
                #md.d$buy=ExRem(md.d$buy,md.d$sell)
                #md.d$sell=ExRem(md.d$sell,md.d$buy)
                md.d$buyprice=md.d$asettle
                md.d$sellprice=md.d$asettle
                md.d$shortprice=md.d$asettle
                md.d$coverprice=md.d$asettle
                atr<-ATR(md.d[,c("ahigh","alow","asettle")],n=20)
                md.d$atrsl=md.d$asettle-atr[,"atr"]*20
                signals=rbind.fill(signals,md.d)
        }
        
}

#        saveRDS(signals,file="signals_all_5year.rds")
#        signals=readRDS("signals_all_5year.rds")
signals=signals[signals$date>=kBackTestStartDate & signals$date<kBackTestEndDate,]
signals=na.omit(signals)
signals<-signals[order(signals$date,signals$symbol),]

#### GENERATE TRADES ####
trades=ProcessSignals(signals,slamount=rep(0,nrow(signals)),tpamount=rep(0,nrow(signals)),maxbar=rep(kMaxBars,nrow(signals)),maxposition = kMaxPositions,scalein = kScaleIn,debug=TRUE)
trades$size=round(kTradeSize/trades$entryprice)
trades=revalPortfolio(trades,kBrokerage,realtime=FALSE)
#### MAP TO DERIVATIES ####
#### WRITE TO REDIS ####
if(!kBackTest & kWriteToRedis){
        saveRDS(trades,file=paste("trades_",strftime(Sys.time(),format="%Y-%m-%d %H-%M-%S"),".rds",sep=""))
        saveRDS(signals,file=paste("signals_",strftime(Sys.time(),,format="%Y-%m-%d %H-%M-%S"),".rds",sep=""))
        referencetime=adjust("India",Sys.Date()-1,bdc=2)
        referencetime=strftime(referencetime,format="%Y-%m-%d")
        referencetime=as.POSIXct(referencetime,tz=kTimeZone)
        order=data.frame( OrderType="LMT",
                          OrderStage="INIT",
                          TriggerPrice="0",
                          Scale="TRUE",
                          TIF="GTC",
                          OrderReference=tolower(args[2]),
                          stringsAsFactors = FALSE)
        placeRedisOrder(trades,referencetime,order,args[3],setLimitPrice=TRUE)
        
}

#### BACKTEST ####
if(kBackTest){
        bizdays=unique(signals$date)
        pnl<-data.frame(bizdays,realized=0,unrealized=0,brokerage=0)
        #trades$exittime=dplyr::if_else(trades$exitreason=="Open",as.POSIXct(NA_character_),trades$exittime)
        cumpnl<-CalculateDailyPNL(trades,pnl,kBrokerage,deriv=FALSE)
        cumpnl$idlecash=kMaxPositions*kTradeSize-cumpnl$cashdeployed
        cumpnl$daysdeployed=as.numeric(c(diff.POSIXt(cumpnl$bizdays),0))
        cumpnl$investmentreturn=ifelse(cumpnl$idlecash>0,cumpnl$idlecash*cumpnl$daysdeployed*kInvestmentReturn/365,-cumpnl$idlecash*cumpnl$daysdeployed*kOverdraftInterest/365)
        
        # calculate sharpe
        CumPNL <-  cumpnl$realized + cumpnl$unrealized - cumpnl$brokerage + cumsum(cumpnl$investmentreturn)
        DailyPNL <-  CumPNL - Ref(CumPNL, -1)
        DailyPNL <-  ifelse(is.na(DailyPNL),0,DailyPNL)
        DailyReturn <-  ifelse(cumpnl$longnpv +cumpnl$shortnpv== 0, 0,DailyPNL / (kMaxPositions*kTradeSize))
        sharpe <- sharpe(DailyReturn)
        
        # calculate IRR
        cumpnl$cashflow[nrow(cumpnl)]=cumpnl$cashflow[nrow(cumpnl)]+(cumpnl$longnpv+cumpnl$shortnpv)[nrow(cumpnl)]
        xirr=xirr(cumpnl$cashflow+cumpnl$investmentreturn,cumpnl$bizdays,trace = TRUE)
        
        cumpnl$group <- strftime(cumpnl$bizdays, "%Y")
        cumpnl$dailypnl<-DailyPNL
        dd.agg <- aggregate(dailypnl ~ group, cumpnl, FUN = sum)
        dd.agg$dailypnl=RTrade::specify_decimal(dd.agg$dailypnl/(kTradeSize*kMaxPositions),3)
        
        md=loadSymbol("NSENIFTY_IND___",days=10000)
        md=convertToXTS(md[,c("date","aopen","ahigh","alow","asettle")])
        returns=yearlyReturn(md,type="arithmetic")
        returns=convertToDF(returns)
        returns.log=yearlyReturn(md,type="log")
        returns.log=convertToDF(returns.log)
        colnames(returns)[which(names(returns)=="yearly.returns")]="index.returns"
        colnames(returns.log)[which(names(returns.log)=="yearly.returns")]="index.log.returns"
        
        returns$date=strftime(returns$date,"%Y")
        returns.log$date=strftime(returns.log$date,"%Y")
        returns=merge(dd.agg,returns,by.x = c("group"),by.y=c("date"),all.x = TRUE,all.y=FALSE)
        returns=merge(returns,returns.log,by.x=c("group"),by.y=c("date"),all.x=TRUE,all.y=FALSE)
        
        
        print(paste("Average Loss in Losing Month:",mean(dd.agg[which(dd.agg$dailypnl<0),'dailypnl']),sep=""))
        print(paste("Percentage Losing Months:",sum(dd.agg$dailypnl<0)/nrow(dd.agg)))
        
        print(paste("# Trades:",nrow(trades)))
        print(paste("Profit:",sum(DailyPNL)))
        print(paste("Return on equity trades:",sum(trades$abspnl)*100/(kTradeSize*kMaxPositions)*365/as.numeric((min(as.Date(kBackTestEndDate),Sys.Date())-as.Date(kBackTestStartDate)))))
        print(paste("xirr:", xirr, sep = ""))
        print(paste("sharpe:", sharpe, sep = ""))
        print(paste("Win Ratio:",sum(trades$abspnl>0)*100/nrow(trades)))
        print(paste("Avg % Profit Per Trade:",sum(trades$abspnl)*100/nrow(trades)/kTradeSize))
        print(paste("Avg Holding Days:",sum(trades$bars)/nrow(trades)))
        print(returns)
        print(paste("Index Aggregate Return:",RTrade::specify_decimal(exp(sum(returns$index.log.returns))-1,2)))
        print(paste("Strategy Aggregate Return:",RTrade::specify_decimal(sum(DailyPNL/(kMaxPositions*kTradeSize)),2)))
        line=paste("start,end,index.return,strategy.return,win.ratio,sharpe,avg.profit.per.trade,trades,total.profit,trade.profit,invest.profit",sep="")
        line=paste(kBackTestStartDate, #start
                   kBackTestEndDate, #end
                   specify_decimal(exp(sum(returns$index.log.returns))-1,2), #index.return
                   specify_decimal(sum(DailyPNL/(kMaxPositions*kTradeSize)),2), #strategy.return
                   sum(trades$abspnl>0)*100/nrow(trades), #win.ratio
                   specify_decimal(sharpe,2), #sharpe
                   specify_decimal(sum(trades$abspnl)*100/nrow(trades)/kTradeSize,2), #avg.profit.per.trade
                   nrow(trades), #trades
                   sum(DailyPNL), #total.profit
                   sum(trades$pnl), #trade.profit
                   last(cumsum(cumpnl$investmentreturn)), #invest.profit
                   sep=",")
        write(line, file = "summary.csv", append =TRUE)
}


#### EXECUTION SUMMARY ####
if(!kBackTest){
        generateExecutionSummary(trades,unique(signals$date),kBackTestStartDate,kBackTestEndDate,args[2],args[3],kSubscribers,kBrokerage,kCommittedCapital=kMaxPositions*kTradeSize,kMarginOnUnrealized = FALSE,realtime=TRUE)
}
#### PRINT RUN TIME ####
timer.end=Sys.time()
runtime=timer.end-timer.start
print(runtime)