# Long term investment algorithm designed to mirror index return
# with potential of outperformance

library(plyr)
library(RTrade)
library(quantmod)
library(bizdays)
r2filter=0.85
returnfilter=0.01
kDuration=120
kBackTestStartDate="2018-01-01"
kBackTestEndDate="2022-12-31"
kMaxBars=10000
kTradeSize=40000
realtime=FALSE
kTimeZone="Asia/Kolkata"
kPerContractBrokerage=0
kValueBrokerage=0.0015
kNiftyDataFolder="/home/psharma/Dropbox/rfiles/daily/"
kMaxPositions=25
kScaleIn=25
kInvestmentReturn=0.06
index="nifty50"
backtest=TRUE
kWriteToRedis=FALSE
args=c("1","LTINVEST01","4")

# niftysymbols <-readAllSymbols(2,"ibsymbols")
# niftysymbols=list.files(kNiftyDataFolder)
# niftysymbols=sapply(strsplit(niftysymbols,"\\."),"[",1)
# symbolchange=getSymbolChange()
# exclude=na.omit(match(symbolchange$key,niftysymbols))
# niftysymbols=niftysymbols[-exclude]

niftysymbols <- createIndexConstituents(2, index, threshold = strftime(as.Date(kBackTestStartDate) -  365))
niftysymbols<-niftysymbols[niftysymbols$startdate<=as.Date(kBackTestEndDate),]
symbols=niftysymbols$symbol
symbols=getMostRecentSymbol(niftysymbols$symbol)


shortlist=data.frame(symbol=as.character(),monthlyreturn.regress=as.numeric(),monthly.return.actual=as.numeric(),r2=as.numeric(),predict=as.numeric(),actual=as.numeric(),stringsAsFactors = FALSE)
i=0
signals=data.frame()
#for(s in niftysymbols$exchangesymbol){
for(s in symbols){
        i=i+1
        print(paste(i,":",s))
        md.m=loadSymbol(s,sourceDuration="DAILY",destDuration = "MONTHLY")
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
                md.w=loadSymbol(s,sourceDuration="DAILY",destDuration = "WEEKLY")
                nextwd=adjust("India",as.Date(md.w$date[nrow(md.w)],tz="Asia/Kolkata")+1,bdc=0)
                if(strftime(nextwd,"%V")<=strftime(md.w$date[nrow(md.w)],"%V")){
                        md.w=md.w[-nrow(md.w),]        
                }
                trend.w=Trend(md.w$date,md.w$ahigh,md.w$alow,md.w$asettle)
                md.w=merge(md.w,trend.w[,c("date","trend")],by=c("date"))
                colnames(md.w)[which(names(md.w)=="trend")]="trend.w"
                md.d=loadSymbol(s)
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
                md.d$buy=md.d$eligible==1 & md.d$livemonths>kDuration & md.d$slope>returnfilter & md.d$r2>r2filter & md.d$drawdown<0.25 & (md.d$predict-md.d$asettle)/md.d$asettle>0 & (md.d$predict-md.d$asettle)/md.d$asettle<0.25 & md.d$trend.m==1 & md.d$trend.d!=1 & md.d$monthlyreturn>0.01
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
trades=ProcessSignals(signals,slamount=rep(0,nrow(signals)),tpamount=rep(0,nrow(signals)),maxbar=rep(kMaxBars,nrow(signals)),maxposition = kMaxPositions,scalein = kScaleIn,debug=TRUE)
#trades=ProcessSignals(signals,slamount=signals$atrsl,tpamount=rep(0,nrow(signals)),maxbar=rep(kMaxBars,nrow(signals)),maxposition = kMaxPositions,scalein = kScaleIn,debug=TRUE)
trades.names=names(trades)
# update mtm
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

bizdays=as.Date(unique(signals$date),tz=kTimeZone)
pnl<-data.frame(bizdays,realized=0,unrealized=0,brokerage=0)
trades$size=kTradeSize/trades$entryprice
trades$size=round(trades$size)
trades$entrybrokerage=ifelse(trades$entryprice==0,0,ifelse(grepl("BUY",trades$trade),kPerContractBrokerage+trades$entryprice*trades$size*kValueBrokerage,kPerContractBrokerage+trades$entryprice*trades$size*(kValueBrokerage+kSTTSell)))
trades$exitbrokerage=ifelse(trades$exitprice==0,0,ifelse(grepl("BUY",trades$trade),kPerContractBrokerage+trades$exitprice*trades$size*kValueBrokerage,kPerContractBrokerage+trades$exitprice*trades$size*(kValueBrokerage+kSTTSell)))
trades$brokerage=(trades$entrybrokerage+trades$exitbrokerage)/(2*trades$size)
trades$percentprofit<-ifelse(grepl("BUY",trades$trade),(trades$exitprice-trades$entryprice)/trades$entryprice,-(trades$exitprice-trades$entryprice)/trades$entryprice)
trades$percentprofit<-ifelse(trades$exitprice==0|trades$entryprice==0,0,trades$percentprofit)
trades$netpercentprofit <- trades$percentprofit - trades$brokerage/(trades$entryprice+trades$exitprice)/2
trades$abspnl=ifelse(trades$trade=="BUY",trades$size*(trades$exitprice-trades$entryprice),-trades$size*(trades$exitprice-trades$entryprice))-trades$entrybrokerage-trades$exitbrokerage
trades$abspnl=ifelse(trades$exitprice==0,0,trades$abspnl)
trades$exittime=dplyr::if_else(trades$exitreason=="Open",as.POSIXct(NA_character_),trades$exittime)

if(backtest){        
        cumpnl<-CalculateDailyPNL(trades,pnl,kNiftyDataFolder,trades$brokerage,deriv=FALSE)
        trades.invest=ProcessSurplusCash(cumpnl,kMaxPositions,kTradeSize,kInvestmentReturn)
        trades.invest=trades.invest[trades.invest$bars>0,]
        trades.invest$size=1
        trades.invest$entrybrokerage=0
        trades.invest$exitbrokerage=0
        trades.invest$brokerage=0
        trades.invest$percentprofit<-(trades.invest$exitprice-trades.invest$entryprice)/trades.invest$entryprice
        trades.invest$netpercentprofit <- trades.invest$percentprofit
        trades.invest$abspnl=trades.invest$exitprice-trades.invest$entryprice
        invest=aggregate(abspnl ~ exittime, trades.invest, FUN = sum)
        trades.all=rbind(trades,trades.invest)
        realized=merge(cumpnl[,c("bizdays","realized")],invest[,c("exittime","abspnl")],by.x=c("bizdays"),by.y=c("exittime"),all.x = TRUE)
        realized$abspnl[is.na(realized$abspnl)]=0
        realized$total=realized$realized+realized$abspnl
        cumpnl$realized=realized$total
        
        DailyPNL <- (cumpnl$realized + cumpnl$unrealized-cumpnl$brokerage) - Ref(cumpnl$realized + cumpnl$unrealized-cumpnl$brokerage, -1)
        DailyPNL <- ifelse(is.na(DailyPNL), 0, DailyPNL)
        DailyReturn <- DailyPNL/(kMaxPositions*kTradeSize)
        df <- data.frame(time = as.Date(unique(signals$date),tz=kTimeZone), return = DailyReturn)
        df <- read.zoo(df)
        #sharpe <-  SharpeRatio((df[df != 0][, 1, drop = FALSE]), Rf = .07 / 365, FUN = "StdDev") * sqrt(252)
        sharpeRatio<- sharpe(DailyReturn)
        
        cashflow<-CashFlow(trades.all,bizdays,kValueBrokerage)
        trades$proceeds=trades$exitprice*trades$size
        maturityproceeds=sum(trades[trades$exitreason=="Open","proceeds"])
        cashflow[length(cashflow)]=last(cashflow)+maturityproceeds
        
        irr=0
        if (sum(cashflow) > 0) {
                irr <- xirr(cashflow, bizdays) * 100
        }
        
        cumpnl$group <- strftime(cumpnl$bizdays, "%Y")
        cumpnl$dailypnl<-DailyPNL
        dd.agg <- aggregate(dailypnl ~ group, cumpnl, FUN = sum)
        dd.agg$dailypnl=RTrade::specify_decimal(dd.agg$dailypnl/(kTradeSize*kMaxPositions),3)
        
        md=loadSymbol("NSENIFTY")
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
        print(paste("Profit as Sum of Equity Trades:",sum(DailyPNL)))
        print(paste("Profit as Sum of Equity + Investment :",sum(trades.all$abspnl)))
        print(paste("Return:",sum(trades.all$abspnl)*100/(kTradeSize*kMaxPositions)*365/as.numeric((min(as.Date(kBackTestEndDate),Sys.Date())-as.Date(kBackTestStartDate)))))
        print(paste("xirr:", irr, sep = ""))
        print(paste("sharpe:", sharpeRatio, sep = ""))
        print(paste("Win Ratio:",sum(trades$abspnl>0)*100/nrow(trades)))
        print(paste("Avg % Profit Per Trade:",sum(trades$abspnl)*100/nrow(trades)/kTradeSize))
        print(paste("Avg Holding Days:",sum(trades$bars)/nrow(trades)))
        print(returns)
        print(paste("Index Aggregate Return:",RTrade::specify_decimal(exp(sum(returns$index.log.returns))-1,2)))
        print(paste("Strategy Aggregate Return:",RTrade::specify_decimal(sum(trades.all$abspnl/(kMaxPositions*kTradeSize)),2)))
        line=paste(kBackTestStartDate,kBackTestEndDate,specify_decimal(exp(sum(returns$index.log.returns))-1,2),
                   specify_decimal(sum(trades.all$abspnl/(kMaxPositions*kTradeSize)),2),sum(trades$abspnl>0)*100/nrow(trades),
                   specify_decimal(sharpeRatio,2),specify_decimal(sum(trades$abspnl)*100/nrow(trades)/kTradeSize,2),
                   nrow(trades),sum(trades.all$abspnl),sum(DailyPNL),sep=",")
        write(line, file = "summary.csv", append =TRUE)
}
if(!backtest & kWriteToRedis){
        saveRDS(trades,file=paste("trades_",strftime(Sys.time())))
        saveRDS(signals,file=paste("signals_",strftime(Sys.time())))
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
        trades$symbol=paste(trades$symbol,"_STK___",sep="")
        placeRedisOrder(trades,referencetime,order,args[3],setLimitPrice=TRUE)
        
}
