library(RTrade)
library(zoo)
library(rredis)

#### functions ####
longshortRisk<-function(s,realtime=FALSE,intraday=FALSE,type=NA_character_){
        print(paste("Processing: ",s,sep=""))
        md<-loadSymbol(s,realtime,type)
        if(!is.na(md)[1]){
                t<-Trend(md$date,md$ahigh,md$alow,md$settle)
                t<-cbind(t,md$asettle)
                daysInUpTrend=RTrade::BarsSince(t$trend!=1)
                daysInDownTrend=RTrade::BarsSince(t$trend!=-1)
                daysIndeterminate=RTrade::BarsSince(t$trend!=0)
                daysinTrend=0
                trend=0
                trend<-ifelse(daysInUpTrend>daysInDownTrend,1,ifelse(daysInUpTrend<daysInDownTrend,-1,0))
                daysinTrend<-ifelse(trend==1,daysInUpTrend,ifelse(trend==-1,daysInDownTrend,daysIndeterminate))
                #                averagemove=(t$swinghighhigh_2-t$swinglowlow_2+t$Swinghighhigh_1-t$swinglowlow_1)/2
                avgupswingindices<-which(diff(t$swinglevel)>0)
                avgupswing<-rollmean(diff(t$swinglevel)[avgupswingindices],averageperiod)
                avgupswingindices<-avgupswingindices[(averageperiod):length(avgupswingindices)]
                t$averageupswing<-NA_real_
                t$averageupswing[avgupswingindices]<-avgupswing
                t$averageupswing<-na.locf(t$averageupswing,na.rm = FALSE)
                t$averageupswing<-na.locf(t$averageupswing,fromLast = TRUE)
                
                avgdnswingindices<-which(diff(t$swinglevel)<0)
                avgdnswing<-rollmean(diff(t$swinglevel)[avgdnswingindices],averageperiod)
                avgdnswingindices<-avgdnswingindices[(averageperiod):length(avgdnswingindices)]
                t$averagednswing<-NA_real_
                t$averagednswing[avgdnswingindices]<- -1*avgdnswing
                t$averagednswing<-na.locf(t$averagednswing,na.rm = FALSE)
                t$averagednswing<-na.locf(t$averagednswing,fromLast = TRUE)
                adjswinghigh=ifelse(t$trend==-1 & t$updownbar==1,t$swinghighhigh_1,t$swinghigh)
                adjswinglow=ifelse(t$trend==1 & t$updownbar==-1,t$swinglowlow_1,t$swinglow)
              
                if(intraday){
                        uptrendsl =ifelse(t$trend==1,
                                          ifelse(t$updownbarclean==1 & t$updownbar==1,ifelse(Ref(t$updownbar,-1)==1,md$settle-t$swinglowlow,md$settle-t$swinglowlow_1),# handle scenario where we get an outside bar by eod whilst updownbarclean==1 during the day
                                                 ifelse(t$updownbarclean==-1 & t$updownbar==-1,md$settle-t$swinglowlow_1,
                                                        ifelse(t$updownbar==1 & t$outsidebar==1,md$settle-t$swinglowlow_1,
                                                               ifelse(t$updownbar==-1 & t$outsidebar==1,md$settle-t$swinglowlow_1,
                                                                      ifelse(t$updownbar==1 & t$insidebar==1,md$settle-t$swinglowlow,
                                                                             ifelse(t$updownbar==-1 & t$insidebar==1,md$settle-t$swinglowlow_1,0)))))),0)
                        
                        dntrendsl =ifelse(t$trend==-1,
                                          ifelse(t$updownbarclean==1 & t$updownbar==1,t$swinghighhigh_1-md$settle,
                                                 ifelse(t$updownbarclean==-1 & t$updownbar==-1,ifelse(Ref(t$updownbar,-1)==-1,t$swinghighhigh-md$settle,t$swinghighhigh_1-md$settle),
                                                        ifelse(t$updownbar==1 & t$outsidebar==1,t$swinghighhigh_1-md$settle,
                                                               ifelse(t$updownbar==-1 & t$outsidebar==1,t$swinghighhigh_1-md$settle,
                                                                      ifelse(t$updownbar==1 & t$insidebar==1,t$swinghighhigh_1-md$settle,
                                                                             ifelse(t$updownbar==-1 & t$insidebar==1,t$swinghighhigh-md$settle,0)))))),0)
                        
                }else{
                        uptrendsl =ifelse(t$trend==1,
                                          ifelse(t$updownbarclean==1 & t$updownbar==1,md$settle-t$swinglowlow,
                                                 ifelse(t$updownbarclean==-1 & t$updownbar==-1,md$settle-t$swinglowlow_1,
                                                        ifelse(t$updownbar==1 & t$outsidebar==1,md$settle-t$swinglowlow_1,
                                                               ifelse(t$updownbar==-1 & t$outsidebar==1,md$settle-t$swinglowlow_1,
                                                                      ifelse(t$updownbar==1 & t$insidebar==1,md$settle-t$swinglowlow,
                                                                             ifelse(t$updownbar==-1 & t$insidebar==1,md$settle-t$swinglowlow_1,0)))))),0)
                        
                        dntrendsl =ifelse(t$trend==-1,
                                          ifelse(t$updownbarclean==1 & t$updownbar==1,t$swinghighhigh_1-md$settle,
                                                 ifelse(t$updownbarclean==-1 & t$updownbar==-1,t$swinghighhigh-md$settle,
                                                        ifelse(t$updownbar==1 & t$outsidebar==1,t$swinghighhigh_1-md$settle,
                                                               ifelse(t$updownbar==-1 & t$outsidebar==1,t$swinghighhigh_1-md$settle,
                                                                      ifelse(t$updownbar==1 & t$insidebar==1,t$swinghighhigh_1-md$settle,
                                                                             ifelse(t$updownbar==-1 & t$insidebar==1,t$swinghighhigh-md$settle,0)))))),0)
                        
                }

                sl=uptrendsl+dntrendsl
                
                tp<-ifelse(trend==1,t$averageupswing-(md$asettle-t$swinglow),ifelse(trend==-1,t$averagednswing-(t$swinghigh-md$asettle),0))
                averagemove=ifelse(trend==1,t$averageupswing,ifelse(trend==-1,t$averagednswing,ifelse(t$updownbar==1,t$averageupswing,t$averagednswing)))
                
                hh=t$numberhh
                ll=t$numberll
                risk<-ifelse(tp!=0 & sl/tp>0,(sl/tp),1000000 )
                averagemoveperday=(t$movementsettle/daysinTrend)/md$asettle
                index=nrow(t)
                sl.level=0
                tp.level=0
                if(trend[index]==1){
                        sl.level= md$asettle[index]-sl[index]
                        tp.level=md$asettle[index]+specify_decimal(tp[index],2)
                }else if(trend[index]==-1){
                        sl.level=md$asettle[index]+sl[index]
                        tp.level=md$asettle[index]-specify_decimal(tp[index],2)
                }
                df<-data.frame(
                        symbol=s,
                        trend = trend[index],
                        days.in.trend=daysinTrend[index],
                        exp.swing.move = specify_decimal(averagemove[index],2),
                        sl = specify_decimal(sl[index],2),
                        tp = specify_decimal(tp[index],2),
                        hh = hh[index],
                        ll = ll[index],
                        trend.hl.move=specify_decimal(t$movementhighlow[index],2),
                        trend.settle.move=specify_decimal(t$movementsettle[index],2),
                        trend.daily.pr.move = specify_decimal(averagemoveperday[index]*100,2),
                        risk = specify_decimal(risk[index],2),
                        close = md$asettle[index],
                        sl.level=sl.level,
                        tp.level=tp.level,
                        change = specify_decimal((md$asettle[index]*100/md$asettle[index-1])-100,1),
                        stringsAsFactors = FALSE
                )
                df
        }
        
        
}

#### Script ####
realtime=TRUE
intraday=TRUE
today=strftime(Sys.Date(),tz=kTimeZone,format="%Y-%m-%d")
kFNODataFolder="/home/psharma/Dropbox/rfiles/dailyfno/"
kNiftyDataFolder="/home/psharma/Dropbox/rfiles/daily/"
backteststart="2017-01-01"
backtestend="2018-12-31"
lookback = 365
averageperiod=3
startingperiod=as.Date(backteststart,tz="Asia/Kolkata")-lookback
kTimeZone = "Asia/Kolkata"
reverseNeutralTrend = FALSE
niftysymbols <- createIndexConstituents(2, "nifty50", threshold = strftime(Sys.Date() -  90))
#niftysymbols <- createFNOConstituents(2, "contractsize", threshold = strftime(Sys.Date() -  90))
folots <- createFNOSize(2, "contractsize", threshold = strftime(Sys.Date() - 90))
symbols <- niftysymbols$symbol
options(scipen = 999)
today = strftime(Sys.Date(), tz = kTimeZone, format = "%Y-%m-%d")
alldata<-vector("list",length(symbols))
out <- data.frame()
signals<-data.frame()
allmd <- list()

scan<-data.frame(
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
for(s in symbols){
        df=longshortRisk(s,realtime,intraday,"STK")
        scan=rbind(scan,df)
}

drops<-c("trend","exp.swing.move","trend.hl.move","trend.settle.move")

longtrades<-scan[scan$trend==1,]
print("Long Trades...")
print(longtrades[order(longtrades$risk),!(names(longtrades) %in% drops)])
shorttrades<-scan[scan$trend==-1,]
print("----------------------------------------------------------------------------------")
print("Short Trades...")
print(shorttrades[order(shorttrades$risk),!(names(shorttrades) %in% drops)])
longs=nrow(longtrades)/length(symbols)
shorts=nrow(shorttrades)/length(symbols)
indeterminate=(length(symbols)-nrow(longtrades)-nrow(shorttrades))/length(symbols)
averageLongMaturity=sum(longtrades$days.in.trend)/nrow(longtrades)
averageShortMaturity=sum(shorttrades$days.in.trend)/nrow(shorttrades)
averageInderminateMaturity=sum(scan[scan$trend==0,c("days.in.trend")])/(length(symbols)-nrow(longtrades)-nrow(shorttrades))
out<-paste("Market Sentiment - Long:",longs*100,", Short:",shorts*100,", Maturity:",(averageLongMaturity*longs-averageShortMaturity*shorts),sep="")
print(out)
filename=paste(strftime(Sys.time(),"%Y%m%d %H:%M:%S"),"longshort.csv",sep="_")
write.csv(rbind(longtrades,shorttrades),file=filename)
