baapcode<-function(){
  library(DBI)
  library(RMySQL)
  options(java.parameters = "-Xmx1048m")
  library(RODBC)
  library(rJava)
  library(RJDBC)
  library(proto)
  library(gsubfn)
  library(RSQLite)
  library(sqldf)
  library(ParamHelpers)
  library(mlr)
  library(xgboost)
  library(caTools)
  ##Outlier Function
  pcap <- function(x){
    for (i in which(sapply(x, is.numeric))) {
      quantiles <- quantile( x[,i], c(.05, .95 ), na.rm =TRUE)
      x[,i] = ifelse(x[,i] < quantiles[1] , quantiles[1], x[,i])
      x[,i] = ifelse(x[,i] > quantiles[2] , quantiles[2], x[,i])}
    x}
  data1<-read.csv("data1.csv")
  
  
  Sys.setenv(JAVA_HOME='C:/Program Files/Java/jre1.8.0_144')
  driver <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver","C:/Program Files/Java/file.jar")
  connection <- dbConnect(driver, "jdbc:sqlserver://tatdemo.cloudapp.net:57784;databaseName=TENANTDB", "nbmetadata", "nbmetadatapoc$123")
  
  
  # tablename<-c("[TENANTDB].qSmDEkwNCqERyAa.sh_sales_prediction_11706066","[TENANTDB].qSmDEkwNCqERyAa.sh_sales_prediction_TC","[TENANTDB].qSmDEkwNCqERyAa.sh_sales_prediction_IT","[TENANTDB].qSmDEkwNCqERyAa.sh_sales_prediction_PH","[TENANTDB].qSmDEkwNCqERyAa.sh_sales_prediction_GA","[TENANTDB].qSmDEkwNCqERyAa.sh_sales_prediction_HA")
  # provide the tablename below from above for which you have to do prediction
  
  tablename<-c("[TENANTDB].qSmDEkwNCqERyAa.sh_sales_prediction_GA")
  for(x in tablename){
    producttype<-sprintf("select distinct producttype from %s ",x)
    producttype<-dbGetQuery(connection,producttype)
    pt<-producttype$producttype
    newframe1<-data.frame(Date=as.character(),item_code=numeric(),feature_code=numeric(),quantity=numeric())
    for(y in pt){
      Sys.setenv(JAVA_HOME='C:/Program Files/Java/jre1.8.0_144')
      driver <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver","C:/Program Files/Java/file.jar")
      connection <- dbConnect(driver, "jdbc:sqlserver://tatdemo.cloudapp.net:57784;databaseName=TENANTDB", "nbmetadata", "nbmetadatapoc$123")
      
      Fetched <- sprintf("select * from %s
                         where producttype = '%s' 
                         and age >0
                         order by inv_date ",x,y)
      
      playnew <- dbGetQuery(connection,Fetched )
      # library(ParamHelpers)
      # library(mlr)
      # library(xgboost)
      # library(caTools)
      ##Outlier Function
      # pcap <- function(x){
      #   for (i in which(sapply(x, is.numeric))) {
      #     quantiles <- quantile( x[,i], c(.05, .95 ), na.rm =TRUE)
      #     x[,i] = ifelse(x[,i] < quantiles[1] , quantiles[1], x[,i])
      #     x[,i] = ifelse(x[,i] > quantiles[2] , quantiles[2], x[,i])}
      #   x}
      #save pcap function in environment
      #save data1 file in environment
      # pass the file name in this function
      
      # baapcode<-function(playnew){
      playnew$inv_date<-as.Date(playnew$inv_date,format = '%Y-%m-%d')
      playnew$item_code<-as.factor(playnew$item_code)
      class(playnew$foc_value)<-"numeric"
      playnew[is.na(playnew)]<-0
      data1<-read.csv("data1.csv")
      newspapercost<-data1$DPS_Cost+data1$FP_Cost+data1$HP_Cost+data1$OBC_Cost+data1$Strip_Cost
      radiocost<-data1$Arabic_Rcost+data1$English_Rcost+data1$Hindi_Rcost+data1$Malayalam_Rcost+data1$Other_Rcost
      date<-data1$date_index
      newdataframe<-data.frame(inv_date=date,ncost=newspapercost,rcost=radiocost)
      newdataframe$inv_date<-as.Date(newdataframe$inv_date,format = '%Y-%m-%d')
      playnew<-merge(playnew,newdataframe,by.x ="inv_date")
      a<-colnames(playnew)[39:57]
      playnew<-subset(playnew,select=!(names(playnew) %in% a))
      a<-c("weekend_1_or_0","ttl_area","count_of_counters","promotion_value","weightage","promo_count","quantity_last_3m","quantity_last_12m","month","DPS","OBC","FP","HP","Strip","Arabic_Radio","English_Radio","Hindi_Radio","Malayalam_Radio","recency_index","week_mm")
      playnew<-subset(playnew,select=!(names(playnew) %in% a))
      a<-c("promo_indicator","modelnumber","itemname","producttype","category","brand","item_status","first_purchase","quantity_last_1m","quantity_last_6m","quantity_last_9m","brand_no.","perday_last180","perday_last270","week_day","mm_month","yyyy_year","schoolholidays_1_or_0","nd_holiday_1_or_0","n2d_holiday_1_or_0","count_of_stores","features","brand_no","perday_last360")
      playnew<-subset(playnew,select=!(names(playnew) %in% a))
      playnew$month<-months(playnew$inv_date)
      month_c<-as.data.frame(with(playnew, model.matrix(~ month + 0)))
      playnew<-cbind(playnew,month_c)
      a<-c("weekend_1_or_0","ttl_area","count_of_counters","promotion_value","weightage","promo_count","quantity_last_3m","quantity_last_12m","month","DPS","OBC","FP","HP","Strip","Arabic_Radio","English_Radio","Hindi_Radio","Malayalam_Radio","recency_index","week_mm")
      playnew<-subset(playnew,select=!(names(playnew) %in% a))
      finaltrain<-playnew[playnew$inv_date<as.Date('8/01/2018',format='%d/%m/%Y'),]
      finaltest<-playnew[playnew$inv_date>=as.Date('8/01/2018',format='%d/%m/%Y'),]
      finaltest<-finaltest[finaltest$inv_date<=as.Date('28/01/2018',format='%d/%m/%Y'),]
      finaltrain$item_code<-as.character(finaltrain$item_code)
      finaltrain$item_code<-as.factor(finaltrain$item_code)
      finaltest$item_code<-as.character(finaltest$item_code)
      finaltest$item_code<-as.factor(finaltest$item_code)
      quantity<-finaltrain$quantity
      a<-c("quantity")
      finaltrain<-subset(finaltrain,select=!(names(finaltrain) %in% a))
      b<-data.frame(quantity=quantity)
      b<-pcap(b)
      finaltrain<-cbind(finaltrain,b)
      finaltrain$unit_price<-as.numeric(finaltrain$unit_price)
      finaltest$unit_price<-as.numeric(finaltest$unit_price)
      finaltrain$voucher_value<-as.numeric(finaltrain$voucher_value)
      finaltest$voucher_value<-as.numeric(finaltest$voucher_value)
      finaltrain$foc_value<-as.numeric(finaltrain$foc_value)
      finaltest$foc_value<-as.numeric(finaltest$foc_value)
      
      # for(i in seq(1,nrow(finaltrain))){
      # vvalue<-finaltrain[i,]$voucher_value
      # price<-finaltrain[i,]$unit_price
      finaltrain$effectiveprice<-finaltrain$unit_price-finaltrain$voucher_value
      # }
      # for(i in seq(1,nrow(finaltest))){
      #   vvalue<-finaltest[i,]$voucher_value
      #   price<-finaltest[i,]$unit_price
      finaltest$effectiveprice<-finaltest$unit_price-finaltest$voucher_value
      # }
      
      
      # data1<-read.csv("data1.csv")
      # newspapercost<-data1$DPS_Cost+data1$FP_Cost+data1$HP_Cost+data1$OBC_Cost+data1$Strip_Cost
      # radiocost<-data1$Arabic_Rcost+data1$English_Rcost+data1$Hindi_Rcost+data1$Malayalam_Rcost+data1$Other_Rcost
      # date<-data1$date_index
      # newdataframe<-data.frame(inv_date=date,ncost=newspapercost,rcost=radiocost)
      # newdataframe$inv_date<-as.Date(newdataframe$inv_date,format = '%Y-%m-%d')
      
      # playnew$month<-months(playnew$inv_date)
      # month_c<-as.data.frame(with(playnew, model.matrix(~ month + 0)))
      # playnew<-cbind(playnew,month_c)
      # first<-c(numeric())
      # second<-c(numeric())
      # third<-c(numeric())
      # fourth<-c(numeric())
      # fifth<-c(numeric())
      # for (i in seq(1:nrow(Tv32))){
      #   itemcode<-Tv32[i,]$item_code
      #   diftime<-difftime(Tv32[i,1],min(Tv32[Tv32$item_code==itemcode,1]),units=c("days"))
      #   diftime<-as.numeric(diftime)
      #   if(diftime<=90){
      #     first<-append(first,1)
      #     second<-append(second,0)
      #     third<-append(third,0)
      #     fourth<-append(fourth,0)
      #     fifth<-append(fifth,0)
      #   }
      #   if(diftime>90 && diftime<=180){
      #     first<-append(first,0)
      #     second<-append(second,1)
      #     third<-append(third,0)
      #     fourth<-append(fourth,0)
      #     fifth<-append(fifth,0)
      #   }
      #   if(diftime>180&&diftime<=270){
      #     first<-append(first,0)
      #     second<-append(second,0)
      #     third<-append(third,1)
      #     fourth<-append(fourth,0)
      #     fifth<-append(fifth,0)
      #   }
      #   if(diftime>270&&diftime<=365){
      #     first<-append(first,0)
      #     second<-append(second,0)
      #     third<-append(third,0)
      #     fourth<-append(fourth,1)
      #     fifth<-append(fifth,0)
      #   }
      #   if(diftime>365){
      #     first<-append(first,0)
      #     second<-append(second,0)
      #     third<-append(third,0)
      #     fourth<-append(fourth,0)
      #     fifth<-append(fifth,1)
      #   }
      # }
      # Tv32<-cbind(Tv32,first,second,third,fourth,fifth)
      # Tv32$weight<-c((.025*Tv32$first)+(.075*Tv32$second)+(.05*Tv32$third)+(.15*Tv32$fourth)+(.70*Tv32$fifth))
      # finalweight<-c(numeric())
      # for (i in seq(1:nrow(Tv32))){
      #   itemcode<-Tv32[i,]$item_code
      #   diftime<-difftime(Tv32[i,1],min(Tv32[Tv32$item_code==itemcode,1]),units=c("days"))
      #   diftime<-as.numeric(diftime)
      #   finalweight<-append(finalweight,(Tv32$weight[i]*diftime))
      # }
      # Tv32<-cbind(Tv32,finalweight)
      
      # finaltrain<-playnew[playnew$date_index<as.Date('13/12/2017',format='%d/%m/%Y'),]
      # finaltest<-playnew[playnew$date_index>=as.Date('26/12/2017',format='%d/%m/%Y'),]
      # finaltest<-finaltest[finaltest$date_index<=as.Date('28/01/2018',format='%d/%m/%Y'),]
      # finaltrain$item_code<-as.character(finaltrain$item_code)
      # finaltrain$item_code<-as.factor(finaltrain$item_code)
      # finaltest$item_code<-as.character(finaltest$item_code)
      # finaltest$item_code<-as.factor(finaltest$item_code)
      # finaltest<-finaltest[finaltest$item_code=11736077,]
      # finaltrain<-finaltrain[finaltrain$item_code=11736077,]
      # finaltrain$features_code<-as.character(finaltrain$features_code)
      # finaltrain$features_code<-as.factor(finaltrain$features_code)
      # finaltest$features_code<-as.character(finaltest$features_code)
      # finaltest$features_code<-as.factor(finaltest$features_code)
      finaltrain$feature_code<-as.factor(finaltrain$feature_code)
      finaltest$feature_code<-as.factor(finaltest$feature_code)
      
      a<-c(numeric())
      for(i in levels(finaltest$feature_code)){
        if(nrow(finaltrain[finaltrain$feature_code==i,])>3){
          a<-append(a,i)
        }}
      
      
      newframe<-data.frame(Date=as.character(),item_code=numeric(),feature_code=numeric(),quantity=numeric())
      set.seed(1001)
      getParamSet("regr.xgboost")
      xg_set <- makeLearner("regr.xgboost", predict.type = "response")
      xg_set$par.vals <- list(
        objective = "reg:linear",
        eval_metric = "rmse",
        nrounds = 10
      )
      xg_ps <- makeParamSet(
        makeIntegerParam("nrounds",lower=9,upper=81),
        makeIntegerParam("max_depth",lower=6,upper=10),
        makeNumericParam("lambda",lower=0.50,upper=0.60),
        makeNumericParam("eta", lower = 0.001, upper = 0.3),
        makeNumericParam("subsample", lower = 0.50, upper = 1),
        makeNumericParam("min_child_weight",lower=1,upper=5),
        makeNumericParam("colsample_bytree",lower = 0.5,upper = 0.9)
      )
      rancontrol <- makeTuneControlRandom(maxit = 70L) #do 100 iterations
      set_cv <- makeResampleDesc("CV",iters = 3L)
      for(i in a){
        # aw<-finaltrain[finaltrain$features_code==i,]
        # aw$item_code<-as.character(aw$item_code)
        # aw$item_code<-as.factor(aw$item_code)
        # for(j in levels(aw$item_code)){
        # testpredictions$item_code<-as.character(testpredictions$item_code)
        # testpredictions$item_code<-as.factor(testpredictions$item_code)
        # newdata$item_code<-as.character(newdata$item_code)
        # newdata$item_code<-as.factor(newdata$item_code)
        #
        sorted<-finaltrain[finaltrain$feature_code==i,]
        finaltest1<-finaltest[finaltest$feature_code==i,]
        #set.seed(101)
        #samplesorted<-sample.split(sorted$quantity,SplitRatio=0.7)
        #sampletrain<-subset(sorted,samplesorted==T)
        #sampletest<-subset(sorted,samplesorted==F)
        traintask_1 <- makeRegrTask(data=sorted[,-c(1,6,7,9,16)],target="quantity")# it should include date,itemcode and month variable
        #testtask_1 <- makeRegrTask(data=sampletest[-c(1,29,31)],target="quantity")
        #Model tuning
        xg_tune <- tuneParams(learner = xg_set, task = traintask_1, resampling = set_cv,measures =mse,par.set = xg_ps, control = rancontrol)
        #set parameters
        xg_new <- setHyperPars(learner = xg_set, par.vals = xg_tune$x)
        #train model
        xgmodel_acc <- train(xg_new, traintask_1)
        #training<-xgboost(data = data.matrix(sampletrain[,-c(1,2,3,17,18,25,26,27,28,29,30,31,32)]), label = sampletrain[,18], max.depth = 4,eta = .2, nthread = 2, nround = 100,objective = "reg:linear")
        #avg<-mean(sorted$unit_price)
        #finaltest1$unit_price<-avg
        predict.xg <- predict(xgmodel_acc,newdata=finaltest1[,-c(1,6,7,8,10,17)]) # it should include date,month,itemcode and quantity
        #prediction<-predict(newdata=data.matrix(finaltest1),training)
        dataf<-data.frame(Date=as.character(finaltest1$inv_date),item_code=(finaltest1$item_code),feature_code=(finaltest1$feature_code),quantity=predict.xg)
        newframe<-rbind(newframe,dataf)
      }
      # write.csv(newframe,paste("predict",x,y,sep="_"))
      newframe1<-rbind(newframe1,newframe)
      rm(playnew)
      rm(finaltrain)
      rm(finaltest)
      rm(sorted)
      rm(finaltest1)
      rm(newframe)
    }
    write.csv(newframe1,paste("output",x,"csv",sep="."))
    modelOutput<-newframe1
    library(sqldf)
    library(lubridate)
    library(dplyr)
    library(reshape2)
    str(modelOutput)
    #modelOutput$Date<-mdy(modelOutput$Date)
    modelOutput$Date<-as.Date(modelOutput$Date)
    modelOutput$wk<-(strftime(modelOutput$Date, format = "%V"))
    modelOutput$response[modelOutput$response<0]<-0
    #sqldf()
    
    modelOutput_Wk <- modelOutput %>%
      group_by(item_code, wk) %>%
      summarise(Net = sum(response))
    modelOutput_Wk<-as.data.frame(modelOutput_Wk)
    modelOutput_Wk$Net<-round(modelOutput_Wk$Net)
    finalOutput<-dcast(modelOutput_Wk, item_code ~ wk, value.var="Net", fun.aggregate=sum)
    
    write.csv(finalOutput,paste("finalOutput",x,"csv",sep="."))
    rm(newframe1)
    rm(finalOutput)
  }}


