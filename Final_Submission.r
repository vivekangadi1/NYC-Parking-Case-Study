rm(list=ls())
# load SparkR
spark_path <- '/usr/local/spark'
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = spark_path)
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
library(sparklyr)
sparkR.session(master = "yarn", sparkConfig = list(spark.driver.memory = "1g"))
path1 <- '/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2017.csv'
path2 <- '/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2015.csv'
path3 <- '/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2016.csv'

#2. Read the data files
nyc2017 <- read.df(path1, "csv", header = "true", inferSchema = "true", na.strings = "NA")
nyc2016 <- read.df(path3, "csv", header = "true", inferSchema = "true", na.strings = "NA")
nyc2015 <- read.df(path2, "csv", header = "true", inferSchema = "true", na.strings = "NA")
#nyc2015 <- read.df('/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2015.csv', "csv", header="true", inferSchema = "true")

#drop the duplicates if any
nyc2017 <- dropDuplicates(nyc2017, "Summons number")
nyc2016 <- dropDuplicates(nyc2016, "Summons number")
nyc2015 <- dropDuplicates(nyc2015, "Summons number")

################################################################################################################################
#Examine the data
################################################################################################################################                          
#Q1. Find total number of tickets for each year.


totalRecords2017 <- nrow(nyc2017)
totalRecords2017
totalRecords2016 <- nrow(nyc2016)
totalRecords2016
totalRecords2015 <- nrow(nyc2015)
totalRecords2015
# 10803028 tickets in 2017
# 10626899 tickets in 2016
# 10951256 tickets in 2015


# Number of unique states

stateDF2017 <- select(nyc2017, countDistinct(nyc2017$`Registration State`))
collect(stateDF2017)
stateDF2016 <- select(nyc2016, countDistinct(nyc2016$`Registration State`))
collect(stateDF2016)
stateDF2015 <- select(nyc2015, countDistinct(nyc2015$`Registration State`))
collect(stateDF2015)
#67 states in 2017
#68 states in 2016
#69 states in 2015

#Tickets with no address in them

#nrow(filter(nyc2017, nyc2017$`Street Name`=="NaN"))
#nrow(filter(nyc2016, nyc2016$`Street Name`=="NaN"))
#nrow(filter(nyc2015, nyc2015$`Street Name`=="NaN"))

# Assumption made here is that when the Street Name is not present then the address is not provided.
withAddress2017 <- filter(nyc2017, "`Street Name` != ''")
nWithAdd2017 <- nrow(withAddress2017)

nWithoutAddress2017 <- totalRecords2017 - nWithAdd2017
nWithoutAddress2017

withAddress2016 <- filter(nyc2016, "`Street Name` != ''")
nWithAdd2016 <- nrow(withAddress2016)

nWithoutAddress2016 <- totalRecords2016 - nWithAdd2016
nWithoutAddress2016

withAddress2015 <- filter(nyc2015, "`Street Name` != ''")
nWithAdd2015 <- nrow(withAddress2015)

nWithoutAddress2015 <- totalRecords2015 - nWithAdd2015
nWithoutAddress2015
#4009 tickets with no Street Name in 2017
#8274 tickets with no Street Name in 2016
#5463 tickets with no Street Name in 2015

################################################################################################################################
#Aggregation Tasks
################################################################################################################################
# Q1. How often does each violation code occur? (frequency of violation codes - find the top 5).########
#### Top 5 violation codes ####

violationDF2017 <- summarize(groupBy(nyc2017, nyc2017$`Violation Code`), count = n(nyc2017$`Violation Code`))
head(arrange(violationDF2017, desc(violationDF2017$count)), 5)

violationDF2016 <- summarize(groupBy(nyc2016, nyc2016$`Violation Code`), count = n(nyc2016$`Violation Code`))
head(arrange(violationDF2016, desc(violationDF2016$count)), 5)

violationDF2015 <- summarize(groupBy(nyc2015, nyc2015$`Violation Code`), count = n(nyc2015$`Violation Code`))
head(arrange(violationDF2015, desc(violationDF2015$count)), 5)
#  Violation Code   count in 2017                               
#1             21 1528588
#2             36 1400614
#3             38 1062304
#4             14  893498
#5             20  618593

###Violation Code   count   in 2016                                                     
#1             21 1531587
#2             36 1253512
#3             38 1143696
#4             14  875614
#5             37  686610

#        Violation Code  count in 2015
#1             21        1501614
#2             38        1324586
#3             14         924627
#4             36         761571
#5             37         746278
#### Top 5 vehicle body type ####

vehicleBodyTyepDF2017 <- summarize(groupBy(nyc2017, nyc2017$`Vehicle Body Type`), count = n(nyc2017$`Vehicle Body Type`))
head(arrange(vehicleBodyTyepDF2017, desc(vehicleBodyTyepDF2017$count)), 5)

vehicleBodyTyepDF2016 <- summarize(groupBy(nyc2016, nyc2016$`Vehicle Body Type`), count = n(nyc2016$`Vehicle Body Type`))
head(arrange(vehicleBodyTyepDF2016, desc(vehicleBodyTyepDF2016$count)), 5)

vehicleBodyTyepDF2015 <- summarize(groupBy(nyc2015, nyc2015$`Vehicle Body Type`), count = n(nyc2015$`Vehicle Body Type`))
head(arrange(vehicleBodyTyepDF2015, desc(vehicleBodyTyepDF2015$count)), 5)

###Vehicle Body Type   count in 2017                   
#1              SUBN 3719802
#2              4DSD 3082020
#3               VAN 1411970
#4              DELV  687330
#5               SDN  438191

###Vehicle Body Type   count                                                     
#1              SUBN 3466037
#2              4DSD 2992107
#3               VAN 1518303
#4              DELV  755282
#5               SDN  424043

#  Vehicle Body Type   count  in 2015
#1              SUBN 3451963
#2              4DSD 3102510
#3               VAN 1605228
#4              DELV  840441
#5               SDN  453992
#### Top 5 vehicle make ####

vehicleMakeDF2017 <- summarize(groupBy(nyc2017, nyc2017$`Vehicle Make`), count = n(nyc2017$`Vehicle Make`))
head(arrange(vehicleMakeDF2017, desc(vehicleMakeDF2017$count)), 5)

vehicleMakeDF2016 <- summarize(groupBy(nyc2016, nyc2016$`Vehicle Make`), count = n(nyc2016$`Vehicle Make`))
head(arrange(vehicleMakeDF2016, desc(vehicleMakeDF2016$count)), 5)

vehicleMakeDF2015 <- summarize(groupBy(nyc2015, nyc2015$`Vehicle Make`), count = n(nyc2015$`Vehicle Make`))
head(arrange(vehicleMakeDF2015, desc(vehicleMakeDF2015$count)), 5)
###Vehicle Make   count in 2017
#1         FORD 1280958
#2        TOYOT 1211451
#3        HONDA 1079238
#4        NISSA  918590
#5        CHEVR  714655

###Vehicle Make   count                                                          
#1         FORD 1324774
#2        TOYOT 1154790
#3        HONDA 1014074
#4        NISSA  834833
#5        CHEVR  759663

# Vehicle Make  count in 2015
#1         FORD 1521874
#2        TOYOT 1217087
#3        HONDA 1102614
#4        NISSA  908783
#5        CHEVR  897845
#Q3. A precinct is a police station that has a certain zone of the city under its command. Find the (5 highest) frequencies of: ##
## 3.1 Top 5 Violating Precincts 

precintDF2017 <- summarize(groupBy(nyc2017, nyc2017$`Violation Precinct`), count = n(nyc2017$`Violation Precinct`))
head(arrange(precintDF2017, desc(precintDF2017$count)), 5)

precintDF2016 <- summarize(groupBy(nyc2016, nyc2016$`Violation Precinct`), count = n(nyc2016$`Violation Precinct`))
head(arrange(precintDF2016, desc(precintDF2016$count)), 5)

precintDF2015 <- summarize(groupBy(nyc2015, nyc2015$`Violation Precinct`), count = n(nyc2015$`Violation Precinct`))
head(arrange(precintDF2015, desc(precintDF2015$count)), 5)
###Violation Precinct   count in 2017                                   
#1                  0 2072400
#2                 19  535671
#3                 14  352450
#4                  1  331810
#5                 18  306920

# Violation Precinct  count in 2016                                                  
#1                  0 1868655
#2                 19  554465
#3                 18  331704
#4                 14  324467
#5                  1  303850

###Violation Precinct  Count in 2015                                                 
#1                  0 1799170
#2                 19  598351
#3                 18  427510
#4                 14  409064
#5                  1  329009
# Violation is more in Precinct '0' - assuming that this is a valid precinct.
# the highest is almost 4 times the next high violating precinct


## 3.2 Top 5 Issuing Precincts

issuingPrecintDF2017 <- summarize(groupBy(nyc2017, nyc2017$`Issuer Precinct`), count = n(nyc2017$`Issuer Precinct`))
head(arrange(issuingPrecintDF2017, desc(issuingPrecintDF2017$count)), 5)

issuingPrecintDF2016 <- summarize(groupBy(nyc2016, nyc2016$`Issuer Precinct`), count = n(nyc2016$`Issuer Precinct`))
head(arrange(issuingPrecintDF2016, desc(issuingPrecintDF2016$count)), 5)

issuingPrecintDF2015 <- summarize(groupBy(nyc2015, nyc2015$`Issuer Precinct`), count = n(nyc2015$`Issuer Precinct`))
head(arrange(issuingPrecintDF2015, desc(issuingPrecintDF2015$count)), 5)
## Issuer Precinct   count in 2017                     
#1               0 2388479
#2              19  521513
#3              14  344977
#4               1  321170
#5              18  296553

## Issuer Precinct  count in 2016                                                       
#1               0 2140274
#2              19  540569
#3              18  323132
#4              14  315311
#5               1  295013

### IssuerPrecinct   Count in 2015                                                  
#1               0 2037745
#2              19  579998
#3              18  417329
#4              14  392922
#5               1  318778
## Q4. Find the violation code frequency across 3 precincts which have issued the most number of tickets ##

top3Precinct2017 <- filter(nyc2017, (nyc2017$`Violation Precinct` %in% c(0,19,14)))
nrow(top3Precinct2017)

topViolationCodes2017 <- summarize(groupBy(top3Precinct2017, top3Precinct2017$`Violation Code`), count=n(top3Precinct2017$`Violation Code`))
head(arrange(topViolationCodes2017, desc(topViolationCodes2017$count)))

top3Precinct2016 <- filter(nyc2016, (nyc2016$`Violation Precinct` %in% c(0,19,18)))
nrow(top3Precinct2016)

topViolationCodes2016 <- summarize(groupBy(top3Precinct2016, top3Precinct2016$`Violation Code`), count=n(top3Precinct2016$`Violation Code`))
head(arrange(topViolationCodes2016, desc(topViolationCodes2016$count)))

top3Precinct2015 <- filter(nyc2015, (nyc2015$`Violation Precinct` %in% c(0,9,18)))
nrow(top3Precinct2015)

topViolationCodes2015 <- summarize(groupBy(top3Precinct2015, top3Precinct2015$`Violation Code`), count=n(top3Precinct2015$`Violation Code`))
head(arrange(topViolationCodes2015, desc(topViolationCodes2015$count)))

###Violation Code   count in 2017                   
#1             36 1400614
#2              7  516389
#3              5  145643
#4             14  135744
#5             46  107425
#6             38   80954

##Violation Code  count in 2016                                                        
#1             36 1253511
#2              7  492470
#3             14  166281
#4              5  112377
#5             46   95122
#6             38   94266

##Violation Code  count in 2015
#1             36 839197
#2              7 719748
#3              5 224518
#4             14 150329
#5             69  65482
#6             38  44150
###### Observation:
## Violation code 36 in 2017, 2016 and 2015 are more prevalent in the most violated precinct.
## 7 are also one of the highest violation code across the years.


### Lets check the distribution of the violation code across these 3 precincts

vioPrecinctDF2017 <- summarize(groupBy(top3Precinct2017, top3Precinct2017$`Violation Precinct`, top3Precinct2017$`Violation Code`), count=n(top3Precinct2017$`Violation Code`))
collect(arrange(vioPrecinctDF2017, desc(vioPrecinctDF2017$count)))

vioPrecinctDF2016 <- summarize(groupBy(top3Precinct2016, top3Precinct2016$`Violation Precinct`, top3Precinct2016$`Violation Code`), count=n(top3Precinct2016$`Violation Code`))
collect(arrange(vioPrecinctDF2016, desc(vioPrecinctDF2016$count)))

vioPrecinctDF2015 <- summarize(groupBy(top3Precinct2015, top3Precinct2015$`Violation Precinct`, top3Precinct2015$`Violation Code`), count=n(top3Precinct2015$`Violation Code`))
collect(arrange(vioPrecinctDF2015, desc(vioPrecinctDF2015$count)))
###### Observation:
## The violation code is not prevelant across the precincts
## Most of the numbers are drive by precinct 0.
## for Violation Code 14, it was given in Precinct in 14 and 19.
## There is considerable violation.
######

## Q5. You’d want to find out the properties of parking violations across different times of the day:
##The Violation Time field is specified in a strange format. Find a way to make this into a time attribute that you can use to divide into groups.
##Find a way to deal with missing values, if any.
##Divide 24 hours into 6 equal discrete bins of time. The intervals you choose are at your discretion. For each of these groups, find the 3 most commonly ##occurring violations.
##Now, try another direction. For the 3 most commonly occurring violation codes, find the most common times of day (in terms of the bins from the previous part).

#######################For 2017, 2016, 2105########################################################
########## THE SAME CODE WAS EXECUTED FOR ALL THE THREE YEARS TO AVOID CLUTTERING OF THE FILE #####
###################################################################################################

createOrReplaceTempView(nyc2017, "TempNYC2017")
newDF <- SparkR::sql("select TempNYC2017.`Violation Time` as Time, TempNYC2017.`Violation Code` as Code from TempNYC2017")

#head(newDF, 50)

Vdf <- mutate(newDF, VHour = substr(newDF$Time,1, 2), VMin = substr(newDF$Time, 3,4), VHalf = substr(newDF$Time, 5,5))

Vdf1 <- transform(Vdf, VHour = ifelse(Vdf$VHalf == 'P' & Vdf$VHour < 12, Vdf$VHour+12, Vdf$VHour))
Vdf2017 <- transform(Vdf1, VHour = cast(Vdf1$VHour, "integer"), VMin = cast(Vdf1$VMin, "integer"))

nrow(filter(Vdf2017, Vdf2017$VHour > 23))

###################### Same above code was run for 2016 & 2015 as well ####################
###########################################################################################
### Observation:
# There are atleast some 99 records which has invalid data in 2017
# THere are atleast some 217 records which has invalid data in 2016
# There are atleast some 143 records which has invalid data in 2015

GByHour2017 <- summarize(groupBy(Vdf2017, Vdf2017$VHour), count = n(Vdf2017$VHour))

createOrReplaceTempView(Vdf2017, "VdfT2017")

BinVdf2017 <- SparkR::sql("select VdfT2017.Time, VdfT2017.Code, VdfT2017.VHour, VdfT2017.VMin, VdfT2017.VHalf, 
                      CASE WHEN VHour > 23 THEN 'Invalid'
                      WHEN VHour < 4 THEN '0-3'
                      WHEN VHour >=4 AND VHour <8 THEN '4-7'
                      WHEN VHour >=8 and VHour <12 THEN '8-11'
                      WHEN VHour >=12 and VHour <16 THEN '12-15'
                      WHEN VHour >=16 and VHour <20 THEN '16-19'
                      ELSE '20-23' END as Bin from VdfT2017")

#head(BinVdf2017)

BinnedVio2017 <- summarize(groupBy(BinVdf2017, BinVdf2017$Bin, BinVdf2017$Code), count=n(BinVdf2017$Code))

#head(BinnedVio2017)

#head(arrange(BinnedVio2017, asc(BinnedVio2017$Bin), desc(BinnedVio2017$count)))

createOrReplaceTempView(BinnedVio2017, "BinnedVioTable2017")

top3Bin2017 <- SparkR::sql("select bin, code, 
                       count from (select bin, code, count, dense_rank() over 
                       (partition by bin order by count desc) as rank from BinnedVioTable2017) tmp where rank <=3")

# 7 bins including invalid
# Top 3 violation codes for each bins are provided here
head(top3Bin2017,21)            

################# 2017 #####################
#bin code   count                                                         
#1    12-15   36  588395
#2    12-15   38  462859
#3    12-15   37  337096
#4     8-11   21 1182691
#5     8-11   36  751422
#6     8-11   38  346518
#7    16-19   38  203232
#8    16-19   37  145784
#9    16-19   14  144749
#10     0-3   21   73160
#11     0-3   40   45960
#12     0-3   14   29313
#13 Invalid   21      35
#14 Invalid   46      12
#15 Invalid   40      10
#16     4-7   14  141276
#17     4-7   21  119469
#18     4-7   40  112187
#19   20-23    7   65593
#20   20-23   38   47029
#21   20-23   14   44781
########################################
############### For 2016 ###############
#bin code   count                                                         
#1    12-15   36  545717
#2    12-15   38  488363
#3    12-15   37  383379
#4     8-11   21 1209244
#5     8-11   36  586791
#6     8-11   38  388099
#7    16-19   38  211267
#8    16-19   37  161655
#9    16-19   14  134977
#10     0-3   21   67798
#11     0-3   40   37261
#12     0-3   78   29473
#13 Invalid   46      50
#14 Invalid   21      42
#15 Invalid   14      19
#16     4-7   14  140111
#17     4-7   21  114029
#18     4-7   40   91693
#19   20-23    7   60965
#20   20-23   38   53433
#21   20-23   40   45226
###################################
########### For 2015 ##############
#bin code   count                                                         
#1    12-15   38  609616
#2    12-15   37  446482
#3    12-15   36  357310
#4     8-11   21 1291540
#5     8-11   38  480358
#6     8-11   36  396838
#7    16-19   38  258838
#8    16-19   37  187186
#9    16-19    7  182347
#10     0-3   21   69557
#11     0-3   40   40334
#12     0-3   78   37575
#13 Invalid   21      34
#14 Invalid   46      31
#15 Invalid   14      14
#16     4-7   14  143264
#17     4-7   21  118316
#18     4-7   40   98135
#19   20-23    7   89813
#20   20-23   38   66023
#21   20-23   40   49931
################################

###################################For 2017, 2016 & 2015#################3##########################
#From the earlier problem solving - 2017:::, its clear that 21,36 and 38 are the most violated code
#From the earlier problem solving - 2016:::, its clear that 21,36 and 38 are the most violated code
#From the earlier problem solving -- 2015, its clear that 21,14 and 38 are the most violated code

top3ViolationCodes <- filter(BinVdf2017, BinVdf2017$Code %in% c(21,14,38))

ViolationCodeSummary <- summarize(groupBy(top3ViolationCodes, top3ViolationCodes$Code, top3ViolationCodes$Bin), count=n(top3ViolationCodes$Bin))

createOrReplaceTempView(ViolationCodeSummary, "ViolationCodeSummaryT")

top3VioCount <- SparkR::sql("select bin, code, count from (select bin, code, count, dense_rank() over 
                            (partition by code order by count desc) as rank from ViolationCodeSummaryT) tmp where rank <=1 order by code")

head(top3VioCount)

#### bin code   count
#1  8-11   21 1182691
#2  8-11   36  751422
#3 12-15   38  462859
##################### Same code was run for 2016 & got these values #################

#bin code   count                                                            
#1  8-11   21 1209244
#2  8-11   36  586791
#3 12-15   38  488363

#########Same is run for 2015 and values returned are ################################

##bin code   count                                                            
#1  8-11   14  317009
#2  8-11   21 1291540
#3 12-15   38  609616
######################################################################################
## Q6. Let’s try and find some seasonality in this data
##First, divide the year into some number of seasons, and find frequencies of tickets for each season.
##Then, find the 3 most common violations for each of these season.
###################################For Year 2017#############################
YearDF2017 <- SparkR::sql("select `Violation Code` as code, `Issue Date` as IDate from TempNYC2017")

#head(YearDF2017)

YearDF2017 <- mutate(YearDF2017, INewDate = to_date(YearDF2017$IDate, 'MM/dd/yyyy'))
YearDF2017 <- mutate(YearDF2017, IWeek = weekofyear(YearDF2017$INewDate))


#nrow(filter(YearDF2017, "INewDate < '2017-01-01'"))
createOrReplaceTempView(YearDF2017, "YearDFT")

SeasonDF <- SparkR::sql("select YearDFT.code, YearDFT.INewDate, CASE WHEN IWeek <=13 THEN 'Q1' 
                        WHEN IWeek > 13 AND IWeek < 27 THEN 'Q2' 
                        WHEN IWeek >=27 AND IWeek < 40 THEN 'Q3'
                        ELSE 'Q4' END as Quarters from YearDFT")

#head(SeasonDF)

TicketsPerSeason <- summarize(groupBy(SeasonDF, SeasonDF$Quarters), count=n(SeasonDF$code))
head(TicketsPerSeason)


##################################For 2016######################################
YearDF2016 <- SparkR::sql("select `Violation Code` as code, `Issue Date` as IDate from TempNYC2016")

#head(YearDF2016)

YearDF2016 <- mutate(YearDF2016, INewDate = to_date(YearDF2016$IDate, 'MM/dd/yyyy'))
YearDF2016 <- mutate(YearDF2016, IWeek = weekofyear(YearDF2016$INewDate))


#nrow(filter(YearDF2016 ,"INewDate < '2016-01-01'"))
createOrReplaceTempView(YearDF2016,"YearDFT")

SeasonDF <- SparkR::sql("select YearDFT.code, YearDFT.INewDate, CASE WHEN IWeek <=13 THEN 'Q1' 
                        WHEN IWeek > 13 AND IWeek < 27 THEN 'Q2' 
                        WHEN IWeek >=27 AND IWeek < 40 THEN 'Q3'
                        ELSE 'Q4' END as Quarters from YearDFT")

#head(SeasonDF)

TicketsPerSeason <- summarize(groupBy(SeasonDF, SeasonDF$Quarters), count=n(SeasonDF$code))
head(TicketsPerSeason)

##################################For 2015######################################
YearDF2015 <- SparkR::sql("select `Violation Code` as code, `Issue Date` as IDate from TempNYC2015")

#head(YearDF2015)

YearDF2015 <- mutate(YearDF2015, INewDate = to_date(YearDF2015$IDate, 'MM/dd/yyyy'))
YearDF2015 <- mutate(YearDF2015, IWeek = weekofyear(YearDF2015$INewDate))


#nrow(filter(YearDF2015, "INewDate < '2015-01-01'"))
createOrReplaceTempView(YearDF2015, "YearDFT")

SeasonDF <- SparkR::sql("select YearDFT.code, YearDFT.INewDate, CASE WHEN IWeek <=13 THEN 'Q1' 
                        WHEN IWeek > 13 AND IWeek < 27 THEN 'Q2' 
                        WHEN IWeek >=27 AND IWeek < 40 THEN 'Q3'
                        ELSE 'Q4' END as Quarters from YearDFT")

#head(SeasonDF)

TicketsPerSeason <- summarize(groupBy(SeasonDF, SeasonDF$Quarters), count=n(SeasonDF$code))
head(TicketsPerSeason)

####Quarters   count in 2017                                                       
#1       Q2 3039386
#2       Q1 2703940
#3       Q3 2440363
#4       Q4 2619339

####Quarters   count in 2016                                                           
#1       Q2 2286025
#2       Q1 2710934
#3       Q3 2681882
#4       Q4 2948058

####Quarters   count in 2015
#1       Q2 3236607
#2       Q1 3135405
#3       Q3 2935987
#4       Q4 2501234
#############################################################################################################
ViolationsPerSeason <- summarize(groupBy(SeasonDF, SeasonDF$Quarters, SeasonDF$code), count=n(SeasonDF$code))
head(ViolationsPerSeason)

createOrReplaceTempView(ViolationsPerSeason, "ViolationPerSeasonT")

top3ViolationPerSeason <- SparkR::sql("select Quarters, code, count from (select Quarters, code, count, dense_rank() over 
                                      (partition by Quarters order by count desc) as rank from ViolationPerSeasonT) tmp where rank <=3")

#4 quarters/seasons , top3 in each

head(top3ViolationPerSeason, 12)

####Quarters code  count in 2017                                                         
#1        Q2   21 428529
#2        Q2   36 369902
#3        Q2   38 269988
#4        Q1   21 376325
#5        Q1   36 348240
#6        Q1   38 291129
#7        Q3   21 378575
#8        Q3   38 241600
#9        Q3   36 239879
#10       Q4   36 442593
#11       Q4   21 345159
#12       Q4   38 259587

#######################################For YearDF2016#########################################
YearDF2016 <- SparkR::sql("select `Violation Code` as code, `Issue Date` as IDate from TempNYC2016")

#head(YearDF2016)

YearDF2016 <- mutate(YearDF2016, INewDate = to_date(YearDF2016$IDate, 'MM/dd/yyyy'))
YearDF2016 <- mutate(YearDF2016, IWeek = weekofyear(YearDF2016$INewDate))


#nrow(filter(YearDF2016, "INewDate < '2016-01-01'"))
createOrReplaceTempView(YearDF2016, "YearDFT")

SeasonDF <- SparkR::sql("select YearDFT.code, YearDFT.INewDate, CASE WHEN IWeek <=13 THEN 'Q1' 
                        WHEN IWeek > 13 AND IWeek < 27 THEN 'Q2' 
                        WHEN IWeek >=27 AND IWeek < 40 THEN 'Q3'
                        ELSE 'Q4' END as Quarters from YearDFT")

#head(SeasonDF)

TicketsPerSeason <- summarize(groupBy(SeasonDF, SeasonDF$Quarters), count=n(SeasonDF$code))
#head(TicketsPerSeason)

ViolationsPerSeason <- summarize(groupBy(SeasonDF, SeasonDF$Quarters, SeasonDF$code), count=n(SeasonDF$code))
#head(ViolationsPerSeason)

createOrReplaceTempView(ViolationsPerSeason, "ViolationPerSeasonT")

top3ViolationPerSeason <- SparkR::sql("select Quarters, code, count from (select Quarters, code, count, dense_rank() over 
                                      (partition by Quarters order by count desc) as rank from ViolationPerSeasonT) tmp where rank <=3")

head(top3ViolationPerSeason, 12)

## Quarters code  count in 2016                                                      
#1        Q2   21 305195
#2        Q2   36 267308
#3        Q2   38 230520
#4        Q1   21 357407
#5        Q1   36 347934
#6        Q1   38 312938
#7        Q3   21    248
#8        Q3   46    208
#9        Q3   40     91
#10       Q4   71   5574
#11       Q4   38   3615
#12       Q4    7   3582


#######################################For YearDF2015#########################################
YearDF2015 <- SparkR::sql("select `Violation Code` as code, `Issue Date` as IDate from TempNYC2015")

head(YearDF2015)

YearDF2015 <- mutate(YearDF2015, INewDate = to_date(YearDF2015$IDate, 'MM/dd/yyyy'))
YearDF2015 <- mutate(YearDF2015, IWeek = weekofyear(YearDF2015$INewDate))


nrow(filter(YearDF2015, "INewDate < '2015-01-01'"))
createOrReplaceTempView(YearDF2015, "YearDFT")

SeasonDF <- SparkR::sql("select YearDFT.code, YearDFT.INewDate, CASE WHEN IWeek <=13 THEN 'Q1' 
                        WHEN IWeek > 13 AND IWeek < 27 THEN 'Q2' 
                        WHEN IWeek >=27 AND IWeek < 40 THEN 'Q3'
                        ELSE 'Q4' END as Quarters from YearDFT")

head(SeasonDF)

TicketsPerSeason <- summarize(groupBy(SeasonDF, SeasonDF$Quarters), count=n(SeasonDF$code))
head(TicketsPerSeason)

ViolationsPerSeason <- summarize(groupBy(SeasonDF, SeasonDF$Quarters, SeasonDF$code), count=n(SeasonDF$code))
head(ViolationsPerSeason)

createOrReplaceTempView(ViolationsPerSeason, "ViolationPerSeasonT")

top3ViolationPerSeason <- SparkR::sql("select Quarters, code, count from (select Quarters, code, count, dense_rank() over 
                                      (partition by Quarters order by count desc) as rank from ViolationPerSeasonT) tmp where rank <=3")

head(top3ViolationPerSeason, 12)
## Quarters code  count in 2015
#1        Q2   21 462435
#2        Q2   38 341047
#3        Q2   36 260632
#4        Q1   38 337891
#5        Q1   21 284974
#6        Q1   14 218766
#7        Q3   21 407265
#8        Q3   38 352766
#9        Q3   14 236939
#10       Q4   21 346940
#11       Q4   38 292882
#12       Q4   14 209587
###############################################################################
## Q7.1 The fines collected from all the parking violation constitute a revenue source for the NYC police department. Let’s take an example of estimating that ##for the 3 most commonly occurring codes.
##Find total occurrences of the 3 most common violation codes
##Then, search the internet for NYC parking violation code fines. You will find a website (on the nyc.gov URL) that lists these fines. They’re divided into two ##categories, one for the highest-density locations of the city, the other for the rest of the city. For simplicity, take an average of the two.
##Using this information, find the total amount collected for all of the fines. State the code which has the highest total collection.
##What can you intuitively infer from these findings?

####Top3 violation codes ####
top3ViolationCodes <- arrange(violationDF2017, desc(violationDF2017$count))
tab <- head(top3ViolationCodes, 3)

###Violation Code   count in 2017                                                     
#1             21 1528588
#2             36 1400614
#3             38 1062304

##### Fine by NYC #####
### Code : 21 :::: $55 ###
### Code : 36 :::: $50 ###
### Code : 38 :::: $50 ###

tab$Cost[1] <- tab$count[1] * 55
tab$Cost[2] <- tab$count[2] * 50
tab$Cost[3] <- tab$count[3] * 50

tab

################For 2016################
top3ViolationCodes <- arrange(violationDF2016, desc(violationDF2016$count))
tab <- head(top3ViolationCodes, 3)

###Violation Code   count in 2016

##### Fine by NYC #####
### Code : 21 :::: $55 ###
### Code : 36 :::: $50 ###
### Code : 38 :::: $50 ###

tab$Cost[1] <- tab$count[1] * 55
tab$Cost[2] <- tab$count[2] * 50
tab$Cost[3] <- tab$count[3] * 50

tab


################For 2015################
top3ViolationCodes <- arrange(violationDF2015, desc(violationDF2015$count))
tab <- head(top3ViolationCodes, 3)

###Violation Code   count in 2015

##### Fine by NYC #####
### Code : 21 :::: $55 ###
### Code : 38 :::: $50 ###
### Code : 14 :::: $115 ###

tab$Cost[1] <- tab$count[1] * 55
tab$Cost[2] <- tab$count[2] * 50
tab$Cost[3] <- tab$count[3] * 115

tab
#####For 2017#########
###Violation Code   count     Cost
#1             21 1528588 84072340
#2             36 1400614 70030700
#3             38 1062304 53115200

#####For 2016#########
#Violation Code   count     Cost
#1             21 1531587 84237285
#2             36 1253512 62675600
#3             38 1143696 57184800

#####For 2015###########
#Violation Code   count     Cost
#1             21 1630912 89700160
#2             38 1418627 70931350
#3             14  988469 113673935

######Inference for 2017, 2016 and 2015: ##############
###### The violation of parking vehicles(21) in a non-parking zone is more prevalent ######
###### This is also the most popular reason for the ticket & earnings by the NYC ######
########################################


