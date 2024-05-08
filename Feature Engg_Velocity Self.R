library(dplyr)
library(lubridate)
library(data.table)

df <- read.csv('ApplicationsFrivLRemo.csv')
#df <- df[1:10000,]
#summary(df)
#raw_data <- df
#rm(raw_data)

# Creat groups

## nameDOB

df$nameDOB <- paste(as.character(df$firstname), as.character(df$lastname), as.character(df$dob), sep = '_')


## full_address


df$fulladdress <- paste(as.character(df$address), as.character(df$zip5), sep = '_')

names(df)[1] = paste('record')

#adjusting columns order

col <- c("record", "ssn", "fulladdress", "nameDOB", "homephone", "date" , 
         "fraud_label", "firstname", "lastname")
df <- df[, col]
df$date <- ymd(df$date)
rm(col)


#combinations <- list(c('ssn','fulladdress'),c('ssn','nameDOB'),c('ssn','homephone'), c('ssn','firstname'),c('ssn','lastname'), c('nameDOB', 'homephone'), c('fulladdress', 'homephone'), c('fulladdress', 'nameDOB'))


#entities <- c('ssn','fulladdress', 'nameDOB', 'homephone')

combinations <- combn(c('ssn', 'fulladdress', 'firstname', 'lastname', 'nameDOB', 'homephone'), m = 2, simplify = FALSE)
entities <- c('ssn', 'fulladdress', 'nameDOB', 'homephone')


# Velocity

timeWinJoin <- function(dt, n, byVar){
  dt1 = dt
  # Generate duplicated copy for the columns we'll join on
  # as they'll disappear after running the data.table join method,
  # n is the length of the time window
  dt1$join_ts1 = dt1$date
  dt1$join_ts2 = dt1$date + n
  dt1$join_rec = dt1$record
  # The join conditions below are equivalent to what in the sqldf code
  keys = c(byVar, 'join_ts1<=date', 'join_ts2>=date', 'record<=record')
  dt2 = dt1[dt, on=keys, allow.cartesian=T]
  rm(dt)
  rm(dt1)
  dt2 <- dt2 %>%
    select(record)
  return(dt2)
}

setDT(df)

for (i in c(0,1,3,7,14,30)){
  for (j in c('ssn','fulladdress', 'nameDOB', 'homephone')){
    assign(paste0("dt",j,i), timeWinJoin(df, i, j))
  }
}
dtssn0 <- dtssn0[,.(count = .N), by=record ]
dtssn1 <- dtssn1[,.(count = .N), by=record ]
dtssn3 <- dtssn3[,.(count = .N), by=record ]
dtssn7 <- dtssn7[,.(count = .N), by=record ]
dtssn14 <- dtssn7[,.(count = .N), by=record ]


dtfulladdress0 <- dtfulladdress0[,.(count = .N), by=record ]
dtfulladdress1 <- dtfulladdress1[,.(count = .N), by=record ]
dtfulladdress3 <- dtfulladdress3[,.(count = .N), by=record ]
dtfulladdress7 <- dtfulladdress7[,.(count = .N), by=record ]
dtfulladdress14 <- dtfulladdress7[,.(count = .N), by=record ]


dtnameDOB0 <- dtnameDOB0[,.(count = .N), by=record ]
dtnameDOB1 <- dtnameDOB1[,.(count = .N), by=record ]
dtnameDOB3 <- dtnameDOB3[,.(count = .N), by=record ]
dtnameDOB7 <- dtnameDOB7[,.(count = .N), by=record ]
dtnameDOB14 <- dtnameDOB7[,.(count = .N), by=record ]


dthomephone0 <- dthomephone0[,.(count = .N), by=record ]
dthomephone1 <- dthomephone1[,.(count = .N), by=record ]
dthomephone3 <- dthomephone3[,.(count = .N), by=record ]
dthomephone7 <- dthomephone7[,.(count = .N), by=record ]


dtssn30 <- timeWinJoin(df, 30, 'ssn')
dtssn30 <- dtssn30[,.(count = .N), by=record ]


dtfulladdress30 <- timeWinJoin(df, 30, 'fulladdress')
dtfulladdress30 <- dtfulladdress30[,.(count = .N), by=record ]

dtnameDOB30 <- timeWinJoin(df, 30, 'nameDOB')
dtnameDOB30 <- dtnameDOB30[,.(count = .N), by=record ]

alldata <- data.table(df[,1], 
                      dtssn0[,2], dtssn1[,2], dtssn3[,2], dtssn7[,2], dtssn14[,2], dtssn30[,2],
                      dtfulladdress0[,2], dtfulladdress1[,2], dtfulladdress3[,2], dtfulladdress7[,2], dtfulladdress14[,2], dtfulladdress30[,2],
                      dtnameDOB0[,2], dtnameDOB1[,2], dtnameDOB3[,2], dtnameDOB7[,2], dtnameDOB14[,2], dtnameDOB30[,2],
                      dthomephone0[,2], dthomephone1[,2], dthomephone3[,2])

rm(dtssn0, dtssn1, dtssn3, dtssn7, dtssn14, dtssn30)
rm(dtfulladdress0, dtfulladdress1, dtfulladdress3, dtfulladdress7, dtfulladdress14, dtfulladdress30)
rm(dtnameDOB0, dtnameDOB1, dtnameDOB3, dtnameDOB7, dtnameDOB14, dtnameDOB30)
rm(dthomephone0, dthomephone1, dthomephone3)

colnames(alldata) <- c('record', 'ssn0', 'ssn1', 'ssn3', 'ssn7', 'ssn14', 'ssn30',
                       'fulladdress0', 'fulladdress1', 'fulladdress3', 'fulladdress7', 'fulladdress14', 'fulladdress30',
                       'dtnameDOB0', 'dtnameDOB1', 'dtnameDOB3', 'dtnameDOB7', 'dtnameDOB14', 'dtnameDOB30',
                       'homephone0', 'homephone1', 'homephone3')

for (i in c(0,1,3,7,14,30)){
  for (j in list(c('ssn','fulladdress'),c('ssn','nameDOB'),c('ssn','homephone'), c('ssn','firstname'),c('ssn','lastname'),
                 c('nameDOB', 'homephone'), c('fulladdress', 'homephone'), c('fulladdress', 'nameDOB'))){
    assign(paste0("dt",j[1],j[2],i), timeWinJoin(df, i, j))
  }
}

dtssnfulladdress0 <- dtssnfulladdress0[,.(count = .N), by=record ]
dtssnfulladdress1 <- dtssnfulladdress1[,.(count = .N), by=record ]
dtssnfulladdress3 <- dtssnfulladdress3[,.(count = .N), by=record ]
dtssnfulladdress7 <- dtssnfulladdress7[,.(count = .N), by=record ]
dtssnfulladdress14 <- dtssnfulladdress14[,.(count = .N), by=record ]
dtssnfulladdress30 <- dtssnfulladdress30[,.(count = .N), by=record ]


dtssnnameDOB0 <- dtssnnameDOB0[,.(count = .N), by=record ]
dtssnnameDOB1 <- dtssnnameDOB1[,.(count = .N), by=record ]
dtssnnameDOB3 <- dtssnnameDOB3[,.(count = .N), by=record ]
dtssnnameDOB7 <- dtssnnameDOB7[,.(count = .N), by=record ]
dtssnnameDOB14 <- dtssnnameDOB14[,.(count = .N), by=record ]
dtssnnameDOB30 <- dtssnnameDOB30[,.(count = .N), by=record ]

dtssnhomephone0 <- dtssnhomephone0[,.(count = .N), by=record ]
dtssnhomephone1 <- dtssnhomephone1[,.(count = .N), by=record ]
dtssnhomephone3 <- dtssnhomephone3[,.(count = .N), by=record ]
dtssnhomephone7 <- dtssnhomephone7[,.(count = .N), by=record ]
dtssnhomephone14 <- dtssnhomephone14[,.(count = .N), by=record ]
dtssnhomephone30 <- dtssnhomephone30[,.(count = .N), by=record ]

dtssnfirstname0 <- dtssnfirstname0[,.(count = .N), by=record ]
dtssnfirstname1 <- dtssnfirstname1[,.(count = .N), by=record ]
dtssnfirstname3 <- dtssnfirstname3[,.(count = .N), by=record ]
dtssnfirstname7 <- dtssnfirstname7[,.(count = .N), by=record ]
dtssnfirstname14 <- dtssnfirstname14[,.(count = .N), by=record ]
dtssnfirstname30 <- dtssnfirstname30[,.(count = .N), by=record ]

dtssnlastname0 <- dtssnlastname0[,.(count = .N), by=record ]
dtssnlastname1 <- dtssnlastname1[,.(count = .N), by=record ]
dtssnlastname3 <- dtssnlastname3[,.(count = .N), by=record ]
dtssnlastname7 <- dtssnlastname7[,.(count = .N), by=record ]
dtssnlastname14 <- dtssnlastname14[,.(count = .N), by=record ]
dtssnlastname30 <- dtssnlastname30[,.(count = .N), by=record ]

dtnameDOBhomephone0 <- dtnameDOBhomephone0[,.(count = .N), by=record ]
dtnameDOBhomephone1 <- dtnameDOBhomephone1[,.(count = .N), by=record ]
dtnameDOBhomephone3 <- dtnameDOBhomephone3[,.(count = .N), by=record ]
dtnameDOBhomephone7 <- dtnameDOBhomephone7[,.(count = .N), by=record ]
dtnameDOBhomephone14 <- dtnameDOBhomephone14[,.(count = .N), by=record ]
dtnameDOBhomephone30 <- dtnameDOBhomephone30[,.(count = .N), by=record ]

dtfulladdresshomephone0 <- dtfulladdresshomephone0[,.(count = .N), by=record ]
dtfulladdresshomephone1 <- dtfulladdresshomephone1[,.(count = .N), by=record ]
dtfulladdresshomephone3 <- dtfulladdresshomephone3[,.(count = .N), by=record ]
dtfulladdresshomephone7 <- dtfulladdresshomephone7[,.(count = .N), by=record ]
dtfulladdresshomephone14 <- dtfulladdresshomephone14[,.(count = .N), by=record ]
dtfulladdresshomephone30 <- dtfulladdresshomephone30[,.(count = .N), by=record ]

dtfulladdressnameDOB0 <- dtfulladdressnameDOB0[,.(count = .N), by=record ]
dtfulladdressnameDOB1 <- dtfulladdressnameDOB1[,.(count = .N), by=record ]
dtfulladdressnameDOB3 <- dtfulladdressnameDOB3[,.(count = .N), by=record ]
dtfulladdressnameDOB7 <- dtfulladdressnameDOB7[,.(count = .N), by=record ]
dtfulladdressnameDOB14 <- dtfulladdressnameDOB14[,.(count = .N), by=record ]
dtfulladdressnameDOB30 <- dtfulladdressnameDOB30[,.(count = .N), by=record ]

alldata_2 <- data.table(df[,1], 
                        dtssnfulladdress0[,2], dtssnfulladdress1[,2], dtssnfulladdress3[,2], dtssnfulladdress7[,2], dtssnfulladdress14[,2], dtssnfulladdress30[,2],
                        dtssnnameDOB0[,2], dtssnnameDOB1[,2], dtssnnameDOB3[,2], dtssnnameDOB7[,2], dtssnnameDOB14[,2], dtssnnameDOB30[,2],
                        dtssnhomephone0[,2], dtssnhomephone1[,2], dtssnhomephone3[,2], dtssnhomephone7[,2], dtssnhomephone14[,2], dtssnhomephone30[,2],
                        dtssnfirstname0[,2], dtssnfirstname1[,2], dtssnfirstname3[,2], dtssnfirstname7[,2], dtssnfirstname14[,2], dtssnfirstname30[,2],
                        dtssnlastname0[,2], dtssnlastname1[,2], dtssnlastname3[,2], dtssnlastname7[,2], dtssnlastname14[,2], dtssnlastname30[,2],
                        dtnameDOBhomephone0[,2], dtnameDOBhomephone1[,2], dtnameDOBhomephone3[,2], dtnameDOBhomephone7[,2], dtnameDOBhomephone14[,2], dtnameDOBhomephone30[,2],
                        dtfulladdresshomephone0[,2], dtfulladdresshomephone1[,2], dtfulladdresshomephone3[,2], dtfulladdresshomephone7[,2], dtfulladdresshomephone14[,2], dtfulladdresshomephone30[,2],
                        dtfulladdressnameDOB0[,2], dtfulladdressnameDOB1[,2], dtfulladdressnameDOB3[,2], dtfulladdressnameDOB7[,2], dtfulladdressnameDOB14[,2], dtfulladdressnameDOB30[,2])

rm(dtssnfulladdress0, dtssnfulladdress1, dtssnfulladdress3, dtssnfulladdress7, dtssnfulladdress14, dtssnfulladdress30,
   dtssnnameDOB0, dtssnnameDOB1, dtssnnameDOB3, dtssnnameDOB7, dtssnnameDOB14, dtssnnameDOB30,
   dtssnhomephone0, dtssnhomephone1, dtssnhomephone3, dtssnhomephone7, dtssnhomephone14, dtssnhomephone30,
   dtssnfirstname0, dtssnfirstname1, dtssnfirstname3, dtssnfirstname7, dtssnfirstname14, dtssnfirstname30,
   dtssnlastname0, dtssnlastname1, dtssnlastname3, dtssnlastname7, dtssnlastname14, dtssnlastname30,
   dtnameDOBhomephone0, dtnameDOBhomephone1, dtnameDOBhomephone3, dtnameDOBhomephone7, dtnameDOBhomephone14, dtnameDOBhomephone30,
   dtfulladdresshomephone0, dtfulladdresshomephone1, dtfulladdresshomephone3, dtfulladdresshomephone7, dtfulladdresshomephone14, dtfulladdresshomephone30,
   dtfulladdressnameDOB0, dtfulladdressnameDOB1, dtfulladdressnameDOB3, dtfulladdressnameDOB7, dtfulladdressnameDOB14, dtfulladdressnameDOB30)

colnames(alldata_2) <- c('record', 'ssnfulladdress0', 'ssnfulladdress1', 'ssnfulladdress3', 'ssnfulladdress7', 'ssnfulladdress14', 'ssnfulladdress30',
                         'ssnnameDOB0', 'ssnnameDOB1', 'ssnnameDOB3', 'ssnnameDOB7', 'ssnnameDOB14', 'ssnnameDOB30',
                         'ssnhomephone0', 'ssnhomephone1', 'ssnhomephone3', 'ssnhomephone7', 'ssnhomephone14', 'ssnhomephone30',
                         'ssnfirstname0', 'ssnfirstname1', 'ssnfirstname3', 'ssnfirstname7', 'ssnfirstname14', 'ssnfirstname30',
                         'ssnlastname0', 'ssnlastname1', 'ssnlastname3', 'ssnlastname7', 'ssnlastname14', 'ssnlastname30',
                         'nameDOBhomephone0', 'nameDOBhomephone1', 'nameDOBhomephone3', 'nameDOBhomephone7', 'nameDOBhomephone14', 'nameDOBhomephone30',
                         'fulladdresshomephone0', 'fulladdresshomephone1', 'fulladdresshomephone3', 'fulladdresshomephone7', 'fulladdresshomephone14', 'fulladdresshomephone30',
                         'fulladdressnameDOB0', 'fulladdressnameDOB1', 'fulladdressnameDOB3', 'fulladdressnameDOB7', 'fulladdressnameDOB14', 'fulladdressnameDOB30')

all_data <- data.table(alldata, alldata_2)

write.csv(all_data, 'Velocity.csv')

colnames(all_data)

#temp <- all_data$record

#all_data <- subset(all_data, select = -record)

#write.csv(all_data, 'Velocity.csv')

#rm(all_data, alldata_2, alldata)


last_time <- read.csv('DataLast.csv')
last_time <- last_time %>%
  select(-X)

velocity <- read.csv('Velocity.csv')
velocity <- velocity %>%
  select(-X)


risk <- read.csv('DataRisk.csv')
risk <- risk %>%
  select(-X)

setDT(last_time)
setDT(risk)
setDT(velocity)
variables <- last_time[risk, on = 'record']
variables <- variables[velocity, on = 'record']

colnames(variables)

write.csv(variables, 'FinalNewVariables.csv')

variables <- read.csv('FinalNewVariables.csv')
