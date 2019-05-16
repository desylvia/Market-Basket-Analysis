library(data.table)

setwd("~/POS_Transmart/Store/City/Data")
store <- "PABELAN"

raw <- fread(paste(store,"2018.txt",sep = "_"))
raw <- raw[POS_GROUP_DESC=="CHECKOUT"]

#== Generate New Code
library(hashids)
h = hashid_settings(salt = 'transmart')

TR_DATE <- gsub("-|:| ","",raw$TRANSACTIONDATE)
TR_DATE <- as.numeric(as.character(TR_DATE))

library(pbapply)
pboptions(type = "txt")
newcode <- pbsapply(TR_DATE, function(x) encode(x,h))

raw <- raw[,NEWCODE:=paste0(TRX_ID,newcode)]
raw <- raw[order(TRX_ID),]
raw <- raw[order(TRANSACTIONDATE),]

write.table(raw,paste(store,"NEWCODE_2018.txt",sep = "_"),sep="|",row.names=FALSE)

rm(list=ls())
gc()

#*************************************************** Prepare the data (segmentation)
setwd("~/POS_Transmart/Store/City/Data")
store <- "PABELAN"

raw <- fread(paste(store,"NEWCODE_2018.txt",sep = "_"))

basket_size <- raw[,.(GROSS_SALES_Sum = sum(GROSS_SALES)),by=NEWCODE]
basket_size <- basket_size[order(-GROSS_SALES_Sum)]

seg1 <- basket_size[GROSS_SALES_Sum < 100000][,.(NEWCODE)] # <100k
seg2 <- basket_size[GROSS_SALES_Sum >= 100000 & GROSS_SALES_Sum <= 200000][,.(NEWCODE)] #100k - 200k
seg3 <- basket_size[GROSS_SALES_Sum > 200000 & GROSS_SALES_Sum <= 300000][,.(NEWCODE)] #200k - 300k
seg4 <- basket_size[GROSS_SALES_Sum > 300000 & GROSS_SALES_Sum <= 500000][,.(NEWCODE)] #300k - 500k
seg5 <- basket_size[GROSS_SALES_Sum > 500000][,.(NEWCODE)] # >500k

raw_seg1 <- raw[seg1, on="NEWCODE", nomatch=0]
raw_seg2 <- raw[seg2, on="NEWCODE", nomatch=0]
raw_seg3 <- raw[seg3, on="NEWCODE", nomatch=0]
raw_seg4 <- raw[seg4, on="NEWCODE", nomatch=0]
raw_seg5 <- raw[seg5, on="NEWCODE", nomatch=0]

setwd(paste("~/POS_Transmart/Store/City/Segmented",store,"Family", sep = "/"))
write.table(raw_seg1,paste(store,"SEG1_2018.txt",sep = "_"),sep="|",row.names=FALSE)
write.table(raw_seg2,paste(store,"SEG2_2018.txt",sep = "_"),sep="|",row.names=FALSE)
write.table(raw_seg3,paste(store,"SEG3_2018.txt",sep = "_"),sep="|",row.names=FALSE)
write.table(raw_seg4,paste(store,"SEG4_2018.txt",sep = "_"),sep="|",row.names=FALSE)
write.table(raw_seg5,paste(store,"SEG5_2018.txt",sep = "_"),sep="|",row.names=FALSE)

rm(list=ls())
gc()

#**************************************************** Prepare the data (transaction)
library(data.table)

setwd(paste("~/POS_Transmart/Store/City/Segmented",store,"Family", sep = "/"))
store <- "PABELAN"
seg <- "SEG5"

raw <- fread(paste(store,seg,"2018.txt",sep = "_"))

#== Get Family level
setwd("~/POS_Transmart/Item")
fam_items <- fread("DIM_ITEMS_FAMILY.txt")

raw <- raw[, SKU:=as.character(SKU)]

#Get Family level
fam_code <- sapply(raw$SKU, function(x) paste(substring(x,first = 1,last = 4)))
raw <- raw[,FAM_CODE:=fam_code]
raw_fam <- raw[fam_items, on="FAM_CODE", nomatch=0]

#Get fields for transaction only and save
raw_fam <- raw_fam[order(TRX_ID),]
raw_fam <- raw_fam[order(TRANSACTIONDATE),]
trx_raw <- raw_fam[,.(NEWCODE,FAMILY,TRX_ID,TRANSACTIONDATE,PRODUCT_NAME)]

setwd("~/POS_Transmart/Store/City/Segmented/Pabelan/Family/")
write.table(trx_raw,paste(store,seg,"TRX_2018.txt",sep = "_"),sep="|",row.names=FALSE)

rm(list=ls())
gc()

#****************************************************************************** MBA
library(arules)

setwd("~/POS_Transmart/Store/City/Segmented/Pabelan/Family/")
store <- "PABELAN"
seg <- "SEG5"

raw <- fread(paste(store,seg,"TRX_2018.txt",sep = "_"))
pab <- raw[complete.cases(raw),]
rm(raw)

#== Prepare data to be transaction data
basket <- split(x=pab[,FAMILY],f=pab[,NEWCODE])
basket <- lapply(basket,unique)
basket <- as(basket,"transactions")

#== Get and Write items frequency table
freq <- itemFrequency(basket)
freq_items <- data.table(Family=names(freq), Support=unname(freq))
freq_items <- freq_items[order(-Support)]

setwd("~/POS_Transmart/Store/City/Segmented/Pabelan/Family/Result")
write.csv(freq_items,paste(store,seg,"FREQ_Fam_2018.csv",sep = "_"), quote = FALSE, 
          row.names = FALSE)

#== Association rule mining (All Family)
confidence = 0.01
support = 0.004

rules <- apriori(basket, parameter = list(minlen=2,maxlen=2,supp=support,conf=confidence))
rules <- sort(rules, by="lift", decreasing = TRUE)
rules <- rules[!is.redundant(rules, measure = "confidence")]
rules_df <- as.data.table(DATAFRAME(rules))

#== Get P(A) and P(B)
rules_df[] <- lapply(rules_df, gsub, pattern="}", replacement="")
rules_df[] <- lapply(rules_df, gsub, pattern="\\{", replacement="")

#Get LHS support / P(A)
lhs <- unique(rules_df[,1])
tmp <- lapply(lhs, function(x) freq_items[Family %in% eval(x)])
LHS <- tmp[["LHS"]][["Family"]]
Supp_LHS <- tmp[["LHS"]][["Support"]]
supp_lhs <- as.data.table(cbind(LHS,Supp_LHS))
rm(lhs);rm(tmp);rm(LHS);rm(Supp_LHS)

#Get RHS support / P(B) -> expected confidence
rhs <- unique(rules_df[,2])
tmp <- lapply(rhs, function(x) freq_items[Family %in% eval(x)])
RHS <- tmp[["RHS"]][["Family"]]
Supp_RHS <- tmp[["RHS"]][["Support"]]
supp_rhs <- as.data.table(cbind(RHS,Supp_RHS))
rm(rhs);rm(tmp);rm(RHS);rm(Supp_RHS)

#Append all and reorder
rules_comp <- rules_df[supp_lhs, on="LHS", nomatch=0]
rules_comp <- rules_comp[supp_rhs, on="RHS", nomatch=0]
setcolorder(rules_comp, c("LHS","RHS","Supp_LHS","Supp_RHS","support","confidence","lift","count"))
colnames(rules_comp) <- c("LHS","RHS","P(A)","P(B)","Support_P(AB)","Confidence_P(B|A)","Lift","count")

#Convert to number and order by Lift
strTmp = c('P(A)','P(B)','Support_P(AB)','Confidence_P(B|A)','Lift','count')
rules_comp[, (strTmp) := lapply(.SD, as.numeric), .SDcols = strTmp]
rules_comp <- rules_comp[order(-Lift)]

#Save the result
setwd(paste("~/POS_Transmart/Store/City/Segmented",store,"Family/Result", sep = "/"))
str <- paste(paste(store,seg,"rules_comp_2018_supp",support,"conf",confidence,sep = "_"),
             "csv",sep = ".")
write.csv(rules_comp,str,row.names=FALSE)

rm(list=ls())
gc()
