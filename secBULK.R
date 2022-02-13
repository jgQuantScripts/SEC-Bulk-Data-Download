require("data.table");require("stringr");require("pbapply")
# bulk data download from SEC
# https://www.sec.gov/dera/data
# download 2021-quarterly data: https://www.sec.gov/dera/data/financial-statement-data-sets.html
Yq = c("2021q4", "2021q3", "2021q2","2021q1","2020q4", "2020q3", "2020q2","2020q1",
       "2019q4", "2019q3", "2019q2","2019q1","2018q4", "2018q3", "2018q2","2018q1")

url = paste0("https://www.sec.gov/files/dera/data/financial-statement-data-sets/",Yq,".zip")
dn = lapply(as.list(1:length(url)), function(ii){
  download.file(url[ii],destfile = paste0(Yq[ii],".zip"))
  dir.create(Yq[ii])
  unzip(paste0(Yq[ii],".zip"),exdir = paste0(Yq[ii]))
})

# contains all numeric variables in filings  
#num0 <- fread(paste0(getwd(),"/2021q4/num.txt"), sep="\t",fill = TRUE)
num = rbindlist(pblapply(as.list(Yq), function(x){
  num0 <- read.table(paste0(getwd(),"/",x,"/num.txt"),header = TRUE ,sep="\t")
  num0
}),use.names = TRUE, fill = TRUE)

# contains meta data/company info
# sub <- fread(paste0(getwd(),"/2021q4/sub.txt"), sep="\t",fill = TRUE) 
sub = rbindlist(pblapply(as.list(Yq), function(x){
  #sub0 <- read.table(paste0(getwd(),"/",x,"/sub.txt"),header = TRUE ,sep="\t")
  sub0 <- fread(paste0(getwd(),"/",x,"/sub.txt"), sep="\t",fill = TRUE) 
  sub0
}),use.names = TRUE, fill = TRUE)

# ********************************************************************************************************************
#                       SEC can't just place tickers! (as they do change I guess)
# ********************************************************************************************************************
getTickers = function(sub){
  gt = sub[,c("cik","name","instance")]
  #gt = gt[str_detect(gt$instance, pattern = "\\d{6}\\_htm.xml"),]
  gt = gt[str_detect(gt$instance, pattern = "-\\d{6}"),]
  gt$ticker = str_sub(gt$instance, 1, 5)
  gt = gt[,c("cik","name","ticker")]
  gt <- unique(gt)
  gt = gt[str_detect(gt$ticker,"\\-"),] # only those with hyphens have tickers
  # regex to delete every character after the hyphen
  gt$ticker <- toupper(gsub("\\-.*","",gt$ticker))
  gt = unique(gt)
  gt = gt[order(gt$ticker, decreasing = FALSE),]
  as.data.frame(gt)
}
cikLookUp = getTickers(sub=sub)
cikLookUp = cikLookUp[!str_detect(cikLookUp$ticker, "[0-9]"),] # removes tickers with numbers
# ********************************************************************************************************************
#                       function to extract revenues for a certain company
# ********************************************************************************************************************
getRev = function(ticker){
  # lookup cik for company
  CIK = cikLookUp[which(cikLookUp$ticker == ticker),]$cik %>% as.numeric
  # lookup adsh (edgar accession number)
  FILINGS = subset(sub, sub$cik == CIK)
  # get unique adsh numbers
  ADSH = unique(FILINGS$adsh)
  # now we can extract revenues for the company
  tmp = rbindlist(lapply(as.list(ADSH), function(x){
    NUM = subset(num, num$adsh == x)
    # add filing date
    NUM$fileDate = subset(sub, sub$adsh == x)$filed
    # return data
    NUM
  }), use.names = TRUE, fill = TRUE)
  # quarterly data have qtrs == 1 , nine months ending in... have qtrs == 3
  tmp = subset(tmp,tmp$qtrs == 1) 
  # extract revenues
  tmp = subset(tmp, tmp$tag == "Revenues" | tmp$tag == "RevenueFromContractWithCustomerExcludingAssessedTax")
  tmp = tmp[,c("ddate","value","fileDate")]
  tmp$ddate <- as.Date(as.character(tmp$ddate), format="%Y%m%d")
  tmp$fileDate <- as.Date(as.character(tmp$fileDate), format="%Y%m%d")
  tmp$Ticker  <- ticker
  tmp <- tmp[order(tmp$ddate, decreasing = FALSE),]
  tmp = do.call(rbind,lapply(as.list(unique(tmp$fileDate)), function(XX){
    df = subset(tmp, tmp$fileDate == XX)
    df <- cbind(paste(df$ddate[1]), df$value[1], paste(df$ddate[2]), df$value[2], 
                 round(df$value[2]/df$value[1]-1,4), paste(df$fileDate[1]), df$Ticker[1])
    colnames(df) <-c("qYOY","qRevYOY","mrq","mrqRev","YOYchange","filingDate","Ticker")
    df <- as.data.frame(df)
    df
  }))
  
  tmp$qRevYOY <- as.numeric(tmp$qRevYOY)
  tmp$mrqRev <- as.numeric(tmp$mrqRev)
  tmp$YOYchange <- as.numeric(tmp$YOYchange)
  tmp
}


REV = getRev(ticker= "TSLA")
getRev(ticker= "O")
getRev(ticker= "AFRM")
getRev(ticker= "AAPL")
# ********************************************************************************************************************
#                        get Revenue from all tickers in this quarter
# ********************************************************************************************************************
tickers = unique(cikLookUp$ticker)

REV = rbindlist(pblapply(as.list(tickers), function(x){
  tmp = try(getRev(ticker = x), silent = TRUE)
  if(!inherits(tmp, 'try-error')) tmp
}), use.names = TRUE, fill = TRUE)

REV <- na.omit(REV)

saveRDS(REV, paste0("SEC_REV_2018-2021q4.rds"))
ALL_DF = readRDS(paste0("SEC_REV_2018-2021q4.rds"))
