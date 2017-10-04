library(dplyr)
library(sparklyr)

Sys.getenv("SPARK_HOME")

sc <- spark_connect(master = "local",
                    spark_home = "C:/Hadoop/spark-2.2.0-bin-hadoop2.7")

summary(sc)

call_center_sdf <- spark_read_csv(sc,
                                  path = "c:/temp/hive/test.txt",
                                  name = "test",
                                  header = T,
                                  delimiter = ",",
                                  overwrite = T)

call_center_sdf

## Dplyr Verbs
# Select sro0003
call_center_sdf %>% 
  select(firstn, lastn)

# Filter
call_center_sdf %>% 
  filter(firstn == "Steven")

# Filter
call_center_sdf %>% 
  filter(lastn == "Brown")

# GROUP BY
call_center_sdf %>% 
  group_by(CALLTYPE) %>%
  summarise(count = n(), avg = mean(calls))


# Mutate
call_center_sdf %>% 
  mutate(time_period = paste(year(date), month(date), sep="_"), year = year(date))


# Arrange (order)
call_center_sdf %>% 
  arrange(date)

# Join
call_center_sdf %>% 
  filter(CALLTYPE %in% c("Tech Support", "Collections"))  %>%
  inner_join(call_center_sdf %>% 
               filter(CALLTYPE %in% c("Tech Support", "Sales")),
             by = c("ACCOUNTID", "CALLTYPE", "date"))


# Left Join
call_center_sdf %>% 
  filter(CALLTYPE %in% c("Tech Support", "Collections"))  %>%
  left_join(call_center_sdf %>% 
              filter(CALLTYPE %in% c("Tech Support", "Sales")),
            by = c("ACCOUNTID", "CALLTYPE", "date"))



## Other important functions
# Pivot
call_center_sdf %>% 
  filter(CALLTYPE %in% c("Tech Support", "Collections")) %>%
  sdf_pivot(as.formula("ACCOUNTID~CALLTYPE"),
            fun.aggregate = list("calls" = "sum"))



# Collect - pull data into memory
call_center_sdf %>%
  filter(date == "2014-12-16 00:00:00.0") %>%
  collect()



# Re-assign
call_center_new_sdf <- call_center_sdf %>% 
  filter(CALLTYPE %in% "Tech Support") %>%
  filter(date == "2015-02-17 00:00:00.0")