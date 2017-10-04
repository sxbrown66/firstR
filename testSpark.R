

Sys.getenv("SPARK_HOME")

sc <- spark_connect(master = "local",
                    spark_home = "C:/users....")

summary sc

dt <- spark_read_csv(c,
                     path = "c:\\users\\......",
                     name = "ntt",
                     header = T,
                     delimiter = ",",
                     overwrite = T
)

dt %>%
  filter(CALLTYPE %in% "Tech")