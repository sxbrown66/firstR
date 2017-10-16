library(dplyr)
library(sparklyr)
library(DBI) #> dbGetQuery
#> library(zoo) #> na.aggregate

employees <- tibble(
  dept_id = c(20,   20,   20,   20,   10,   10,   10,   10,   30,   30),
  emp_id  = c(2011, 2012, 2015, 2018, 1501, 1506, 1510, NA,   3101, 3102),
  salary  = c(1000, 1000, 2000, 3000, 5000, NA,   6000, NA,   NA,   NA))

sc <- spark_connect(master = "local")

employees_tbl <- copy_to(sc, employees)

employees_tbl %>%
  mutate(
    min = min(salary),
    max = max(salary))

# ex_tbl <- spark_read_csv(sc, 
#                          name = "srbo0003",
#                          path = "C:/temp")

ex_tbl <- spark_read_csv(sc, 
                         name = "univar_data",
                         path = "C:/temp")

