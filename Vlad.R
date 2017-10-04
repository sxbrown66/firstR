library(dplyr)
library(sparklyr)
library(DBI) #> dbGetQuery

sc <- spark_connect(master = "local")

employees <- tibble(
  dept_id = c(10,   10,   10,   10,   20,   20,   20),
  salary  = c(1000, 1000, 2000, 3000, 5000, 6000, NA))

employees_tbl <- copy_to(sc, employees)

#> http://www.folkstalk.com/2009/12/netezza-max-and-min-analytic-functions.html

(min_max_query_1 <- dbGetQuery(sc,
                               "SELECT dept_id, salary,
                               MIN(salary) OVER() min_sal,
                               MAX(salary) OVER() max_sal
                               FROM employees"))

(min_max_query_1 <- employees_tbl %>%
    mutate(
      min_sal = min(salary),
      max_sal = max(salary)) %>%
    collect)

(min_max_query_2 <- dbGetQuery(sc,
                               "SELECT dept_id, salary,
                               MIN(salary) OVER(PARTITION BY dept_id) group_min,
                               MAX(salary) OVER(PARTITION BY dept_id) group_max
                               FROM employees"))

(min_max_query_2 <- employees_tbl %>%
    group_by(dept_id) %>%
    mutate(
      group_min = min(salary),
      group_max = max(salary)) %>%
    collect)

#> http://www.folkstalk.com/2009/12/netezza-average-analytic-function.html

(avg_query_1 <- dbGetQuery(sc,
                           "SELECT dept_id, salary,
                           AVG(salary) OVER() avg_sal
                           FROM employees"))

(avg_query_1 <- employees_tbl %>%
    mutate(
      avg_sal = mean(salary)) %>%
    collect)

(avg_query_2 <- dbGetQuery(sc,
                           "SELECT dept_id, salary,
                           AVG(salary) OVER(PARTITION BY dept_id) group_avg
                           FROM employees"))

(avg_query_2 <- employees_tbl %>%
    group_by(dept_id) %>%
    mutate(
      group_avg = mean(salary)) %>%
    collect)

#> http://www.folkstalk.com/2009/12/cumulative-sum-and-average-using.html

(sum_avg_query_1 <- dbGetQuery(sc,
                               "SELECT dept_id, salary,
                               SUM(salary) OVER(ORDER BY salary NULLS LAST ROWS UNBOUNDED PRECEDING) cum_sum,
                               AVG(salary) OVER(ORDER BY salary NULLS LAST ROWS UNBOUNDED PRECEDING) cum_avg
                               FROM employees"))

(sum_avg_query_1 <- employees_tbl %>%
    arrange(desc(-salary)) %>% #> NULLS LAST
    mutate(
      cum_sum = cumsum(salary),
      cum_avg = cummean(salary)) %>%
    collect)

(sum_avg_query_2 <- dbGetQuery(sc,
                               "SELECT dept_id, salary,
                               SUM(salary) OVER(PARTITION BY dept_id
                               ORDER BY salary NULLS LAST ROWS UNBOUNDED PRECEDING) cum_sum,
                               AVG(salary) OVER(PARTITION BY dept_id
                               ORDER BY salary NULLS LAST ROWS UNBOUNDED PRECEDING) cum_avg
                               FROM employees"))

(sum_avg_query_1 <- employees_tbl %>%
    group_by(dept_id) %>%
    arrange(dept_id, desc(-salary)) %>% #> NULLS LAST
    mutate(
      cum_sum = cumsum(salary),
      cum_avg = cummean(salary)) %>%
    collect)
