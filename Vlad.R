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

#> http://www.folkstalk.com/2009/12/netezza-max-and-min-analytic-functions.html

(min_max_query_1 <- employees %>%
    mutate_at(vars(salary, salary), funs(min, max), na.rm = TRUE) %>%
    mutate_at(vars(min, max),
              function(x) replace(x, is.infinite(x), NA)))

(min_max_query_1 <- dbGetQuery(sc,
                               "SELECT dept_id, emp_id, salary,
                               MIN(salary) OVER() min,
                               MAX(salary) OVER() max
                               FROM employees"))

(min_max_query_1 <- employees_tbl %>%
    mutate(
      min = min(salary),
      max = max(salary)) %>%
    collect)

(min_max_query_2 <- employees %>%
    group_by(dept_id) %>%
    mutate_at(vars(salary, salary), funs(min, max), na.rm = TRUE) %>%
    mutate_at(vars(min, max),
              function(x) replace(x, is.infinite(x), NA)))

(min_max_query_2 <- dbGetQuery(sc,
                               "SELECT dept_id, emp_id, salary,
                               MIN(salary) OVER(PARTITION BY dept_id) min,
                               MAX(salary) OVER(PARTITION BY dept_id) max
                               FROM employees"))

(min_max_query_2 <- employees_tbl %>%
    group_by(dept_id) %>%
    mutate(
      min = min(salary),
      max = max(salary)) %>%
    collect)

#> http://www.folkstalk.com/2009/12/netezza-average-analytic-function.html

(avg_query_1 <- employees %>%
    mutate(
      avg = replace(salary, !all(is.na(salary)),
                    round(mean(salary, na.rm = TRUE), 2))))

(avg_query_1 <- dbGetQuery(sc,
                           "SELECT dept_id, emp_id, salary,
                           ROUND(AVG(salary) OVER(), 2) avg
                           FROM employees"))

(avg_query_1 <- employees_tbl %>%
    mutate(
      avg = round(mean(salary), 2)) %>%
    collect)

(avg_query_2 <- employees %>%
    group_by(dept_id) %>%
    mutate(
      avg = replace(salary, !all(is.na(salary)),
                    round(mean(salary, na.rm = TRUE), 2))))

(avg_query_2 <- dbGetQuery(sc,
                           "SELECT dept_id, emp_id, salary,
                           ROUND(AVG(salary) OVER(PARTITION BY dept_id), 2) avg
                           FROM employees"))

(avg_query_2 <- employees_tbl %>%
    group_by(dept_id) %>%
    mutate(
      avg = round(mean(salary), 2)) %>%
    collect)

#> http://www.folkstalk.com/2009/12/cumulative-sum-and-average-using.html

(sum_avg_query_1 <- employees %>%
    arrange(dept_id, emp_id) %>%
    mutate(
      cum_sum = ifelse(cumall(is.na(salary)), NA,
                       cumsum(replace(salary, is.na(salary), 0))),
      cum_avg = ifelse(cumall(is.na(salary)), NA,
                       round(cum_sum / cumsum(!is.na(salary)), 2))) %>%
    collect)

(sum_avg_query_1 <- dbGetQuery(sc,
                               "SELECT dept_id, emp_id, salary,
                               SUM(salary) OVER(
                               ORDER BY dept_id, emp_id NULLS LAST
                               ROWS UNBOUNDED PRECEDING) cum_sum,
                               ROUND(AVG(salary) OVER(
                               ORDER BY dept_id, emp_id NULLS LAST
                               ROWS UNBOUNDED PRECEDING), 2) cum_avg
                               FROM employees"))

(sum_avg_query_1 <- employees_tbl %>%
    arrange(dept_id, desc(-emp_id)) %>% #> NULLS LAST
    mutate(
      cum_sum = cumsum(salary),
      cum_avg = round(cummean(salary), 2)) %>%
    collect)

(sum_avg_query_2 <- employees %>%
    group_by(dept_id) %>%
    arrange(dept_id, emp_id) %>%
    mutate(
      cum_sum = ifelse(cumall(is.na(salary)), NA,
                       cumsum(replace(salary, is.na(salary), 0))),
      cum_avg = ifelse(cumall(is.na(salary)), NA,
                       round(cum_sum / cumsum(!is.na(salary)), 2))) %>%
    collect)

(sum_avg_query_2 <- dbGetQuery(sc,
                               "SELECT dept_id, emp_id, salary,
                               SUM(salary) OVER(PARTITION BY dept_id
                               ORDER BY dept_id, emp_id NULLS LAST
                               ROWS UNBOUNDED PRECEDING) cum_sum,
                               ROUND(AVG(salary) OVER(PARTITION BY dept_id
                               ORDER BY dept_id, emp_id NULLS LAST
                               ROWS UNBOUNDED PRECEDING), 2) cum_avg
                               FROM employees"))

(sum_avg_query_2 <- employees_tbl %>%
    group_by(dept_id) %>%
    arrange(dept_id, desc(-emp_id)) %>% #> NULLS LAST
    mutate(
      cum_sum = cumsum(salary),
      cum_avg = round(cummean(salary), 2)) %>%
    collect)

#> http://www.folkstalk.com/2009/12/netezza-rank-analytic-function.html

#> http://www.folkstalk.com/2009/12/netezza-count-analytic-functions.html

#> http://www.folkstalk.com/2009/12/netezza-lastvalue-analytic-function.html

#> Impute missing values as the average of non-missing values:

(imp_query_1 <- employees %>%
    mutate(
      salary = replace(salary,
                       !all(is.na(salary)) & is.na(salary),
                       round(mean(salary, na.rm = TRUE), 2))))

(imp_query_1 <- dbGetQuery(sc,
                           "SELECT dept_id, emp_id,
                           NVL(salary, ROUND(AVG(salary) OVER(), 2)) salary
                           FROM employees"))

(imp_query_1 <- employees_tbl %>%
    mutate(
      salary = case_when(
        is.na(salary) ~ round(mean(salary), 2),
        TRUE ~ salary)) %>%
    collect)

(imp_query_2 <- employees %>%
    group_by(dept_id) %>%
    mutate(
      salary = replace(salary,
                       !all(is.na(salary)) & is.na(salary),
                       round(mean(salary, na.rm = TRUE), 2))))

(imp_query_2 <- dbGetQuery(sc,
                           "SELECT dept_id, emp_id,
                           NVL(salary, ROUND(AVG(salary) OVER(PARTITION BY dept_id), 2)) salary
                           FROM employees"))

(imp_query_2 <- employees_tbl %>%
    group_by(dept_id) %>%
    mutate(
      salary = case_when(
        is.na(salary) ~ round(mean(salary), 2),
        TRUE ~ salary)) %>%
    collect)

#> Impute missing values using linear interpolation (and no extrapolation):

(imp_query_1 <- employees %>%
    arrange(dept_id, emp_id) %>%
    group_by(cumsum(!is.na(lag(salary)))) %>%
    mutate(
      max_sal = replace(salary, !all(is.na(salary)),
                        max(salary, na.rm = TRUE))) %>%
    ungroup() %>%
    group_by(cumsum(!is.na(salary))) %>%
    mutate(
      min_sal = replace(salary, !all(is.na(salary)),
                        min(salary, na.rm = TRUE)),
      salary = ifelse(min_sal != max_sal,
                      round(min_sal + (max_sal - min_sal) * (row_number() - 1) / n(), 2),
                      min_sal)) %>%
    ungroup() %>%
    select(dept_id, emp_id, salary))

(imp_query_1 <- dbGetQuery(sc,
                           "SELECT dept_id, emp_id,
                           CASE WHEN min_sal != max_sal THEN
                           ROUND(min_sal + (max_sal - min_sal) * (rn - 1) / n, 2)
                           ELSE
                           salary
                           END salary
                           FROM (
                           SELECT dept_id, emp_id, salary,
                           MAX(salary) OVER (PARTITION BY max_cnt) max_sal,
                           MIN(salary) OVER (PARTITION BY min_cnt) min_sal,
                           ROW_NUMBER() OVER (
                           PARTITION BY min_cnt ORDER BY dept_id, emp_id NULLS LAST) rn,
                           COUNT(*) OVER (PARTITION BY min_cnt) n
                           FROM (
                           SELECT dept_id, emp_id, salary,
                           COUNT(lagsal) OVER (
                           ORDER BY dept_id, emp_id NULLS LAST
                           ROWS UNBOUNDED PRECEDING) max_cnt,
                           COUNT(salary) OVER (
                           ORDER BY dept_id, emp_id NULLS LAST
                           ROWS UNBOUNDED PRECEDING) min_cnt
                           FROM (
                           SELECT dept_id, emp_id, salary,
                           LAG(salary) OVER (ORDER BY dept_id, emp_id NULLS LAST) lagsal
                           FROM employees)))"))

(imp_query_1 <- employees_tbl %>%
    arrange(dept_id, desc(-emp_id)) %>% #> NULLS LAST
    mutate(
      max_cnt = cumsum(as.integer(!is.na(lag(salary)))),
      min_cnt = cumsum(as.integer(!is.na(salary)))) %>%
    group_by(max_cnt) %>%
    mutate(
      max_sal = max(salary)) %>%
    ungroup() %>%
    group_by(min_cnt) %>%
    mutate(
      min_sal = min(salary),
      salary = ifelse(min_sal != max_sal,
                      round(min_sal + (max_sal - min_sal) * (row_number() - 1) / n(), 2),
                      salary)) %>%
    ungroup() %>%
    select(dept_id, emp_id, salary) %>%
    collect)

(imp_query_2 <- employees %>%
    arrange(dept_id, emp_id) %>%
    group_by(dept_id, cumsum(!is.na(lag(salary)))) %>%
    mutate(
      max_sal = replace(salary, !all(is.na(salary)),
                        max(salary, na.rm = TRUE))) %>%
    ungroup() %>%
    group_by(dept_id, cumsum(!is.na(salary))) %>%
    mutate(
      min_sal = replace(salary, !all(is.na(salary)),
                        min(salary, na.rm = TRUE)),
      salary = ifelse(min_sal != max_sal,
                      round(min_sal + (max_sal - min_sal) * (row_number() - 1) / n(), 2),
                      min_sal)) %>%
    ungroup() %>%
    select(dept_id, emp_id, salary))

(imp_query_2 <- dbGetQuery(sc,
                           "SELECT dept_id, emp_id,
                           CASE WHEN min_sal != max_sal THEN
                           ROUND(min_sal + (max_sal - min_sal) * (rn - 1) / n, 2)
                           ELSE
                           salary
                           END salary
                           FROM (
                           SELECT dept_id, emp_id, salary,
                           MAX(salary) OVER (PARTITION BY dept_id, max_cnt) max_sal,
                           MIN(salary) OVER (PARTITION BY dept_id, min_cnt) min_sal,
                           ROW_NUMBER() OVER (
                           PARTITION BY dept_id, min_cnt ORDER BY dept_id, emp_id NULLS LAST) rn,
                           COUNT(*) OVER (PARTITION BY dept_id, min_cnt) n
                           FROM (
                           SELECT dept_id, emp_id, salary,
                           COUNT(lagsal) OVER (
                           PARTITION BY dept_id ORDER BY emp_id NULLS LAST
                           ROWS UNBOUNDED PRECEDING) max_cnt,
                           COUNT(salary) OVER (
                           PARTITION BY dept_id ORDER BY emp_id NULLS LAST
                           ROWS UNBOUNDED PRECEDING) min_cnt
                           FROM (
                           SELECT dept_id, emp_id, salary,
                           LAG(salary) OVER (
                           PARTITION BY dept_id ORDER BY emp_id NULLS LAST) lagsal
                           FROM employees)))"))

(imp_query_2 <- employees_tbl %>%
    arrange(dept_id, desc(-emp_id)) %>% #> NULLS LAST
    mutate(
      max_cnt = cumsum(as.integer(!is.na(lag(salary)))),
      min_cnt = cumsum(as.integer(!is.na(salary)))) %>%
    group_by(dept_id, max_cnt) %>%
    mutate(
      max_sal = max(salary)) %>%
    ungroup() %>%
    group_by(dept_id, min_cnt) %>%
    mutate(
      min_sal = min(salary),
      salary = ifelse(min_sal != max_sal,
                      round(min_sal + (max_sal - min_sal) * (row_number() - 1) / n(), 2),
                      salary)) %>%
    ungroup() %>%
    select(dept_id, emp_id, salary) %>%
    collect)

#> Impute missing values as the previous (or next) non-missing value:

(imp_query_1 <- employees %>%
    arrange(dept_id, emp_id) %>%
    group_by(cumsum(!is.na(salary))) %>%
    mutate(
      salary = replace(salary,
                       !all(is.na(salary)) & is.na(salary),
                       min(salary, na.rm = TRUE))) %>%
    ungroup() %>%
    group_by(cumsum(!is.na(lag(salary)))) %>%
    mutate(
      salary = replace(salary,
                       !all(is.na(salary)) & is.na(salary),
                       max(salary, na.rm = TRUE))) %>%
    ungroup() %>%
    select(dept_id, emp_id, salary))

(imp_query_1 <- dbGetQuery(sc,
                           "SELECT dept_id, emp_id,
                           COALESCE(salary, min_sal, max_sal) salary
                           FROM (
                           SELECT dept_id, emp_id, salary,
                           MAX(salary) OVER (PARTITION BY max_cnt) max_sal,
                           MIN(salary) OVER (PARTITION BY min_cnt) min_sal,
                           ROW_NUMBER() OVER (
                           PARTITION BY min_cnt ORDER BY dept_id, emp_id NULLS LAST) rn,
                           COUNT(*) OVER (PARTITION BY min_cnt) n
                           FROM (
                           SELECT dept_id, emp_id, salary,
                           COUNT(lagsal) OVER (
                           ORDER BY dept_id, emp_id NULLS LAST
                           ROWS UNBOUNDED PRECEDING) max_cnt,
                           COUNT(salary) OVER (
                           ORDER BY dept_id, emp_id NULLS LAST
                           ROWS UNBOUNDED PRECEDING) min_cnt
                           FROM (
                           SELECT dept_id, emp_id, salary,
                           LAG(salary) OVER (ORDER BY dept_id, emp_id NULLS LAST) lagsal
                           FROM employees)))"))

(imp_query_1 <- employees_tbl %>%
    arrange(dept_id, desc(-emp_id)) %>% #> NULLS LAST
    mutate(
      max_cnt = cumsum(as.integer(!is.na(lag(salary)))),
      min_cnt = cumsum(as.integer(!is.na(salary)))) %>%
    group_by(min_cnt) %>%
    mutate(
      salary = case_when(
        is.na(salary) ~ min(salary),
        TRUE ~ salary)) %>%
    ungroup() %>%
    group_by(max_cnt) %>%
    mutate(
      salary = case_when(
        is.na(salary) ~ max(salary),
        TRUE ~ salary)) %>%
    ungroup() %>%
    select(dept_id, emp_id, salary) %>%
    collect)

(imp_query_2 <- employees %>%
    arrange(dept_id, emp_id) %>%
    group_by(dept_id, cumsum(!is.na(salary))) %>%
    mutate(
      salary = replace(salary,
                       !all(is.na(salary)) & is.na(salary),
                       min(salary, na.rm = TRUE))) %>%
    ungroup() %>%
    group_by(dept_id, cumsum(!is.na(lag(salary)))) %>%
    mutate(
      salary = replace(salary,
                       !all(is.na(salary)) & is.na(salary),
                       max(salary, na.rm = TRUE))) %>%
    ungroup() %>%
    select(dept_id, emp_id, salary))

(imp_query_2 <- dbGetQuery(sc,
                           "SELECT dept_id, emp_id,
                           COALESCE(salary, min_sal, max_sal) salary
                           FROM (
                           SELECT dept_id, emp_id, salary,
                           MAX(salary) OVER (PARTITION BY dept_id, max_cnt) max_sal,
                           MIN(salary) OVER (PARTITION BY dept_id, min_cnt) min_sal,
                           ROW_NUMBER() OVER (
                           PARTITION BY dept_id, min_cnt ORDER BY dept_id, emp_id NULLS LAST) rn,
                           COUNT(*) OVER (PARTITION BY dept_id, min_cnt) n
                           FROM (
                           SELECT dept_id, emp_id, salary,
                           COUNT(lagsal) OVER (
                           PARTITION BY dept_id ORDER BY emp_id NULLS LAST
                           ROWS UNBOUNDED PRECEDING) max_cnt,
                           COUNT(salary) OVER (
                           PARTITION BY dept_id ORDER BY emp_id NULLS LAST
                           ROWS UNBOUNDED PRECEDING) min_cnt
                           FROM (
                           SELECT dept_id, emp_id, salary,
                           LAG(salary) OVER (
                           PARTITION BY dept_id ORDER BY emp_id NULLS LAST) lagsal
                           FROM employees)))"))

(imp_query_2 <- employees_tbl %>%
    arrange(dept_id, desc(-emp_id)) %>% #> NULLS LAST
    mutate(
      max_cnt = cumsum(as.integer(!is.na(lag(salary)))),
      min_cnt = cumsum(as.integer(!is.na(salary)))) %>%
    group_by(dept_id, min_cnt) %>%
    mutate(
      salary = case_when(
        is.na(salary) ~ min(salary),
        TRUE ~ salary)) %>%
    ungroup() %>%
    group_by(dept_id, max_cnt) %>%
    mutate(
      salary = case_when(
        is.na(salary) ~ max(salary),
        TRUE ~ salary)) %>%
    ungroup() %>%
    select(dept_id, emp_id, salary) %>%
    collect)
=======
  
  # Haarstick Edits ---------------------------------------------------------

# Summarize all example
employees_tbl %>%
  group_by(dept_id) %>%
  summarise_all(.funs = funs(avg = mean), na.rm=T)

>>>>>>> fee8c21799974642a54cbd610cc83e81e3ba3a3c
