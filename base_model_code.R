script_name = sub("--file=", "", commandArgs(trailingOnly = FALSE)[grep("--file=", commandArgs(trailingOnly = FALSE))])
print(paste("Running script:", script_name))

library(dplyr)
library(data.table)
library(purrr)

Sys.setenv(TZ='America/Los_Angeles')

print_timestamped = function(message) {
  timestamped_message = paste(Sys.time(), message)
  cat(timestamped_message)
  flush.console()
  Sys.sleep(1)
}

print_timestamped("script started")
schedules = expand.grid(replicate(24, c(0, 1), simplify = FALSE))

print_timestamped("filtering invalid schedules")
doubleschedule = cbind(schedules,schedules)
colnames(doubleschedule) = -24:23
negative_check = function(row) {
  first_zero_found = FALSE
  results = numeric(length(row)) 
  accum = 0 
  
  for (i in seq_along(row)) {
    gen_hours = sum(row) / 2  
    
    if (!first_zero_found && row[i] == 0) {
      first_zero_found = TRUE
    }
    
    if (first_zero_found) {
      if (row[i] == 1) {
        accum <- accum + 1 - (24 / gen_hours)  
      } else if (row[i] == 0) {
        accum <- accum + 1  
      }
    }
    results[i] = accum  
  }
  
  return(min(results))  
}

low_value = data.frame(
  lowest_tank_value = apply(doubleschedule, 1, negative_check),
  row_index = seq_len(nrow(doubleschedule))
)

valid_schedules = schedules[low_value$row_index[low_value$lowest_tank_value >= -0.000001], ]
invalid_schedules = schedules[low_value$row_index[low_value$lowest_tank_value < -0.000001], ]

bottom_limit = 1
n_options = sum(rowSums(valid_schedules) >= bottom_limit & rowSums(valid_schedules) <= 24)
n_hours = length(seq(bottom_limit, 24))

#test case 
feed_rate = 50 #tons/day

#technical assumptions
num_days = 366 #2024 leap year
biogas_yield = 75000 #L biogas per ton feedstock, taken from R. Zhang lab EPA report 
energy_density = 596.790663 #kWh per ton feedstock, calculated from R. Zhang lab analysis of READ biomass
conv_kW_to_MW=1/1000 #MWh per kWh
conv_MW_to_kW=1000 #kWh to MWh
turbine_eff = .4 #[-]
storage_lifetime = 20 #years 
gen_lifetime = 80000 #active hours
yearly_maint_cost = 200000 #$/year
hourly_worker_cost = 100 #$/(hr - worker)
tipping_fee = 72 #$/ton (short ton)
worker_time_per_week = 40 #hrs/week
number_workers = 3 #workers/facility

#unused
conv_tonne_to_ton = 0.907185 #tonnes per ton
conv_toe_to_MWh = 11.63 #MWh per toe

#calculated assumptions
Atable_list = vector("list", num_days)
Btable_list = vector("list", num_days)
size_gen_dict = NULL
lived_rows = NULL
right_hours_merge_rev = NULL
lived_hours = NULL
year_bal_summary = NULL
biogas_per_day = feed_rate*biogas_yield #L per day
biogas_per_hour = biogas_per_day/24
#energy_density = energy_density_IEA*conv_tonne_to_ton*conv_toe_to_MWh*conv_MW_to_kW #final kWh per ton feedstock
yearly_worker_cost = hourly_worker_cost*worker_time_per_week*number_workers*52
yearly_tipping_rev = feed_rate*num_days*tipping_fee

#load elect prices, node = DLAP_PGEA-APND, MODEL DOES NOT USE DAYLIGHT SAVINGS TIME
file_paths <- c(
  "Input Files/20240101_20240201_PRC_LMP_DAM_20241126_15_30_27_v12.csv",
  "Input Files/20240201_20240301_PRC_LMP_DAM_20241126_15_31_50_v12.csv",
  "Input Files/20240301_20240401_PRC_LMP_DAM_20241126_15_21_34_v12.csv",
  "Input Files/20240401_20240501_PRC_LMP_DAM_20241126_15_22_57_v12.csv",
  "Input Files/20240501_20240601_PRC_LMP_DAM_20241126_15_23_40_v12.csv",
  "Input Files/20240601_20240701_PRC_LMP_DAM_20241126_15_24_18_v12.csv",
  "Input Files/20240701_20240801_PRC_LMP_DAM_20241126_15_24_54_v12.csv",
  "Input Files/20240801_20240901_PRC_LMP_DAM_20241126_15_26_14_v12.csv",
  "Input Files/20240901_20241001_PRC_LMP_DAM_20241126_15_26_52_v12.csv",
  "Input Files/20241001_20241101_PRC_LMP_DAM_20241126_15_27_30_v12.csv",
  "Input Files/20241101_20241201_PRC_LMP_DAM_20250131_11_27_34_v12.csv",
  "Input Files/20241201_20250101_PRC_LMP_DAM_20250131_11_28_54_v12.csv"
)

month_names <- c(
  "elect_prices_jan", "elect_prices_feb", "elect_prices_mar",
  "elect_prices_apr", "elect_prices_may", "elect_prices_jun",
  "elect_prices_jul", "elect_prices_aug", "elect_prices_sep",
  "elect_prices_oct", "elect_prices_nov", "elect_prices_dec"
)

print_timestamped("building price curves")
for (i in seq_along(file_paths)) {
  df = read.csv(file_paths[i])
  df = df[df$XML_DATA_ITEM == "LMP_ENE_PRC", ]
  df = df[order(df$INTERVALSTARTTIME_GMT), ]
  df = t(df$MW)
  df = as.data.table(df)
  assign(month_names[i], df)
}

elect_prices_yr = cbind(elect_prices_jan, elect_prices_feb, elect_prices_mar,
                        elect_prices_apr, elect_prices_may, elect_prices_jun,
                        elect_prices_jul, elect_prices_aug, elect_prices_sep,
                        elect_prices_oct, elect_prices_nov, elect_prices_dec)

print_timestamped("starting main loop")
for (gen_hours in bottom_limit:24) {
  schedules_live = valid_schedules[rowSums(valid_schedules) == gen_hours, ]
  
  if (is.null(lived_rows)) {
    lived_rows = nrow(schedules_live)
  } else {
    lived_rows = lived_rows + nrow(schedules_live)
  }
  
  double_sched = cbind(schedules_live,schedules_live)
  colnames(double_sched) = -24:23
  
  cumulative_sum = function(row) {
    result = accumulate(row, function(accum, x) {
      if (x == 1 && (24 / gen_hours) > (1 + accum)) {
        accum = 0 
      } else if (x == 1) {
        accum = accum + 1 - (24 / gen_hours) 
      } else if (x == 0) {
        accum = accum + 1  
      }
      accum
    }, .init = 0)
    
    return(max(result))
  }
  
  max_values = double_sched %>%
    rowwise() %>%
    mutate(max_storage_capacity = cumulative_sum(c_across(everything()))) %>%
    ungroup()
  
  max_values$max_storage_capacity = max_values$max_storage_capacity*biogas_per_day/24
  max_values$Generator_Hours = gen_hours
  
  storage_maintain_cost = 0
  storage_capital = 0.8*max_values$max_storage_capacity*5 ###FINALIZE
  total_storage_cost = storage_capital + storage_maintain_cost 
  storage_cost_per_day = total_storage_cost / (storage_lifetime*365)
  
  size_gen = feed_rate * energy_density * turbine_eff / gen_hours ##biogas in L; energy density in kWh per L; gen_hours in hr; final in kW
  size_gen = floor(size_gen)
  
  gen_maintain_cost = 0
  gen_capital_cost = 200*size_gen*10 ###FINALIZE
  total_gen_cost = gen_maintain_cost + gen_capital_cost
  gen_cost_per_day = total_gen_cost*gen_hours / gen_lifetime ##cost * generator hours per day / total generator hours = cost per day
  
  for (day in 1:num_days) {
    hour_range = (1+(day-1)*24):(24*day)
    elect_sched = schedules_live*size_gen/1000 ##divide by 1000 to convert from kWh to MWh
    elect_sched = as.data.table(elect_sched)
    elect_prices = elect_prices_yr[,..hour_range]
    rev_sched = elect_sched*elect_prices[rep(1,nrow(elect_sched)),]
    col_name = paste("daily_rev", day, sep = "_")
    rev_sched[[col_name]] = rowSums(rev_sched)
    
    sched_summary = cbind(max_values,storage_cost_per_day,gen_cost_per_day,size_gen,rev_sched)
    sched_summary$profit = sched_summary$daily_rev-sched_summary$storage_cost-sched_summary$gen_cost
    
    ##A tables
    columns=c("max_storage_capacity","storage_cost_per_day","Generator_Hours","size_gen","gen_cost_per_day",col_name,"profit")
    sched_summary_A = sched_summary[columns]
    
    sched_summary_A = sched_summary_A %>% 
      group_by(max_storage_capacity) %>% 
      filter(profit == max(profit)) %>% 
      ungroup()
    
    if (is.null(Atable_list[[day]])) {
      Atable_list[[day]] = sched_summary_A
    } else {
      Atable_list[[day]] = rbind(Atable_list[[day]], sched_summary_A)
    }
    
    ##B tables
    sched_summary = sched_summary %>% 
      arrange(desc(profit)) %>% 
      slice_head(n = 300)
    
    sched_summary = sched_summary[,-c(1:24)]
    sched_summary = sched_summary[ , !grepl("^Var", colnames(sched_summary))]
    
    if (is.null(Btable_list[[day]])) {
      Btable_list[[day]] = sched_summary
    } else {
      Btable_list[[day]] = rbind(Btable_list[[day]], sched_summary)
    }
  }
  
  #dictionary_sizes = data.table("Constructed Generator Hours"=gen_hours,"Size Generator"=size_gen,"gen_cost"=gen_cost_per_day)
  #if (is.null(size_gen_dict)) {
  #  size_gen_dict = dictionary_sizes
  #} else {
  #  size_gen_dict = rbind (size_gen_dict,dictionary_sizes)
  #}
  
  percent_done = lived_rows/n_options*100
  print_timestamped(sprintf("finished processing gen_hours %f. approx %f%% done.",gen_hours,percent_done))
  flush.console()
}

print_timestamped("starting A table loop")

#Atable summary
for (gen_hr in bottom_limit:24) { 
  if (is.null(lived_hours)) {
    lived_hours = 1
  } else {
    lived_hours = lived_hours + 1
  }  
  
  for (day in 1:num_days) {
    right_hours = Atable_list[[day]] %>% filter(Generator_Hours==gen_hr)
    col_name = paste("daily_rev", day, sep = "_")
    
    if (is.null(right_hours_merge_rev)) {
      columns=c("max_storage_capacity","storage_cost_per_day","Generator_Hours","size_gen","gen_cost_per_day",col_name)
      right_hours = right_hours[columns]
      right_hours_merge_rev = right_hours
    } else{
      columns=c("max_storage_capacity",col_name)
      right_hours = right_hours[columns]
      right_hours_merge_rev = merge(right_hours_merge_rev,right_hours,by="max_storage_capacity",all.x=TRUE)
    }
  }
  
  right_hours_merge_rev = right_hours_merge_rev %>%
    mutate(
      yearly_rev = rowSums(select(., starts_with("daily_rev")), na.rm = TRUE)
    )
  
  right_hours_merge_rev = right_hours_merge_rev %>%
    mutate(yearly_tipping_rev = yearly_tipping_rev, yearly_worker_cost = yearly_worker_cost, yearly_maint_cost = yearly_maint_cost)
  
  right_hours_merge_rev = right_hours_merge_rev %>%
    mutate(Yearly_Bal_Profit = yearly_rev - gen_cost_per_day*num_days - storage_cost_per_day*num_days + yearly_tipping_rev - yearly_worker_cost - yearly_maint_cost)  
  
  best_profit_year_balance = right_hours_merge_rev %>%
    arrange(desc(Yearly_Bal_Profit)) %>%
    slice(1)
  
  right_hours_merge_rev = NULL
  
  ##add to interloop
  if (is.null(year_bal_summary)) {
    year_bal_summary = best_profit_year_balance
  } else {
    year_bal_summary = rbind (year_bal_summary,best_profit_year_balance)
  }
  
  percent_done = lived_hours/n_hours*100
  
  print_timestamped(sprintf("finished processing gen_hours %f. ROUGHLY ROUGHLY %f%% done.",gen_hr,percent_done))
}

print_timestamped("merging and ordering final A table")
Final_Balanced = year_bal_summary[order(year_bal_summary$Yearly_Bal_Profit,decreasing = TRUE),]
fwrite(Final_Balanced,"Full_Output/Full_Summ_Bal.csv")
print_timestamped("final A tables complete")

#Btable summaries
print_timestamped("ordering and writing final B tables")
for (day in 1:num_days) {
  current_table = Btable_list[[day]]
  writable = current_table[order(current_table$profit, decreasing = TRUE), ]
  writable = head(writable, 300)
  file_name = paste0("Full_Output/Full_Summ_Day_", day, ".csv")
  fwrite(writable, file_name)
}

print_timestamped("script completed")
