*******************************************************************
* 导入数据，环境配置
*******************************************************************
cd "D:\01 实践与科研\2023 气候变化 非洲\data\09 roads type\stata"
clear 
import excel "D:\01 实践与科研\2023 气候变化 非洲\data\05 WFP-VAM crop prices\05 WFP crop price\02 working data 2000+\10_WFP_FAO_API_all_crop_price_conflict_other_month_market_panel_2017_2024\local nominal\WFP_FAO_all_crop_price_conflict_other_market_monthly_stata_2017_2024_mine.xlsx", firstrow clear



*******************************************************************
* 生成需要的变量
*******************************************************************
*

gen id = gridcode
keep if month > 0

gen distance_original = exp(distance)/1000
gen distance_capital_original = exp(distance_to_capital)/1000
gen oil_original = exp(price_inter_oil)/100
gen light_original = exp(light_admin2)
replace dist_line = dist_line/1000000
replace distance_shore = distance_shore/1000000
replace distance_border = distance_border/1000000
gen dist_line_log = ln(dist_line)
gen dist_shore_log = ln(distance_shore)
gen dist_border_log = ln(distance_border)
gen mprice_maize_origin = exp(mprice_maize)

egen id_country=group(country)
egen id_region=group(GID_1)

* 生成lead期

tsset id month
gen mprice_all_2 = F2.mprice_all
gen mprice_millet_2 = F2.mprice_millet
gen mprice_sorghum_2 = F2.mprice_sorghum
gen mprice_maize_2 = F2.mprice_maize

gen ucdp_all_10_sum_2 = F2.ucdp_all_10_sum
gen ucdp_state_10_sum_2 = F2.ucdp_state_10_sum
gen ucdp_nonstate_10_sum_2 = F2.ucdp_nonstate_10_sum
gen ucdp_all_end_10_sum_2 = F2.ucdp_all_end_10_sum
gen acled_all_10_sum_2 = F2.acled_all_10_sum
gen acled_state_10_sum_2 = F2.acled_state_10_sum
gen acled_nonstate_10_sum_2 = F2.acled_nonstate_10_sum
gen best_2 = F2.best
gen fatalities_2 = F2.fatalities

* 生成lag期
gen trans_cost = distance_original*oil_original
gen trans_cost_L1 = distance_original*L1.oil_original
gen trans_cost_L2 = distance_original*L2.oil_original
gen trans_cost_L3 = distance_original*L3.oil_original
gen month_L1 = L1.month
gen month_L2 = L2.month
gen month_L3 = L3.month
gen oil_original_L1 = L1.oil_original

* 插值 mean_radiance
bys id: ipolate mean_radiance month, gen (mean_radiance_lin)

* 设置控制变量
global controls mean_radiance_lin c.distance_capital_original#c.mean_radiance_lin




* 保留纳入 baseline 的样本
ivreghdfe mprice_maize (trans_cost_L1 = c.dist_line#c.L1.oil_news), absorb(id month) cluster(id) first
gen byte _reg = e(sample)
bys id: egen byte _id_keep = max(_reg)
keep if _id_keep


* 生成石油信息冲击变量
gen double oil_news = .

replace oil_news = -0.56463600517105 if month==0
replace oil_news = 0.370926855635361 if month==1
replace oil_news = 0.268768832 if month==2
replace oil_news = 0.560370097 if month==3
replace oil_news = 0.772460924 if month==4
replace oil_news = 0.390471065 if month==5
replace oil_news = 0.091248014 if month==6
replace oil_news = -0.355182319 if month==7
replace oil_news = 1.146057817 if month==8
replace oil_news = 0.72100128  if month==9
replace oil_news = -0.471221033 if month==10
replace oil_news = -0.450282951 if month==11
replace oil_news = 0.835651563 if month==12
replace oil_news = 0.156938327 if month==13
replace oil_news = 0.951307198 if month==14
replace oil_news = -0.453420009 if month==15
replace oil_news = 0.820566202 if month==16
replace oil_news = 0.357989231 if month==17
replace oil_news = -1.574946679 if month==18
replace oil_news = -0.684136546 if month==19
replace oil_news = -0.604412259 if month==20
replace oil_news = 0.623507953 if month==21
replace oil_news = -0.281108538 if month==22
replace oil_news = -0.609589586 if month==23
replace oil_news = 0.165616319 if month==24
replace oil_news = -0.302322302 if month==25
replace oil_news = -0.728296721 if month==26
replace oil_news = 0.75185382  if month==27
replace oil_news = -0.931960471 if month==28
replace oil_news = 0.227280268 if month==29
replace oil_news = 0.228067607 if month==30
replace oil_news = 0.326701256 if month==31
replace oil_news = 0.591255269 if month==32
replace oil_news = -0.076280856 if month==33
replace oil_news = -0.564427583 if month==34
replace oil_news = 0.000155091 if month==35


*******************************************************************
* 	TABLE I: Distance to lines on distance to ports.
*******************************************************************
glo tab "D:\01 实践与科研\2023 气候变化 非洲\data\09 roads type\stata\04 results_news_shock\02 Africa trans cost on crop price 2SLS mine IV" /* directory to store tables*/
cd  "${tab}"
global outoption1 addtext("Country FE","Yes") nocons addstat("Number of Market", e(N_clust))
global outoption2 addtext("Market FE","No","Month FE","No","Control var.","No") nocons addstat("Number of Markets", e(N_clust))
global outoption3 addtext("Market FE","Yes","Month FE","Yes","Control var.","No") nocons addstat("Number of Markets", e(N_clust))
global outoption4 addtext("Market FE","Yes","Month FE","Yes","Control var.","Yes") nocons addstat("Number of Markets", e(N_clust))


*
preserve

bys id: gen num = _n 
keep if num == 1
reghdfe distance_original dist_line, absorb (id_country) cluster(id)
outreg2  using  distance_ports_and_network.xls, $outoption1 replace
reghdfe distance_capital_original dist_line, absorb (id_country) cluster(id)
outreg2  using  distance_ports_and_network.xls, $outoption1 append
reghdfe distance_shore dist_line, absorb (id_country) cluster(id)
outreg2  using  distance_ports_and_network.xls, $outoption1 append
reghdfe distance_border dist_line, absorb (id_country) cluster(id)
*outreg2  using  distance_ports_and_network.xls, $outoption1 append
restore







*******************************************************************
* 	TABLE II: Trans cost on crop price, 2SLS
*******************************************************************
* 2SLS, absorb (id), price cost mprice_all mprice_maize mprice_millet mprice_sorghum
// no fixed effect
ivreghdfe mprice_maize (trans_cost_L1 = c.dist_line#c.L1.oil_news), noabsorb cluster(id) first
outreg2  using  price_cost_2sls.xls, $outoption2 replace

// fixed effect
ivreghdfe mprice_maize (trans_cost_L1 = c.dist_line#c.L1.oil_news) , absorb(id month) cluster(id) first
outreg2  using  price_cost_2sls.xls, $outoption3 append

// fixed effect + control variables
ivreghdfe mprice_maize (trans_cost_L1 = c.dist_line#c.L1.oil_news) mean_radiance_lin c.distance_capital_original#c.oil_original_L1 , absorb(id month) cluster(id) first
outreg2  using  price_cost_2sls.xls, $outoption4 append






*******************************************************************
* 	TABLE II: Descriptive Variables
*******************************************************************
// 回归之前&之后样本的国家list
*1
tabulate country
bysort country: egen unique_addresses = tag(Address)
by country: summarize unique_addresses

*2
preserve
ivreghdfe mprice_maize (trans_cost_L1 = c.dist_line#c.month_L1), absorb(id month) cluster(id) first
keep if e(sample)
tabulate country
restore

// 筛选纳入回归的样本，描述性统计!!!!!!!!!!!!!!需要再确定下是否需要取滞后!!!!!!!!!!!!!!
preserve

ivreghdfe mprice_maize (trans_cost_L1 = c.dist_line#c.month_L1), absorb(id month) cluster(id) first
keep if e(sample)
* 把这一轮回归真正用到的观测标记出来
*gen byte _reg = e(sample)
* 只要某个 id 有任意一个月进过回归，就保留这个 id 的所有观测（所有月份）
*bys id: egen byte _id_keep = max(_reg)
*keep if _id_keep


distinct id country
levelsof country
* 转换单位 distance_original-1000km,oil_original-100 dollars/barrel
replace mprice_maize = exp(mprice_maize)/100000 // local currency/100000t
replace ucdp_all_10_sum = exp(ucdp_all_10_sum)-1 // times
replace acled_all_10_sum = exp(acled_all_10_sum)-1 // times
replace best = exp(best)-1 // people
replace fatalities = exp(fatalities)-1 // people
replace best = best/1000 // best, 1000 people
// distance, 1000 km
// maize_suit_ratio_1to4 (%), suitability (>40% potential yield (0,1,2,3,4) land%)
// spei_mean_month, spei average (market and month average)
replace mean_radiance_lin = exp(mean_radiance_lin)-1 // economic nigh light, DN

logout, save("D:\01 实践与科研\2023 气候变化 非洲\data\09 roads type\stata\04 results_news_shock\02 Africa trans cost on crop price 2SLS mine IV\descriptive.xlsx") excel replace: ///
  tabstat  mprice_maize ucdp_all_10_sum acled_all_10_sum best fatalities distance_original distance_capital_original dist_line oil_original mean_radiance_lin, ///
  stats(n mean sd min p50 max) c(s) f(%9.3f)

restore



*******************************************************************
* 	TABLE III: Trans cost on crop price, OLS
*******************************************************************

reghdfe mprice_maize c.distance_original#c.oil_original_L1, noabsorb cluster(id)
outreg2  using  price_cost_ols_results_all.xls, $outoption2 replace

reghdfe mprice_maize c.distance_original#c.oil_original_L1, absorb(id Time) cluster(id)
outreg2  using  price_cost_ols_results_all.xls, $outoption3 append

reghdfe mprice_maize c.distance_original#c.oil_original_L1 mean_radiance_lin c.distance_capital_original#c.oil_original_L1, absorb(id Time) cluster(id)
outreg2  using  price_cost_ols_results_all.xls, $outoption4 append





*******************************************************************
* 	TABLE III: The Effect of Stright Line on Crop Prices, reduced-form, Z on Y
*******************************************************************

reghdfe mprice_maize c.dist_line#c.L1.oil_news, noabsorb cluster(id)
outreg2  using  price_cost_reduced_results_all.xls, $outoption2 replace

reghdfe mprice_maize c.dist_line#c.L1.oil_news, absorb(id month) cluster(id)
outreg2  using  price_cost_reduced_results_all.xls, $outoption3 append

reghdfe mprice_maize c.dist_line#c.L1.oil_news mean_radiance_lin c.distance_capital_original#c.oil_original_L1, absorb(id month) cluster(id)
outreg2  using  price_cost_reduced_results_all.xls, $outoption4 append





*******************************************************************
* 	TABLE IV: Trans cost on crop price, 2SLS, 1st Stage
*******************************************************************

eststo clear
ivreghdfe mprice_maize (trans_cost_L1 = c.dist_line#c.L1.oil_news),noabsorb cluster(id) first savefirst savefprefix(f)
eststo
estadd scalar F = `e(widstat)' : ftrans_cost_L1
esttab ftrans_cost_L1 est1 using price_cost_2sls_first_1.doc, scalar(F) replace drop($ctrl) se

eststo clear
ivreghdfe mprice_maize (trans_cost_L1 = c.dist_line#c.L1.oil_news),absorb(id month) cluster(id) first savefirst savefprefix(f)
eststo
estadd scalar F = `e(widstat)' : ftrans_cost_L1
esttab ftrans_cost_L1 est1 using price_cost_2sls_first_2.doc, scalar(F) replace drop($ctrl) se


eststo clear
ivreghdfe mprice_maize (trans_cost_L1 = c.dist_line#c.L1.oil_news) mean_radiance_lin c.distance_capital_original#c.oil_original_L1,absorb(id month) cluster(id) first savefirst savefprefix(f)
eststo
estadd scalar F = `e(widstat)' : ftrans_cost_L1
esttab ftrans_cost_L1 est1 using price_cost_2sls_first_3.doc, scalar(F) replace drop($ctrl) se













































