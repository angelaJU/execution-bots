########################################################################################
# Date: 2018.07
# Author: min li 
# Email: min.li@altonomy.io
# Github: https://github.com/happyshiller/Altonomy
#
########################################################################################

import pandas
import numpy as np

def mean_reversion_ema(price, emaprice, cap=0.0050):
	if price < 1e-10 or emaprice < 1e-10:
		return 0.0
	signal = (emaprice - price)/price
	## default 50bps cap on signal, but can be scaled by time horizon
	signal = max(-cap, min(cap, signal))
	return signal

def book_pressure(depthbid, depthask, qty_exp = 0.5, cap=0.0050):
    
    sv = [x['volume'] for x in depthask]
    sp = [x['price'] for x in depthask]
    bv = [x['volume'] for x in depthbid]
    bp = [x['price'] for x in depthbid]

    sv_total = np.power(sv, qty_exp).sum()
    bv_total = np.power(bv, qty_exp).sum()
    v_tmin = np.minimum(sv_total, bv_total)
    v_tmin_s = np.repeat(v_tmin, len(sv))
    v_tmin_b = np.repeat(v_tmin, len(bv))

    sv_arr = np.power(sv, qty_exp)
    sv_arr_cap = np.minimum( np.cumsum(sv_arr), v_tmin_s)
    sv_arr_cap = np.insert(sv_arr_cap, 0, 0)
    sv_arr_cap = np.diff(sv_arr_cap)
    sp_adj = np.divide(np.multiply(sv_arr_cap, sp).sum(),  sv_arr_cap.sum())

    bv_arr = np.power(bv, qty_exp)
    bv_arr_cap = np.minimum( np.cumsum(bv_arr), v_tmin_b)
    bv_arr_cap = np.insert(bv_arr_cap, 0, 0)
    bv_arr_cap = np.diff(bv_arr_cap)
    bp_adj = np.divide(np.multiply(bv_arr_cap, bp).sum(),  bv_arr_cap.sum())

    mid = (sp[0] + bp[0]) / 2.0
    padj = (sp_adj + bp_adj)/2.0
    signal = np.log(padj / mid)
    signal = max(-cap, min(cap, signal))
    return signal

def quote_imbalance(bid,ask,bsize,asize,cap=0.0050):
    mid = (bid + ask) / 2.0
    wmid = (bid * asize + ask * bsize) / (bsize + asize)
    signal = np.log(wmid / mid)
    signal = max(-cap, min(cap, signal))
    return signal
	
	
