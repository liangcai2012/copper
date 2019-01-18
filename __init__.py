import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

_strp = lambda x : str(int(x)) if x-int(x)==0 else str(x)
path = "./minutes.csv"
start_hour_path = "./start_hour.csv"
daily_path = "./daily.csv"
daily_ext_path = "./daily_ext.csv"
hourly_path = "./hour.csv"

_parser = lambda x,y : pd.datetime.strptime(x+'0'*(4-len(y))+y, "%Y%m%d%H%M")

dst_start =[20160314, 20170313, 20180312]
dst_end = [20161107, 20171103, 20181105]

def load_data():
    return pd.read_csv(path, parse_dates={'ts':['SDATE','UTCBARTS']}, index_col='ts', date_parser=_parser) 

def load_data_csv(csv_path):
    return pd.read_csv(csv_path, parse_dates={'ts':['SDATE','UTCBARTS']}, index_col='ts', date_parser=_parser) 

def load_daily_data(csv_path):
    return  pd.read_csv(csv_path, index_col=[0], date_parser=lambda x:pd.datetime.strptime(x, "%Y%m%d"))
 

def __extract_dates():
    df = load_data()
    udf = df.drop_duplicates('UTCDATE')
    print "date,start_hour"
    for i, row in udf.iterrows():
        print str(row["UTCDATE"]) + ',' + str(i.hour)
 
def __extract_daily():
    dates = pd.read_csv(start_hour_path)
    df = load_data()
    print "date,open,high,low,close,size"
    for i, row in dates.iterrows():
        po = ph = pc = 0
        size = 0
        pl = 100000000
        ndate = row["date"]
        ddf = slice(df, ndate, ["OPEN", "HIGH", "LOW", "LAST","TRADES"])
        for j, drow in ddf.iterrows():
            if po == 0:
                po = drow["OPEN"]
            pc = drow["LAST"]
            if ph < drow["HIGH"]:
                ph = drow["HIGH"]
            if pl > drow["LOW"]:
                pl = drow["LOW"]
            size += drow["TRADES"]
        res = []
        res.append(str(ndate))
        res.append(str(po))
        res.append(str(ph))
        res.append(str(pl))
        res.append(str(pc))
        res.append(str(int(size)))
        print ','.join(res)


def __extract_daily_ext():
    print "date,open,high,low,close,volume,num_min_bar,start_hour,end_hour,max_hourly_vol,hour_max_vol,max_hourly_change,hour_max_change,ave_hourly_change"
    df = load_data()
    udf = dates(df)
    for i, row in udf.iterrows():
        sdf = slice(df, row["UTCDATE"], ["OPEN", "HIGH", "LOW", "LAST","TRADES"]) 
        num_min_bar = len(sdf)
        volume = 0
        open = high = close = -1
        low = 100000000
        current_hour = start_hour = end_hour = -1
        hourly_vol = []
        hourly_change = []
        current_hour_vol = 0
        current_hour_start = 0
        res = []
        for j, srow in sdf.iterrows():
            if open == -1:
                open = srow["OPEN"]
            if high < srow["HIGH"]:
                high = srow["HIGH"]
            if low > srow["LOW"]:
                low = srow["LOW"]
            close = srow["LAST"]
            volume += srow["TRADES"]
            if current_hour != j.hour:
                if start_hour == -1:
                    start_hour = j.hour
                else:
                    if j.hour - start_hour  > 17:
                        break
                end_hour = j.hour
                if current_hour != -1:
                    hourly_vol.append(current_hour_vol)
                    hourly_change.append(srow["OPEN"]-current_hour_start)
                    if j.hour - current_hour != 1:
#                        print "missing hour: ", j.strftime("%Y%m%d%H%M%S"), j.hour, current_hour, srow["OPEN"], srow["HIGH"], srow["LOW"], srow["LAST"]
                        hourly_vol += [0]*(j.hour-current_hour-1)
                        hourly_change += [0]*(j.hour-current_hour-1)
                current_hour_vol = 0
                current_hour_start = srow["OPEN"]
                current_hour = j.hour
            else:
                current_hour_vol += srow["TRADES"]
        hourly_vol.append(current_hour_vol)
        hourly_change.append(close-current_hour_start)
        if end_hour-start_hour!= len(hourly_vol) - 1:
            print "inconsistent hourly results: ", start_hour, end_hour, len(hourly_vol)
            continue
        max_hourly_vol = max(hourly_vol)
        hour_max_vol = hourly_vol.index(max_hourly_vol) + start_hour
        abs_hourly_change = []
        for hc in hourly_change:
            if hc > 0:
                abs_hourly_change.append(hc)
            else:
                abs_hourly_change.append(-1 * hc)
        max_hourly_change = max(abs_hourly_change)
        hour_max_change = abs_hourly_change.index(max_hourly_change)
        max_hourly_change = hourly_change[hour_max_change]
        hour_max_change += start_hour
        ave_hourly_change = sum(abs_hourly_change)*1.0/len(abs_hourly_change)

        res.append(str(row["UTCDATE"]))
        res.append(_strp(open))
        res.append(_strp(high))
        res.append(_strp(low))
        res.append(_strp(close))
        res.append(_strp(int(volume)))
        res.append(str(len(sdf)))
        res.append(str(start_hour))
        res.append(str(end_hour))
        res.append(_strp(max_hourly_vol))
        res.append(str(hour_max_vol))
        res.append(_strp(max_hourly_change))
        res.append(str(hour_max_change))
        res.append("%.2f"%ave_hourly_change)
        print ','.join(res) 


def __extract_hourly():
    dates = pd.read_csv(start_hour_path)
    df = load_data()
    print "date,hour,open,high,low,close,size"
    for i, row in dates.iterrows():
        po = ph = pc = 0
        size = 0
        pl = 100000000
        ndate = row["date"]
        ddf = slice(df, ndate, ["OPEN", "HIGH", "LOW", "LAST","TRADES"])
        hour = -1
        for j, drow in ddf.iterrows():
            cur_hour = j.hour
            if hour != cur_hour:
                if hour!= -1:
                    print str(ndate) + ',' + str(hour) + ',' + str(po) + ',' + str(ph) + ',' + str(pl) + ',' + str(pc) + ',' + str(int(size)) 
                hour = cur_hour
                po = ph = pc = 0
                size = 0
                pl = 1000000
                
            if po == 0:
                po = drow["OPEN"]
            pc = drow["LAST"]
            if ph < drow["HIGH"]:
                ph = drow["HIGH"]
            if pl > drow["LOW"]:
                pl = drow["LOW"]
            size += drow["TRADES"]
        print str(ndate) + ',' + str(hour) + ',' + str(po) + ',' + str(ph) + ',' + str(pl) + ',' + str(pc) + ',' + str(int(size)) 


def slice(df, ndate, cols):
    return df.loc[df["UTCDATE"]==ndate][cols]

def dates(df):
    return df.drop_duplicates('UTCDATE')

def filter(df, valve = 10, over_night = True, print_all = False):
    print "date,startmin,endmin,c0,c1,c17,c18,ch17_18,ch17_0,gap,ch1_0"
    udf = pd.read_csv(start_hour_path)
    prev_c17 = prev_c18 = "n"
    for i, row in udf.iterrows():
        ents = []
        ddate = row["date"] 
        strdate = str(ddate)
        dsdate = False
        for j in range(3):
            if dst_start[j] <= ddate <= dst_end[j]:
                dsdate = True
                break
        if not dsdate:
            continue
        ents.append(strdate)
        openhour = datetime.strptime(strdate+"0100", "%Y%m%d%H%M")
        closehour = datetime.strptime(strdate+"1700", "%Y%m%d%H%M")
        ddf = df.loc[df["UTCDATE"] == ddate]
        startmin = ddf.index[0]
        endmin = ddf.index[-1]
        ents.append(startmin.strftime("%H%M"))
        ents.append(endmin.strftime("%H%M"))
        if startmin.hour != 0:
            c0 = c1 = "n"
        else:
            ohdf = ddf.loc[ddf.index < openhour]
            c0 = ohdf.iloc[0][15]
            c1 = ohdf.iloc[-1][15]
        ents.append(str(c0))
        ents.append(str(c1))
        if endmin.hour != 17: 
            c17 = c18 = "n"
        else:
            chdf = ddf.loc[ddf.index >= closehour]
            c17 = chdf.iloc[0][15]
            c18 = chdf.iloc[-1][15]
        ents.append(str(c17))
        ents.append(str(c18))
        if prev_c17 == "n":
            ch17_18 = "n"
        else:
            ch17_18 = prev_c18 - prev_c17
        ents.append(str(ch17_18))
        if prev_c17 == "n" or c0 == "n":
            ch17_0 = "n"
        else:
            ch17_0 = c0 - prev_c17
        ents.append(str(ch17_0))
        prev_c17 = c17
        prev_c18 = c18

        measure_gap = ch17_0
        if not over_night:
            measure_gap = ch17_18
        measure_gap_abs = measure_gap if measure_gap >=0 else (0-measure_gap)

        if measure_gap_abs < valve or measure_gap == "n":
            ents.append("n")
            ents.append("n")
        else:
            ents.append(str(measure_gap_abs))
            ents.append(str(c1 -c0))
        line = ','.join(ents) 

        if 'n' not in line or print_all:
            print line




def plotbar(df, ndate, col="OPEN"):
    ddf = slice(df, ndate, ["OPEN", "HIGH", "LOW", "LAST","TRADES"])
    ddf.plot(y=col)
    plt.show()

def plotdailybar(daily_df, col="open"):
    daily_df.plot(y=col)
    plt.show()


#print slice(20180102, ["OPEN", "HIGH", "LOW", "LAST","TRADES","TICKS","CHANGECMO","IBS"])
#__extract_hourly()
#__extract_daily_ext()
#exit()

#filter(load_data())
#exit()
#plotdailybar()
