import pandas as pd
import os
import pyopencl as cl
import requests
import json
import datetime
import numpy as np
from multiprocessing import Pool

from secret import key
from us_state_abbrev import us_state_to_abbrev as abrev


def parallel_work(num, url):
    dt = '%Y-%m-%d'
    year = 2010 + num
    url_addend = "&startDate=" + str(year) + "-04-15&endDate=" + str(year) + "-10-31&format=JSON"
    data = np.zeros(0)
    stuff1 = requests.get(url + url_addend).text
    stuff = json.loads(stuff1)
    day = datetime.timedelta(days=1)
    yesterday = datetime.datetime.strptime(str(year) + '-04-14', dt)
    for attribute in stuff:
        try:
            today = datetime.datetime.strptime(attribute['DATE'], dt)
            time_between = today - yesterday
            if (time_between != day):
                for days in range(time_between.days):
                    data = np.append(data, 0)
            else:
                try:
                    data = np.append(data, int(attribute['TMAX'].strip()))
                except:
                    data = np.append(data, 0)
            yesterday = today
        except:
            yesterday = yesterday + day
    print(year)
    pdframe = pd.DataFrame(data=data, columns=['Max temp'])

    if pdframe.empty:
        data = np.zeros(200)
    print(pdframe)
    if data.size < 200:
        file2 = open('ohgod.txt', 'w')
        file2.write(stuff1)
        file2.close()
    return data


def temp_per_state(state_alpha):
    if __name__ == '__main__':
        temp_data = []
        file = open("coop-stations.txt")
        counter = 0
        done_flag = False
        for line in file:
            if state_alpha in line[93:97]:
                url = "https://www.ncei.noaa.gov/access/services/data/v1?dataset=daily-summaries&stations="
                done_flag = True
                if 'USC' in line[25:37] or 'USW' in line[25:37] or 'USE' in line[25:37]:
                    url += line[25:37]
                    with Pool() as pool:
                        temporary = pool.starmap(parallel_work, zip(range(0, 10), [url for x in range(10)]))
                        temp_data.append(temporary)
                        counter += 1
            elif done_flag or counter >= 5:
                break
        file.close()
        return temp_data


def get_states():
    states_info = json.loads(
        requests.get("https://quickstats.nass.usda.gov/api/get_param_values/?key=" + key + "&param=state_alpha").text)
    states_yield = {}
    for state in states_info['state_alpha']:
        try:
            yield_info = json.loads(requests.get(
                "https://quickstats.nass.usda.gov/api/api_GET/?key=" + key + "&commodity_desc=SWEET CORN&year__GE=2012&state_alpha=" + state).text)
            num = yield_info['data'][0]["Value"]
            num.replace(',', '')
            states_yield[state] = int(num)
            print(states_yield[state])
        except:
            print(state + " not working at the moment")

    return states_yield


def develop_multiplier(arrs):
    frame = pd.DataFrame()
    counter = 0
    for arr in arrs:
        highs_per_day = np.transpose(arr)
        highs_per_day_avg = np.zeros(200, dtype="uint16")
        ctx = cl.create_some_context()
        queue = cl.CommandQueue(ctx)

        highs_per_day.flatten()

        mf = cl.mem_flags

        fin_arr_buf = cl.Buffer \
            (ctx, mf.WRITE_ONLY, highs_per_day_avg.nbytes)
        prg = cl.Program(ctx, """
            __kernel void avg(__global int *pArr, __global int *finArr) {
            
                int gid = get_global_id(0);
                int counter = 0;
                int sum = 0;
                
                __global int* startPos = &pArr[gid * 10];
                
                for (int idx = 0; idx < 10; idx++) {
                    int current = *(startPos + idx);
                    if( current > 0) {
                    
                        sum += current;
                        counter++;
                    
                    }
                
                }
            if (counter != 0)
                finArr[gid] = (sum / counter);
                
            }
        """).build()
        arr_buf = cl.Buffer \
            (ctx, mf.READ_ONLY | mf.COPY_HOST_PTR, hostbuf=highs_per_day)
        prg.avg(queue, highs_per_day_avg.shape, None, arr_buf, fin_arr_buf)
        fin = np.empty_like(highs_per_day_avg, dtype="uint16")
        cl.enqueue_copy(queue, fin, fin_arr_buf)
        frame[str(counter)] = fin
        counter+=1
    print(frame)
    derived = pd.DataFrame(columns=['AVG', 'STD'])
    counter = 0
    for num in range(200):
        ser = frame.iloc[num].reset_index(drop=True).squeeze()
        derived.loc[str(counter)] = [ser.mean(), ser.std()]
        counter+=1
    return derived


def complete_multiplier():
    

develop_multiplier([np.ones((200, 10), dtype="uint16"), np.ones((200,10), dtype="uint16")])


