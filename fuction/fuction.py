import logging
import time, csv
import statistics as stats
from lithops import Storage
from lithops import FunctionExecutor
from lithops.utils import setup_lithops_logger

setup_lithops_logger(logging.DEBUG)

# global var
ave = []
mo = []
med = []


def getdates(bucket_name, obj_key, year, type, storage):
    print('I am processing the object //{}/{}'.format(bucket_name, obj_key))
    list = []
    data = storage.get_object(bucket_name, obj_key)
    for line in data.splitlines():
        for word in line.decode('utf-8').split(';'):
            if type == 'units' and word.isdigit() and word != year:
                list.append(word)
            if type == 'country' and word.isalpha() and word != 'country' and word != 'units' and word != 'year':
                list.append(word)

    return list


def average(bucket_name, obj_key, year, type, storage):
    global ave
    l = getdates(bucket_name, obj_key, year, type, storage)
    r = map(int, l)
    result = stats.mean(r)
    result = round(result, 2)
    ave.append(result)
    return result


def mode(bucket_name, obj_key, year, type, storage):
    global med
    l = getdates(bucket_name, obj_key, year, type, storage)
    r = map(int, l)
    result = stats.mode(r)
    result = round(result, 2)
    med.append(result)
    return result


def median(bucket_name, obj_key, year, type, storage):
    global mo
    l = getdates(bucket_name, obj_key, year, type, storage)
    r = map(int, l)
    result = stats.median(r)
    result = round(result, 2)
    mo.append(result)
    return result


def store(file):
    global ave,med,mo
    year = ['2000', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010',
            '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020']
    with open(file, 'w') as csvfile:
        fieldnames = ['year', 'average', 'median', 'mode']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        j = 0
        for i in year:
            writer.writerow({'year': i, 'average': ave[j], 'median': med[j], 'mode': mo[j]})
            j += 1
    return file


if __name__ == "__main__":
    storage = Storage()
    numbers = ['2000', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010',
               '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020']
    i = 0
    bucket_name = 'carbigdata2'
    obj_key = 'car2000.csv'
    name = 'a.csv'
    with FunctionExecutor() as exe:
        while i < len(numbers) - 1:
            start = time.time()
            exe.call_async(average, (bucket_name, obj_key, numbers[i], 'units'))
            exe.call_async(median, (bucket_name, obj_key, numbers[i], 'units'))
            exe.call_async(mode, (bucket_name, obj_key, numbers[i], 'units'))
            end = time.time()
            obj_key = obj_key.replace(numbers[i], numbers[i + 1])
            i += 1
        exe.call_async(store, (name,))
