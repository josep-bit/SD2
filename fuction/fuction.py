import logging
import csv, time
from lithops.storage.cloud_proxy import open
from functools import reduce
from lithops import Storage
from lithops import FunctionExecutor
from lithops.utils import setup_lithops_logger
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()
setup_lithops_logger(logging.DEBUG)


def alpha(x):
    if x.isalnum() or x.isalpha() or x.isdigit() or x.__contains__('-,.%:'):
        return 'True'
    else:
        return 'False'


def getnumberwords(bucket_name, obj_key, storage):
    listn = []
    listnumber = []
    for i in obj_key:
        data = storage.get_object(bucket_name, i)
        for line in data.splitlines():
            for word in line.decode('utf-8').split(';'):
                w = split(word)
                x = map(alpha, w)
                z = list(x).count('True')
                listn.append(z)
        if len(listn) != 0:
            n = reduce(lambda x, y: x + y, listn)
            listnumber.append(n)
        else:
            listnumber.append(0)
        listn.clear()
    return listnumber


def getnumberoflines(bucket_name, obj_key, storage):
    listl = []
    listline = []
    for i in obj_key:
        data = storage.get_object(bucket_name, i)
        for line in data.splitlines():
            for word in line.decode('utf-8').split(';'):
                listl.append(word)
        n = len(listl)
        listline.append(n)
        listl.clear()
    return listline


def exetime(ini, end):
    r = round(end - ini, 2)
    dic = {'Min': round(r / 60, 2), 'Seg': r}
    return dic


def getnumberofwhitespace(bucket_name, obj_key, storage):
    c = 0
    listspace = []
    for i in obj_key:
        data = storage.get_object(bucket_name, i)
        for line in data.splitlines():
            for word in line.decode('utf-8').split(';'):
                c = c + word.count(' ')
        listspace.append(c)
        c = 0
    return listspace


def getfilterwordnumber(bucket_name, obj_key, covid, storage):
    listf = []
    listcovid = []
    for i in obj_key:
        data = storage.get_object(bucket_name, i)
        for line in data.splitlines():
            for word in line.decode('utf-8').split(';'):
                w = split(word)
                result = filter(lambda x: covid in x, w)
                listf.append(len(list(result)))
        if len(listf) != 0:
            m = map(int, listf)
            n = reduce(lambda x, y: x + y, m)
            listcovid.append(n)
            listf.clear()
        else:
            listcovid.append(0)
    return listcovid


def analisi(x):
    vs = analyzer.polarity_scores(x)
    if vs['compound'] > 0:
        return 'positive'
    if vs['compound'] < 0:
        return 'negative'
    if ['compound'] == 0.5:
        return 'neutral'


def sentimalAnalisi(bucket_name, obj_key, storage):
    correct = 0
    neutral = 0
    negative = 0
    listsanalisi = []
    for i in obj_key:
        data = storage.get_object(bucket_name, i)
        for line in data.splitlines():
            for word in line.decode('utf-8').split(';'):
                w = split(word)
                x = map(analisi, w)
                result = list(x)
                correct = correct + result.count('positive')
                negative = negative + result.count('negative')
                neutral = neutral + result.count('neutral')
        m = max(correct, neutral, negative)

        if m == correct:
            listsanalisi.append('Positive')
        if m == negative:
            listsanalisi.append('Negative')
        else:
            listsanalisi.append('Neutral')
        correct = 0
        neutral = 0
        negative = 0
    return listsanalisi


def createdlist(n):
    l = []
    for i in range(1, n):
        date = 'data' + str(i) + '.txt'
        l.append(date)
    return l


def fcsv(obj_key, nw, nl, ns, a, covid):
    results = 'results.csv'
    with open(results, 'w') as f:
        fieldnames = ['file', 'numberofword', 'Covid-19', 'numerberoflines', 'numberofspaces', 'sentimentanalysis']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        j = 0
        n = len(obj_key)
        while j < n:
            writer.writerow({'file': obj_key[j], 'numberofword': nw[j], 'Covid-19': covid[j],
                             'numerberoflines': nl[j],
                             'numberofspaces': ns[j], 'sentimentanalysis': a[j]})
            j += 1

    return 'ok'


def exetimeforfile(bucket_name, covid, storage):
    n = 15
    l_time = []
    obj_key = createdlist(n)

    while n < 976:
        ini = time.time()
        getnumberwords(bucket_name, obj_key, storage)
        getfilterwordnumber(bucket_name, obj_key, covid, storage)
        getnumberofwhitespace(bucket_name, obj_key, storage)
        getnumberoflines(bucket_name, obj_key, storage)
        sentimalAnalisi(bucket_name, obj_key, storage)
        end = time.time()
        l_time.append(exetime(ini, end))
        n = n + 15
        obj_key = createdlist(n)

    return l_time


def timecsv(l_times):
    r_times = 'times.csv'
    with open(r_times, 'w') as f:
        fieldnames = ['numberoffile', 'Minutes', 'Seconds']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        j = 0
        i = 15
        n = len(l_times)
        while j < n:
            writer.writerow({'numberoffile':i , 'Minutes':l_times[j].get('Min'), 'Seconds':l_times[j].get('Seg')})
            j += 1
            i +=15
    return 'ok'


def split(word):
    return str(word).split(" ")


if __name__ == "__main__":
    storage = Storage()
    bucket_name = 'carbigdata2'
    print("Choose the option:")
    print("1-time for files")
    print("2-Complete analysis")
    choose = input()
    if choose =='1':
        with FunctionExecutor() as exe:
            exe.call_async(exetimeforfile, (bucket_name,'covid'))
            result = exe.get_result()
            exe.call_async(timecsv, result)
            print(exe.get_result())
    if choose == '2':
        obj_key = createdlist(976)
        with FunctionExecutor() as exe:
            ini = time.time()
            exe.call_async(getfilterwordnumber, (bucket_name, obj_key, 'Covid-19'))
            covid = exe.get_result()
            exe.call_async(getnumberwords, (bucket_name, obj_key))
            nw = exe.get_result()
            exe.call_async(getnumberoflines, (bucket_name, obj_key))
            nl = exe.get_result()
            exe.call_async(getnumberofwhitespace, (bucket_name, obj_key))
            ns = exe.get_result()
            exe.call_async(sentimalAnalisi, (bucket_name, obj_key))
            a = exe.get_result()
            fi = time.time()
            exe.call_async(exetime, (ini, fi))
            print(exe.get_result())
            exe.call_async(fcsv, (obj_key, nw, nl, ns, a, covid))
            print(exe.get_result())
