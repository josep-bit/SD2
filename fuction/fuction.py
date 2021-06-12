import logging
from functools import reduce
from lithops import Storage
from lithops import FunctionExecutor
from lithops.utils import setup_lithops_logger
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()
setup_lithops_logger(logging.DEBUG)


def getnumberwords(bucket_name, obj_key, storage):
    listn = []
    print('I am processing the object //{}/{}'.format(bucket_name, obj_key))
    data = storage.get_object(bucket_name, obj_key)
    for line in data.splitlines():
        for word in line.decode('utf-8').split(';'):
            w = split(word)
            listn.append(len(w))
    m = map(int, listn)
    return reduce(lambda x, y: x + y, m)


def getnumberoflines(bucket_name, obj_key, storage):
    listl = []
    print('I am processing the object //{}/{}'.format(bucket_name, obj_key))
    data = storage.get_object(bucket_name, obj_key)
    for line in data.splitlines():
        for word in line.decode('utf-8').split(';'):
            listl.append(word)
    return len(listl)


def getnumberweirdcaracter(bucket_name, obj_key, storage):
    count = 0
    print('I am processing the object //{}/{}'.format(bucket_name, obj_key))
    data = storage.get_object(bucket_name, obj_key)
    for line in data.splitlines():
        for word in line.decode('utf-8').split(';'):
            if word.isalnum() is not True:
                count += 1
    return count


def getfilterwordnumber(bucket_name, obj_key, covid, storage):
    listf = []
    print('I am processing the object //{}/{}'.format(bucket_name, obj_key))
    data = storage.get_object(bucket_name, obj_key)
    for line in data.splitlines():
        for word in line.decode('utf-8').split(';'):
            w = split(word)
            result = filter(lambda x: covid in x, w)
            listf.append(len(list(result)))
    m = map(int, listf)
    return reduce(lambda x, y: x + y, m)


def sentimalAnalisi(bucket_name, obj_key, storage):
    correct = 0
    neutral = 0
    negative = 0
    file = ''
    print('I am processing the object //{}/{}'.format(bucket_name, obj_key))
    data = storage.get_object(bucket_name, obj_key)
    for line in data.splitlines():
        for word in line.decode('utf-8').split(';'):
            vs = analyzer.polarity_scores(word)
            if vs['compound'] >= 0.75:
                correct += 1
            if vs['compound'] <= 0.25:
                negative += 1
            else:
                neutral += 1
    m = max(correct, neutral, negative)
    if m == correct:
        file = 'Positive'
    if m == negative:
        file = 'Negative'
    else:
        file = 'Neutral'
    return file


def split(word):
    return str(word).split(" ")


if __name__ == "__main__":
    storage = Storage()
    bucket_name = 'carbigdata2'
    obj_key = 'data1.txt'

    with FunctionExecutor() as exe:
        exe.call_async(getnumberwords, (bucket_name, obj_key))
        print(exe.get_result())
        exe.call_async(getfilterwordnumber, (bucket_name, obj_key, 'Covid-19'))
        print(exe.get_result())
        exe.call_async(getnumberoflines, (bucket_name, obj_key))
        print(exe.get_result())
        exe.call_async(getnumberweirdcaracter, (bucket_name, obj_key))
        print(exe.get_result())
        exe.call_async(sentimalAnalisi, (bucket_name, obj_key))
        print(exe.get_result())
