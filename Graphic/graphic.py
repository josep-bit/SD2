import matplotlib.pyplot as plt
from lithops import Storage, FunctionExecutor
import statistics as stats

def split(word):
    return str(word).split(',')


def cretelistAtributs(atr, storage):
    listrow = []
    i = 0
    data = storage.get_object('carbigdata2', 'results.csv')
    for line in data.splitlines():
        for word in line.decode('utf-8').split(';'):
            if i != 0:
                w = split(word)
                if atr == 'numberofword':
                    listrow.append(w[1])
                if atr == 'Covid-19':
                    listrow.append(w[2])
                if atr == 'numberoflines':
                    listrow.append(w[3])
                if atr == 'numberofspaces':
                    listrow.append(w[4])
                if atr == 'sentimentanalysis':
                    listrow.append(w[5])
            i += 1
    return listrow


def average(storage):
    nw = cretelistAtributs('numberofword', storage)
    r = map(int, nw)
    r1 = stats.mean(r)
    covid = cretelistAtributs('Covid-19', storage)
    r = map(int, covid)
    r2 = stats.mean(r)
    nl = cretelistAtributs('numberoflines', storage)
    r = map(int, nl)
    r3 = stats.mean(r)
    ns = cretelistAtributs('numberofspaces', storage)
    r = map(int, ns)
    r4 = stats.mean(r)
    av = [round(r1, 2), round(r2, 2), round(r3, 2), round(r4, 2)]
    return av


def graphic(av):
    x = ['numberofword', 'Covid-19', 'numerberoflines', 'numberofspaces']
    fig, ax = plt.subplots()
    ax.set_ylabel('average')
    ax.set_title('average study')
    plt.bar(x, av)
    plt.savefig('bar_simple.png')
    plt.show()


def listsentimentanalisi(storage):
    return cretelistAtributs('sentimentanalysis', storage)


def graphicanalisi(lista):
    p = lista.count('Positive')
    ng = lista.count('Negative')
    ne = lista.count('Neutral')
    pp = p * 100 / 975
    png = ng * 100 / 975
    pne = ne * 100 / 975
    sentimentanalisi = 'Positive', 'Negative', 'Neutral'
    sizes = [pp, png, pne]
    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, labels=sentimentanalisi, autopct='%1.1f%%',
            shadow=True, startangle=90)
    ax1.axis('equal')
    plt.title("Sentiment Analysis")
    plt.legend()
    plt.savefig('circular.png')
    plt.show()


if __name__ == '__main__':
    storage = Storage()
    with FunctionExecutor() as exe:
        exe.call_async(average, ())
        av = exe.get_result()
        print(av)
        exe.call_async(listsentimentanalisi, ())
        sa = exe.get_result()
        print(sa)
