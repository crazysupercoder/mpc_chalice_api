import collections, re
class ProductSizeSort:
    def sortSizes(self, fullsizes):
        original = fullsizes

        for k, v in original.items():
            if k.upper().replace(' ', '') == 'ONESIZE':
                del original[k]
                original['ONE_SIZE'] = v
            else:
                original[k] = v

        fullsizes = list(fullsizes.keys())

        fullsizes = [val.strip() for val in fullsizes]

        new = {}

        for s in fullsizes:
            new[s] = s

        split = {}

        for k, v in new.items():
            k = k.strip().upper()
            f = False

            if not f and (k[-2:] == 'ML' or k[-2:] == 'ml'):
                k = k.replace(' ', '')
                if not 'ML' in split:
                    split['ML'] = {}
                split['ML'][k] = v

                f = True

            if not f and k.find('YEAR') != -1:
                k = k.replace('YEARS', 'YEAR')
                k = k.replace(' ', '')
                if not 'YEAR' in split:
                    split['YEAR'] = {}
                split['YEAR'][k] = v
                f = True

            if not f and k.find('MONTH') != -1:
                if not 'MONTH' in split:
                    split['MONTH'] = {}
                split['MONTH'][k] = v
                f = True

            if not f and k.find('SINGLE') != -1:
                if not 'SINGLE' in split:
                    split['SINGLE'] = {}
                split['SINGLE'][k] = v
                f = True

            if not f and k in ['RRH', 'LRH']:
                if not 'RRHLRH' in split:
                    split['RRHLRH'] = {}
                split['RRHLRH'][k] = v
                f = True

            if not f and k.find('ONE') != -1 and k.find('SIZE') != -1:
                if not 'ONE_SIZE' in split:
                    split['ONE_SIZE'] = {}
                split['ONE_SIZE']['One Size'] = 'ONE_SIZE'
                f = True

            if not f and k.find('W') != -1 and k.find('L') != -1:
                if not 'WL' in split:
                    split['WL'] = {}
                split['WL'][k] = v
                f = True

            if not f and k.find('W') != -1 and k[-1] == 'W':
                if not 'W' in split:
                    split['W'] = {}
                split['W'][k] = v
                f = True

            if (
                    not f and len(k) > 2 and k[-1] in ['A', 'B', 'C', 'D', 'DD', 'E', 'F', 'G', 'H', 'I', 'J'] and
                    not (k in ['LRH', 'ONE SIZE', 'ONE_SIZE', 'PREEMIE', 'RRH', 'SINGLE'])
                ):
                if not 'BRA' in split:
                    split['BRA'] = {}
                split['BRA'][k] = v
                f = True

            if (
                    not f and len(k) == 3 and k[-1] in ['L', 'R'] and
                    not (k in ['LRH', 'ONE SIZE', 'ONE_SIZE', 'PREEMIE', 'RRH', 'SINGLE', 'PETITE NEWBORN'])
                ):
                if (
                    self.isnumeric(k.replace('L', '')) or
                    self.isnumeric(k.replace('R', ''))
                ):
                    if not 'JACKETS' in split:
                        split['JACKETS'] = {}
                    split['JACKETS'][k] = v
                    f = True

            if (
                    not f and
                    k in ['NEWBORN', 'PREEMIE', 'PETITE NEWBORN']
                ):
                if not 'BABY' in split:
                    split['BABY'] = {}
                split['BABY'][k] = v
                f = True

            if k.find('CM') != -1:
                if not 'CM' in split:
                    split['CM'] = {}
                split['CM'][k] = v
                f = True

            if not f and self.isnumeric(k):
                if not 'NUM' in split:
                    split['NUM'] = {}
                split['NUM'][k] = v
                f = True

            if not f:
                if '-' in k:
                    kk = k.split('-')
                    isnum = 0
                    for kv in kk:
                        if self.isnumeric(kv):
                            isnum += 1
                    if isnum == len(kk):
                        if not 'NUM' in split:
                            split['NUM'] = {}
                        split['NUM'][k] = v
                        f = True
                if not f:
                    if not '_' in split:
                        split['_'] = {}
                    split['_'][k] = v

        # print(split)

        newList = {}
        if 'NUM' in split:
            for k, v in split['NUM'].items():
                if '-' in k:
                    exp = k
                    s = k[:k.find('-')]
                    if not s in newList:
                        newList[s] = {}
                    newList[s][k] = v
                else:
                    if not k in newList:
                        newList[k] = {}
                    newList[k][k] = v

            for k, v in newList.items():
                newList[k] = v
                newList[k] = self.numsort(newList[k])
                newList = self.numsort(newList)
            split['NUM'] = newList

        # print(newList)
        # print(split)

        if '_' in split:
            split['_'] = self.ksort(split['_'])

            othersizes = split['_']
            other2 = {}
            kmap = {}

            for k, v in othersizes.items():
                k = k.replace('/', '-')
                kk = k

                if k[-2:] == 'XL' or k[-2:] == 'XS':
                    x = k[:-2]
                    if self.isnumeric(x):
                        x = self.explod(x)
                        k = x + k[-2:]


                kmap[self._getSizeVal(k)] = kk
                
                kmap = self.numsort(kmap)
            for k, v in kmap.items():
                if v in othersizes:
                    other2[v] = othersizes[v]
            split['_'] = other2

        # print(kmap)
        # print(split)

        if 'ML' in split:
            ml = split['ML']
            ml = self.numsort(ml)
            newMl = {}
            for k in ml.keys():
                newMl[k] = split['ML'][k]
            split['ML'] = newMl

        if 'YEAR' in split:
            years = split['YEAR']
            years = self.numsort(years)
            newYear = {}
            for k in years.keys():
                newYear[k] = split['YEAR'][k]
            split['YEAR'] = newYear

        # print(split)

        if 'MONTH' in split:
            months = split['MONTH']
            months = self.numsort(months)
            newMonth = {}
            for k in months.keys():
                newMonth[k] = split['MONTH'][k]
            split['MONTH'] = newMonth
        
        if 'CM' in split:
            split['CM'] = self.numsort(split['CM'])

        if 'BRA' in split:
            split['BRA'] = self.ksort(split['BRA'])

        if 'WL' in split:
            split['WL'] = self.ksort(split['WL'])

        if 'W' in split:
            split['W'] = self.ksort(split['W'])

        if 'JACKETS' in split:
            split['JACKETS'] = self.numsort(split['JACKETS'])

        if 'RRHLRH' in split:
            split['RRHLRH'] = self.ksort(split['RRHLRH'])

        # print(split)

        alphaCount = 0
        smlCount = {'S': 0, 'M': 0, 'L': 0}
        if '_' in split:
            for k, v in split['_'].items():
                if len(k) == 1 and ord(k) > 64 and ord(k) < 91:
                    alphaCount+=1
                    if k in ['S', 'M', 'L']: 
                        smlCount[k]+=1


            if alphaCount > 3:
                if 'W' in split: 
                    if not ('_' in split and 'W' in split['_']): 
                        del split['W']
                        split['_']['W'] = 'W'

                split['_'] = self.ksort(split['_'])

            # print(split)
            if (smlCount['S'] + smlCount['M'] + smlCount['L'] < 2 and alphaCount > 0):
                split['_'] = self.ksort(split['_'])
            # print(split)

        total = {}
        if 'ONE_SIZE' in split:
            total['ONE_SIZE'] = split['ONE_SIZE']
        if '_' in split:
            total['_'] = split['_']
        if 'BABY' in split:
            total['BABY'] = split['BABY']
        if 'MONTH' in split:
            total['MONTH'] = split['MONTH']
        if 'YEAR' in split:
            total['YEAR'] = split['YEAR']
        if 'ML' in split:
            total['ML'] = split['ML']
        if 'BRA' in split:
            total['BRA'] = split['BRA']
        if 'CM' in split:
            total['CM'] = split['CM']
        if 'JACKETS' in split:
            total['JACKETS'] = split['JACKETS']
        if 'BEDDING' in split:
            total['BEDDING'] = split['BEDDING']
        if 'RRHLRH' in split:
            total['RRHLRH'] = split['RRHLRH']
        if 'WL' in split:
            total['WL'] = split['WL']
        if 'W' in split:
            total['W'] = split['W']
        if 'NUM' in split:
            total['NUM'] = split['NUM']

        res = {}

        for sub in total.values():
            for k, v in sub.items():
                if isinstance(v, dict):
                    for vv in v.values():
                        try:
                            res[vv] = original[vv]
                        except:
                            continue
                else:
                    try:
                        res[k] = original[v]
                    except:
                        continue
        return res

    def array_flip(self, trans):
        if trans is not None:
            return {v:k for k,v in trans.items()}
        return None

    def _getSizeVal(self, size):
        if size.find('-') != -1:
            self.exploded = size.split('-')
            return self._getSizeVal(self.exploded[0]) + '-' + self._getSizeVal(self.exploded[1])
        else:
            base = size[-1:]
            baseVal = {'S': 2000, 'M': 4000, 'L': 6000}
            if size.find('X') != -1:
                temp = size[:-1]
                xs = len(temp)
            else:
                xs = 0

            if base == 'S':
                index = baseVal[base]
                index = index - int(xs) * 10
            elif base == 'M':
                index = baseVal[base]
            elif base == 'L':
                index = baseVal[base]
                index = index + int(xs) * 10
            else:
                index = 1 + ord(base)
            return str(index)

    def ksort(self, d):
        return {k: d[k] for k in sorted(d.keys())}

    def explod(self, x):
        r = ''
        for i in range(6):
            r += 'X'
        return r

    def isnumeric(self, s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def numsort(self, data):
        numberDict = {}

        # get first word of items
        for item in data.keys():
            word = re.split('-| ', item)[0]
            number = re.findall(r'^\D*(\d*\.?\d+)', word)
            if number:
                numberDict[item]=float(number[0])
        # sort number dict
        sorted_x = sorted(numberDict.items(), key=lambda kv: kv[1])
        numberDict = collections.OrderedDict(sorted_x)

        # get dict with same number
        new_dict = {}
        for key, value in numberDict.items():
            new_dict.setdefault(value, []).append(key)

        # sort number dict key
        numberDicts = []
        for key, values in new_dict.items():
            temp = {}
            values.sort()
            temp[key] = values
            numberDicts.append(temp)

        # make result
        ret = list()
        for _dict in numberDicts:
            for value in _dict.values():
                ret += list(value)

        return { k:data[k] for k in ret}

    def makeDict(self, sizes):
        v = 1
        ret = {}
        for size in sizes:
            ret[size] = v
            v+=1
        return ret

    def sort(self, sizes):
        return list(self.sortSizes(self.makeDict(sizes)).keys())


test = [
    "iPhone X-XS",
    "iPhone 7/iPhone 8",
    "XS-S",
    "XS",
    "XL",
    "Samsung 7G-8G",
    "S-M",
    "S",
    "Preemie",
    "P",
    "One Size",
    "O",
    "Newborn",
    "N",
    "M-L",
    "M",
    "L-XL",
    "L",
    "K",
    "J",
    "I",
    "H",
    "G",
    "250 ML",
    "200 X 290 CM",
    "200 ML",
    "160 X 230 CM",
    "150 ML",
    "140 X 200 CM",
    "125 ML",
    "120 X 170 CM",
    "100 ML",
    "90 ML",
    "80 X 150 CM ",
    "75 ML",
    "60 X 110 CM",
    "60 ML",
    "54RG",
    "54",
    "52RG",
    "52E",
    "52DD",
    "52D",
    "52",
    "50RG",
    "50F",
    "50E",
    "50DD",
    "50D",
    "50 ML",
    "50",
    "48G",
    "48F",
    "48E",
    "48DD",
    "48D",
    "48C",
    "48B",
    "48",
    "47",
    "46R",
    "46G",
    "46F",
    "46E",
    "46DD",
    "46D",
    "46C",
    "46B",
    "46",
    "45",
    "44W34L",
    "44R",
    "44G",
    "44F",
    "44E",
    "44DD",
    "44D",
    "44C",
    "44B",
    "44-46",
    "44",
    "43",
    "42W34L",
    "42W32L",
    "42R",
    "42G",
    "42F",
    "42E",
    "42DD",
    "42D",
    "42C",
    "42B",
    "42",
    "41-46",
    "41",
    "40W34L",
    "40W32L",
    "40R",
    "40L",
    "40G",
    "40F",
    "40E",
    "40DD",
    "40D",
    "40C",
    "40B",
    "40-42",
    "40 ML",
    "40",
    "39",
    "38W34L",
    "38W32L",
    "38RG",
    "38F",
    "38E",
    "38DD",
    "38D",
    "38C",
    "38BC",
    "38B",
    "38",
    "37",
    "36W36L",
    "36W34L",
    "36W32L",
    "36W30L",
    "36RG",
    "36G",
    "36F",
    "36E",
    "36DD",
    "36D",
    "36C",
    "36BC",
    "36B",
    "36A",
    "36-40",
    "36",
    "35W32L",
    "35",
    "34W34L",
    "34W32L",
    "34W30L",
    "34G",
    "34F",
    "34E",
    "34DD",
    "34D",
    "34C",
    "34BC",
    "34B",
    "34A",
    "34",
    "33W34L",
    "33W32L",
    "33",
    "32W34L",
    "32W32L",
    "32W30L",
    "32G",
    "32F",
    "32E",
    "32DD",
    "32D",
    "32C",
    "32BC",
    "32B",
    "32AB",
    "32A",
    "32",
    "31W34L",
    "31W32L",
    "31W30L",
    "31",
    "30W34L",
    "30W32L",
    "30W30L",
    "30B",
    "30 ML",
    "30",
    "29W34L",
    "29W32L",
    "29W30L",
    "29",
    "28W34L",
    "28W32L",
    "28W30L",
    "28",
    "27W32L",
    "27W30L",
    "27",
    "26W34L",
    "26W32L",
    "26W30L",
    "26",
    "25W32L",
    "25W30L",
    "25 ML",
    "25",
    "24 Months",
    "24",
    "22",
    "20 ML",
    "20",
    "19",
    "18-24 Months",
    "18 Months",
    "18",
    "17",
    "16 Years",
    "16",
    "15-18 Months",
    "15-16 Years",
    "15 Years",
    "15 ML",
    "15",
    "14-16 Years",
    "14-15 Years",
    "14 Years",
    "14",
    "13.5",
    "13-15 Years",
    "13-14 Years",
    "13-14",
    "13-1",
    "13 Years",
    "13",
    "12.5",
    "12-3.5",
    "12-24 Months",
    "12-18 Months",
    "12-15 Months",
    "12-14 Years",
    "12-13 Years",
    "12-13",
    "12 Years",
    "12 Months",
    "12",
    "11.5",
    "11-13 Years",
    "11-12 Years",
    "11-12",
    "11 Years",
    "11",
    "10.5",
    "10-13 Years",
    "10-12 Years",
    "10-12 Months",
    "10-12",
    "10-11 Years",
    "10-11",
    "10 Years",
    "10 ML",
    "10",
    "9.5",
    "9-2",
    "9-12 Months",
    "9-12",
    "9-11 Years",
    "9-11",
    "9-10 Years",
    "9-10",
    "9 Years",
    "9 Months",
    "9",
    "8.5",
    "8-9 Years",
    "8-9",
    "8-12",
    "8-10 Years",
    "8 Years",
    "8",
    "7.5-9",
    "7.5",
    "7-9",
    "7-8 Years",
    "7-8",
    "7-10 Years",
    "7 Years",
    "7",
    "6.5",
    "6XL",
    "6-9 Months",
    "6-8 Years",
    "6-7 Years",
    "6-7",
    "6-12 Months",
    "6 Years",
    "6 Months",
    "6",
    "5.5-7",
    "5.5",
    "5XL",
    "5-7",
    "5-6 Years",
    "5-6",
    "5 Years",
    "5",
    "4.5",
    "4XL",
    "4-7 Years",
    "4-7",
    "4-6 Years",
    "4-5 Years",
    "4-5",
    "4 Years",
    "4",
    "3.5W",
    "3.5",
    "3XL",
    "3-7 Years",
    "3-6 Months",
    "3-5",
    "3-4 Years",
    "3-4",
    "3 Years",
    "3 Months",
    "3",
    "2.5",
    "2XS",
    "2XL",
    "2-4 Years",
    "2-4 Months",
    "2-3 Years",
    "2-3",
    "2 Years",
    "2",
    "1.5",
    "1-3 Years",
    "1-3 Months",
    "1-2 Years",
    "1-2 Months",
    "1-2",
    "1 Years",
    "1 Year",
    "1",
    "0-6 Months",
    "0-3 Months",
    "0-1 Years"
]

# ProductSizeSort().sort(test)
print(ProductSizeSort().sort(test))