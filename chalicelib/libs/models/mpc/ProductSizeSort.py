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
