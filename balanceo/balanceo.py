import hashlib
import os
import uuid


listaDec = []

listaSHA = []

rangos = {
}

for item in range(500):
    cadena = uuid.uuid4()
    item = hashlib.new("sha1", cadena.bytes)
    num = int(item.hexdigest(), 16)
    listaDec.append(num)


listaDec.sort()

for item in listaDec:
    rangos[item] = []

""" for file in range(2000):
    namefile = 'archivo-{}.txt'.format(file)
    with open(namefile, 'wb') as fout:
        fout.write(os.urandom(1024)) """


for file in range(2000):
    namefile = 'archivo-{}.txt'.format(file)
    with open(namefile, 'rb') as f: 
        listaSHA.append(hashlib.sha1(f.read()).hexdigest())



for item in listaSHA:
    num = int(item, 16)
    for dec in listaDec:
        if num < dec:
            rangos[dec].append(num)
            break
        if num >= listaDec[-1]:
            rangos[listaDec[0]].append(num)
            break


""" for item in listaDec:
    print(item)
print('')
for item in listaSHA:
    print(int(item, 16))
print('') """
for item in rangos.keys():
    print(item)
    print(len(rangos.get(item)))
    print('')


