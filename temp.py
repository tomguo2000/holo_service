import time

query_lst = [-60000,-6000,-600,-60,-6,0,6,60,600,6000,60000]

lst = []
dic = {}

length = 10000000

for i in range(length):
    lst.append([i,i])
    dic[i] = i

start = time.time()
for v in lst:
    print(v)
    if lst[v[0]] > 50:
        continue
end1 = time.time()

for v in dic:
    if dic[v] > 50:
        continue

end2 = time.time()

print ("list search time : %f"%(end1-start))
print ("dict search time : %f"%(end2-end1))
