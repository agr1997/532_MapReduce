from collections import defaultdict, OrderedDict

inv_idx = defaultdict(list)
with open('hamlet_line_num.txt', 'r') as input:
    for line in input:
        words = line.split()
        line_num = int(words[0])
        for word in words[1:]:
            inv_idx[word].append(line_num)

od = OrderedDict(sorted(inv_idx.items()))
with open('benchmark_index_out.txt', 'w') as output:
    for k, v in od.items():
        #output.write(k + \t)
        output.write(k + ' ' + str(v) + '\n')