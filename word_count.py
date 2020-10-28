from collections import Counter

with open('./resources/hamlet.txt', 'r') as f, open('benchmark_wc_out.txt', 'w') as f_out:
    word_ctr = Counter()
    for line in f:
        words = line.strip().split(' ')
        for word in words:
            word_ctr[word] += 1
    for word, cnt in word_ctr.items():
        f_out.write('(' + word + '%&%' + str(cnt) + ')' + '\n')
