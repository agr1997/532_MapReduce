with open('hamlet_line_num.txt', 'r') as input:
    lines = []
    for line in input:
        words = line.split(' ')
        line_num = words[0]
        sentence_words = words[1:]
        if len(sentence_words) > 10:
            lines.append(line_num + ' ' + ' '.join(sentence_words))
with open('benchmark_grep_out.txt', 'w') as output:
    for line in lines:
        output.write(line)