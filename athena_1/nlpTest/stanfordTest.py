import stanfordnlp

nlp = stanfordnlp.Pipeline(processors='tokenize,mwt,pos', lang='zh')
doc = nlp("化学工业中是制造各种无机氰化物和发生氢氰酸的原料，也用于制造有机玻璃、各种合成材料、丁腈橡胶、合成纤维的共聚物。")
print(*[f'word: {word.text+" "}\t upos: {word.upos}\txpos: {word.xpos}' for sent in doc.sentences for word in sent.words], sep='\n')