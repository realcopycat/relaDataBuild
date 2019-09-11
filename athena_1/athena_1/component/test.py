#
from cocoNLP.config.phrase import rake

r = rake.Rake()

r.extract_keywords_from_sentences(['黄铜常被用于制造阀门、水管、空调内外机连接管和散热器等。'],
                                  2, 6)

ranked_words = r.get_ranked_phrases()

ranked_words_score = r.get_ranked_phrases_with_scores()

for ele in ranked_words_score:
    print(ele)



