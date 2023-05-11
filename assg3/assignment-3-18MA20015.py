"""
18MA20015, Harshal Dupare
Big Data Processing, Assignment 3
Dated : 15-4-2023
"""
import sys
import re
import math
import pyspark

def main():
    DATA_FILE_PATH = sys.argv[1]
    QUERY_WORD = sys.argv[2]
    K = int(sys.argv[3])
    STOPWORD_FILE_PATH = sys.argv[4]
    
    sc = pyspark.SparkContext(conf=pyspark.SparkConf().setAppName("BDP3"))

    text_data = sc.textFile(DATA_FILE_PATH)
    n = text_data.count()
    # print(f"number of documents is {n}")

    stopwords = set(sc.textFile(STOPWORD_FILE_PATH).collect())
    def preprocess(text):
        words = re.sub("[^a-z ]", " ", text.lower()).split()
        words = list(set([w for w in words if w not in stopwords]))
        # print(words)
        return words

    word_set_per_line = text_data.map(preprocess)

    # Compute the cooccurrence matrix count values
    def words_to_query_word_cooccurrence_count_pair(words):
        if QUERY_WORD not in words:
            return []
        return [((w, QUERY_WORD), 1) for w in words if w!= QUERY_WORD]

    def unbind_paired_key(entry):
        return (entry[0][0], (entry[0][1], entry[1]))

    def reduce_by_addition(x, y):
        return x + y

    # ((w,qw),1)
    # ((w,qw),coocc(w,qw))
    # (w,(qw,coocc(w,qw)))
    coocc_matrix = word_set_per_line.flatMap(words_to_query_word_cooccurrence_count_pair).reduceByKey(reduce_by_addition).map(unbind_paired_key)

    # Compute the frequency count of each word
    def words_to_word_count_pair(words):
        return [(w, 1) for w in words]

    # (w,1)
    # (w, occ(w))
    word_freq_count = word_set_per_line.flatMap(words_to_word_count_pair).reduceByKey(reduce_by_addition)

    # Compute the PMI scores
    def swap_key(entry):
        """
        Need this function to compute make use of the match by key propery in join operation to pair the right (w,qw) with their (w,occ(w))
        """
        return (entry[1][0][0], (entry[0], entry[1][0][1], entry[1][1]))

    def get_pmi_score(entry):
        return (
            entry[1][0][0],
            entry[0],
            math.log2(entry[1][0][1] * n / (entry[1][0][2] * entry[1][1])),
        )

    # (w,((qw,coocc(w,qw)),occ(w)))
    # (qw,(w,coocc(w,qw),occ(w)))
    # (qw,((w,coocc(w,qw),occ(w)),occ(qw)))
    # (w,qw, pmi(w,qw))
    word_pmi_values = coocc_matrix.join(word_freq_count).map(swap_key).join(word_freq_count).map(get_pmi_score)
    
    def positive_filter(entry):
        return entry[2] > 0
        # return True
    def positive_order_key(entry):
        return -entry[2]

    top_k_positive = word_pmi_values.filter(positive_filter).takeOrdered(K, key=positive_order_key)

    def nagative_filter(entry):
        return entry[2] < 0
        # return True
    def negative_order_key(entry):
        return entry[2]

    top_k_negative = word_pmi_values.filter(nagative_filter).takeOrdered(K, key=negative_order_key)

    print(30*"-"+f"\n Positively associated top {K} words:")
    print(f"Note that this might give less than {K} words as there might not be {K} words with positive pmi-score\n"+30*"-")
    for w_qw_pmi in top_k_positive:
        print(f"{w_qw_pmi[0]}, PMI = {w_qw_pmi[2]}")
    print(flush=True)

    print(30*"-"+f"\n Negatively associated top {K} words:")
    print(f"Note that this might give less than {K} words as there might not be {K} words with negative pmi-score\n"+30*"-")
    for w_qw_pmi in top_k_negative:
        print(f"{w_qw_pmi[0]}, PMI = {w_qw_pmi[2]}")
    print(flush=True)

    sc.stop()

if __name__ == "__main__":
    main()

