"""
18MA20015, Harshal Dupare
Big Data Processing, Assignment 1
Dated : 10-2-2023
"""
import sys
import os
import re
import threading
import time

def text_to_n_grams(N, text):
    tokens = re.findall(r'\b\w+\b', text)
    return [" ".join(tokens[i:i+N]).lower() for i in range(len(tokens)-N+1)]

def get_n_grams_count_dict_for_thread(N, files_to_read_in_thread, results, thread_id):
    n_gram_dict_thread = dict()
    for file_name in files_to_read_in_thread:
        f = open(file_name, 'r', encoding="latin1")
        text = f.read()
        f.close()
        dir_name = os.path.dirname(file_name)
        n_grams = text_to_n_grams(N, text)
        if dir_name not in n_gram_dict_thread.keys():
            n_gram_dict_thread[dir_name] = dict()
        for n_gram_item in n_grams:
            n_gram_dict_thread[dir_name][n_gram_item] = 1 + n_gram_dict_thread[dir_name].get(n_gram_item,0)
    results[thread_id] = n_gram_dict_thread
    return

if __name__ == '__main__':
    # get the system input for the program
    DATA_PATH = sys.argv[1]
    THREAD_COUNT = int(sys.argv[2])
    N = int(sys.argv[3])
    K = int(sys.argv[4])

    # get all the files in class folders
    files = []
    file_counts_dict = dict()
    for class_folder in os.listdir(DATA_PATH):
        class_path = os.path.join(DATA_PATH, class_folder)
        if os.path.isdir(class_path):
            _files = os.listdir(class_path)
            files += [ os.path.join(class_path,fn) for fn in _files ]
            file_counts_dict[class_path] = len(_files)
    number_of_files = len(files)
    files_per_thread = (number_of_files+THREAD_COUNT-1)//THREAD_COUNT

    # split workload over threads
    result_list = THREAD_COUNT * [None]
    threads = THREAD_COUNT * [None]
    i = 0
    thread_id = 0
    while i < number_of_files:
        threads[thread_id] = threading.Thread(target=get_n_grams_count_dict_for_thread, 
            args=(N, files[i:min(i+files_per_thread, number_of_files)], result_list, thread_id))
        threads[thread_id].start()
        thread_id += 1
        i += files_per_thread

    for i in range(thread_id):
        threads[i].join()

    n_gram_class_count_dict_all = dict()
    for i in range(thread_id):
        for dir_key, dir_dict in result_list[i].items():
            if dir_key not in n_gram_class_count_dict_all.keys():
                n_gram_class_count_dict_all[dir_key] = dict()
            for n_gram_item in dir_dict.keys():
                n_gram_class_count_dict_all[dir_key][n_gram_item] = dir_dict[n_gram_item] + n_gram_class_count_dict_all[dir_key].get(n_gram_item,0)

    for dir_key in n_gram_class_count_dict_all.keys():
        n_gram_class_count_dict_all[dir_key] = sorted( n_gram_class_count_dict_all[dir_key].items(), key = lambda key_value: -key_value[1])[:K]
        n_gram_class_count_dict_all[dir_key] = { key : value/file_counts_dict[dir_key] for key, value in n_gram_class_count_dict_all[dir_key] }

    # to maintain unique top k n-grams combine all the scores with keeping maximum score only
    top_k_n_grams_all = dict()
    for class_path, class_n_grams_list_top_k in n_gram_class_count_dict_all.items():
        for n_gram, score in class_n_grams_list_top_k.items():
            top_k_n_grams_all[n_gram] = max(top_k_n_grams_all.get(n_gram, -1), score)

    top_k_n_grams_all = sorted( top_k_n_grams_all.items(), key = lambda key_value: -key_value[1])[:K]

    for key, value in top_k_n_grams_all:
        print(f"{key} : {value}")
    print(flush=True)