import json
import bz2
import re
import pandas as pd
from nltk.util import ngrams
from collections import Counter

# preprocess commit messages - convert to lowercase and remove punctuation
def preprocess_message(message):
    message = message.lower()
    message = re.sub(r'[^\w\s]', '', message)
    return message.split()

# generate trigrams using nltk
def generate_ngrams(words, n=3):
    return [' '.join(gram) for gram in ngrams(words, n)]

# process a single PushEvent and update the author data
def process_push_event(event, author_data):
    author = event['actor']['login']
    commits = event['payload']['commits']
    
    if author not in author_data:
        author_data[author] = Counter()
    
    for commit in commits:
        message = commit['message']
        words = preprocess_message(message)
        three_grams = generate_ngrams(words, 3)
        author_data[author].update(three_grams)

# extract top 5 trigrams for each author
def extract_top_trigrams(author_data):
    csv_data = []
    for author, counts in author_data.items():
        top_trigrams = counts.most_common(5)
        row = [author] + [gram for gram, _ in top_trigrams]
        row.extend([''] * (5 - len(row)))  # fill empty slots if < 5 n-grams
        csv_data.append(row)
    return csv_data

# read the jsonl.bz2 file and process only the PushEvents
def process_file(file_path):
    author_data = {}
    with bz2.open(file_path, 'rt', encoding='utf-8') as f:
        for line in f:
            event = json.loads(line)
            if event['type'] == 'PushEvent':
                process_push_event(event, author_data)
    return author_data

# save the data to a CSV file
def save_to_csv(csv_data, output_file):
    df = pd.DataFrame(csv_data, columns=['author', 'first trigram', 'second trigram', 'third trigram', 'fourth trigram', 'fifth trigram'])
    df.to_csv(output_file, index=False, sep=';')

# main function to run the process
def main():
    file_path = '10K.github.jsonl.bz2'
    output_file = 'top_5_trigrams.csv'
    
    author_data = process_file(file_path)
    
    csv_data = extract_top_trigrams(author_data)
    
    save_to_csv(csv_data, output_file)

if __name__ == "__main__":
    main()