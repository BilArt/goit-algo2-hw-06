import requests
import re
import matplotlib.pyplot as plt
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

# Function to fetch text from a given URL
def fetch_text(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text

# Mapping function: splits text into words and assigns a count of 1 to each
def map_function(text):
    words = re.findall(r'\b\w+\b', text.lower())
    return [(word, 1) for word in words]

# Shuffle function: groups values by key (word)
def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()

# Reduce function: sums up the occurrences of each word
def reduce_function(shuffled_values):
    reduced = {}
    for key, values in shuffled_values:
        reduced[key] = sum(values)
    return reduced

# Parallelized MapReduce function
def parallel_map_reduce(text, num_threads=4):
    words = re.findall(r'\b\w+\b', text.lower())

    # Split the words into chunks for parallel processing
    chunk_size = len(words) // num_threads
    chunks = [words[i * chunk_size:(i + 1) * chunk_size] for i in range(num_threads)]
    if len(words) % num_threads:
        chunks.append(words[num_threads * chunk_size:])

    # Parallel mapping using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        mapped_results = list(executor.map(map_function, [' '.join(chunk) for chunk in chunks]))

    # Combine mapping results
    combined_mapped_values = [item for sublist in mapped_results for item in sublist]

    # Apply Shuffle and Reduce
    shuffled_values = shuffle_function(combined_mapped_values)
    reduced_values = reduce_function(shuffled_values)

    return reduced_values

# Function to visualize the top N frequent words
def visualize_top_words(word_counts, top_n=10):
    # Sort words by frequency and select the top N
    sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    words, counts = zip(*sorted_words)

    # Plot horizontal bar chart
    plt.figure(figsize=(10, 6))
    plt.barh(words[::-1], counts[::-1], color="skyblue")
    plt.xlabel("Frequency")
    plt.ylabel("Words")
    plt.title("Top 10 Most Frequent Words")
    plt.show()

if __name__ == '__main__':
    url = "https://www.gutenberg.org/files/1342/1342-0.txt" # Example
    
    # Fetch text from the given URL
    text = fetch_text(url)

    # Execute parallel MapReduce
    word_counts = parallel_map_reduce(text, num_threads=4)

    # Visualize the results
    visualize_top_words(word_counts)
